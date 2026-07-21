package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

func TestQuotaSnapshotHasCapacityUsesRawRemainingPercentAcrossAllWindows(t *testing.T) {
	falseValue := false
	tests := []struct {
		name     string
		windows  []codexQuotaWindow
		allowed  *bool
		expected bool
	}{
		{
			name: "5h 30 and 7d 20 can be actively verified",
			windows: []codexQuotaWindow{
				{ID: "code-5h", RemainingPercent: float64Pointer(30)},
				{ID: "code-7d", RemainingPercent: float64Pointer(20)},
			},
			expected: true,
		},
		{
			name: "one exhausted blocking window cannot be verified",
			windows: []codexQuotaWindow{
				{ID: "code-5h", RemainingPercent: float64Pointer(30)},
				{ID: "code-7d", RemainingPercent: float64Pointer(0), Exhausted: true},
			},
			expected: false,
		},
		{
			name: "raw 0.4 percent remains eligible even if UI rounds to zero",
			windows: []codexQuotaWindow{
				{ID: "code-5h", RemainingPercent: float64Pointer(0.4)},
				{ID: "code-7d", RemainingPercent: float64Pointer(12)},
			},
			expected: true,
		},
		{
			name: "raw windows select candidates even when stale top-level allowed is false",
			windows: []codexQuotaWindow{
				{ID: "code-5h", RemainingPercent: float64Pointer(30)},
				{ID: "code-7d", RemainingPercent: float64Pointer(20)},
			},
			allowed:  &falseValue,
			expected: true,
		},
		{
			name: "unknown raw percentage fails closed",
			windows: []codexQuotaWindow{
				{ID: "code-5h"},
				{ID: "code-7d", RemainingPercent: float64Pointer(20)},
			},
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := &codexQuotaSnapshot{Windows: test.windows, Allowed: test.allowed}
			if got := quotaSnapshotHasCapacity(snapshot); got != test.expected {
				t.Fatalf("quotaSnapshotHasCapacity = %v, want %v: %+v", got, test.expected, snapshot)
			}
		})
	}
}

func TestQuotaEligibilityAllowsAgentIdentityQueriesAndCreditActions(t *testing.T) {
	token := store.Token{RefreshToken: agentidentity.SyntheticRefreshPrefix + "runtime", IsActive: true}
	if !quotaEligible(token) {
		t.Fatal("agent identity token should be eligible for assertion-authenticated quota queries")
	}
	if !quotaActionEligible(token) {
		t.Fatal("agent identity token should be eligible for assertion-authenticated quota reset credits")
	}
}

func TestQuotaRecoveryEffectiveConcurrencyReservesDatabaseConnections(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		maxConns   int32
		want       int
	}{
		{name: "production defaults remain unchanged", configured: 4, maxConns: 32, want: 4},
		{name: "large setting is capped to paired connection budget", configured: 20, maxConns: 32, want: 15},
		{name: "small pool keeps two connections in reserve", configured: 4, maxConns: 8, want: 3},
		{name: "minimum safe pool allows one worker", configured: 1, maxConns: 4, want: 1},
		{name: "unsafe pool disables recovery", configured: 1, maxConns: 3, want: 0},
		{name: "disabled configuration stays disabled", configured: 0, maxConns: 32, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := quotaRecoveryEffectiveConcurrency(test.configured, test.maxConns); got != test.want {
				t.Fatalf("effective concurrency = %d, want %d", got, test.want)
			}
		})
	}
}

func TestQuotaRecoveryHTTPErrorReasonUsesFixedCredentialAndStatusClasses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   quotaRecoveryCheckErrorReason
	}{
		{
			name:   "inactive personal access token",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Personal access token inactive"}}`,
			want:   quotaRecoveryCheckErrorAccessTokenInactive,
		},
		{
			name:   "invalidated authentication token",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"The authentication token has been invalidated"}}`,
			want:   quotaRecoveryCheckErrorAuthenticationInvalidated,
		},
		{
			name:   "structured token invalidated code",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"token_invalidated"}}`,
			want:   quotaRecoveryCheckErrorAuthenticationInvalidated,
		},
		{
			name:   "inactive owner",
			status: http.StatusForbidden,
			body:   `{"error":{"message":"Account owner inactive"}}`,
			want:   quotaRecoveryCheckErrorOwnerInactive,
		},
		{
			name:   "expired authentication token",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Authentication token expired"}}`,
			want:   quotaRecoveryCheckErrorAuthenticationTokenExpired,
		},
		{
			name:   "unknown unauthorized response",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"unknown credential problem"}}`,
			want:   quotaRecoveryCheckErrorHTTPUnauthorized,
		},
		{
			name:   "rate limited response",
			status: http.StatusTooManyRequests,
			body:   `{"error":{"message":"slow down"}}`,
			want:   quotaRecoveryCheckErrorHTTPRateLimited,
		},
		{
			name:   "server response",
			status: http.StatusBadGateway,
			body:   `upstream unavailable`,
			want:   quotaRecoveryCheckErrorHTTPServer,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := quotaRecoveryHTTPErrorReason(test.status, []byte(test.body), responseErrorDetail(test.status, []byte(test.body))); got != test.want {
				t.Fatalf("quotaRecoveryHTTPErrorReason = %q, want %q", got, test.want)
			}
		})
	}
}

func TestQuotaRecoveryCheckErrorStatsAreCompleteAndLowCardinality(t *testing.T) {
	worker := &quotaRecoveryWorker{}
	worker.recordQuotaCheckError(quotaRecoveryCheckErrorAccessTokenInactive)
	worker.recordQuotaCheckError(quotaRecoveryCheckErrorAccessTokenInactive)
	worker.recordQuotaCheckError("unbounded-upstream-message")
	worker.recordFreshQuotaErrorSkip(quotaRecoveryCheckErrorCredentialRefreshRejected)

	stats := worker.Stats()
	if stats.QuotaCheckErrors != 3 {
		t.Fatalf("quota check errors = %d, want 3", stats.QuotaCheckErrors)
	}
	if got := stats.QuotaCheckErrorsByReason[string(quotaRecoveryCheckErrorAccessTokenInactive)]; got != 2 {
		t.Fatalf("inactive access token errors = %d, want 2", got)
	}
	if got := stats.QuotaCheckErrorsByReason[string(quotaRecoveryCheckErrorOther)]; got != 1 {
		t.Fatalf("other errors = %d, want 1", got)
	}
	if len(stats.QuotaCheckErrorsByReason) != len(quotaRecoveryCheckErrorReasons) {
		t.Fatalf("reason cardinality = %d, want fixed %d", len(stats.QuotaCheckErrorsByReason), len(quotaRecoveryCheckErrorReasons))
	}
	var total int64
	for _, count := range stats.QuotaCheckErrorsByReason {
		total += count
	}
	if total != stats.QuotaCheckErrors {
		t.Fatalf("categorized errors = %d, total errors = %d", total, stats.QuotaCheckErrors)
	}
	if stats.FreshQuotaErrorSkips != 1 || stats.FreshQuotaErrorSkipsByReason[string(quotaRecoveryCheckErrorCredentialRefreshRejected)] != 1 {
		t.Fatalf("fresh quota error skip stats = %+v", stats)
	}
}

func TestQuotaRecoveryHTTPErrorReasonUsesStructuredCodeBeforeLocalizedMessage(t *testing.T) {
	body := []byte(`{"message":"owner inactive","error":{"message":"登录状态异常","code":"authentication_token_invalidated"}}`)
	if got := quotaRecoveryHTTPErrorReason(http.StatusUnauthorized, body, responseErrorDetail(http.StatusUnauthorized, body)); got != quotaRecoveryCheckErrorAuthenticationInvalidated {
		t.Fatalf("structured error reason = %q, want %q", got, quotaRecoveryCheckErrorAuthenticationInvalidated)
	}
}

func TestQuotaRecoveryCredentialRefreshErrorReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want quotaRecoveryCheckErrorReason
	}{
		{name: "concurrent credential update", err: store.ErrTokenCredentialsChanged, want: quotaRecoveryCheckErrorCredentialRefreshConflict},
		{name: "permanent oauth rejection", err: errors.New(`oauth refresh failed with status 400: {"error":"invalid_grant"}`), want: quotaRecoveryCheckErrorCredentialRefreshRejected},
		{name: "generic oauth forbidden", err: errors.New(`oauth refresh failed with status 403: forbidden`), want: quotaRecoveryCheckErrorCredentialRefreshRejected},
		{name: "deadline", err: context.DeadlineExceeded, want: quotaRecoveryCheckErrorDeadlineExceeded},
		{name: "other refresh failure", err: errors.New("oauth backend unavailable"), want: quotaRecoveryCheckErrorCredentialRefreshOther},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := quotaRecoveryCredentialRefreshErrorReason(test.err); got != test.want {
				t.Fatalf("refresh error reason = %q, want %q", got, test.want)
			}
		})
	}
}

func TestQuotaRecoveryFetchDiagnosticPreservesBehaviorAndClassifiesFailureStage(t *testing.T) {
	t.Run("refresh rejection takes precedence over stale access token response", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":"authentication_token_invalidated"}}`))
		}))
		defer upstream.Close()
		service := quotaRecoveryTestQuotaService(upstream.URL, &quotaRecoveryOAuthStub{
			err: errors.New(`oauth refresh failed with status 400: {"error":"invalid_grant"}`),
		})
		snapshot, reason := service.fetchSnapshotWithoutHistory(t.Context(), store.Token{
			ID: 1, AccessToken: "stale-access", RefreshToken: "rejected-refresh",
		})
		if snapshot == nil || snapshot.Error == nil || !strings.Contains(*snapshot.Error, "authentication_token_invalidated") {
			t.Fatalf("original quota error behavior changed: %#v", snapshot)
		}
		if reason != quotaRecoveryCheckErrorCredentialRefreshRejected {
			t.Fatalf("diagnostic reason = %q, want %q", reason, quotaRecoveryCheckErrorCredentialRefreshRejected)
		}
	})

	t.Run("successful refresh followed by unauthorized response uses new response", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			if r.Header.Get("Authorization") == "Bearer refreshed-access" {
				_, _ = w.Write([]byte(`{"error":{"message":"unknown credential problem"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"error":{"code":"authentication_token_invalidated"}}`))
		}))
		defer upstream.Close()
		service := quotaRecoveryTestQuotaService(upstream.URL, &quotaRecoveryOAuthStub{
			result: oauth.RefreshResult{AccessToken: "refreshed-access"},
		})
		_, reason := service.fetchSnapshotWithoutHistory(t.Context(), store.Token{
			ID: 2, AccessToken: "stale-access", RefreshToken: "valid-refresh",
		})
		if reason != quotaRecoveryCheckErrorHTTPUnauthorized {
			t.Fatalf("diagnostic reason = %q, want %q", reason, quotaRecoveryCheckErrorHTTPUnauthorized)
		}
	})

	tests := []struct {
		name   string
		status int
		body   string
		want   quotaRecoveryCheckErrorReason
	}{
		{name: "rate limit", status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`, want: quotaRecoveryCheckErrorHTTPRateLimited},
		{name: "server error cannot masquerade as credential failure", status: http.StatusBadGateway, body: `{"error":{"code":"authentication_token_invalidated"}}`, want: quotaRecoveryCheckErrorHTTPServer},
		{name: "invalid json", status: http.StatusOK, body: `{`, want: quotaRecoveryCheckErrorInvalidJSON},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer upstream.Close()
			service := quotaRecoveryTestQuotaService(upstream.URL, &quotaRecoveryOAuthStub{})
			_, reason := service.fetchSnapshotWithoutHistory(t.Context(), store.Token{ID: 3, AccessToken: "access"})
			if reason != test.want {
				t.Fatalf("diagnostic reason = %q, want %q", reason, test.want)
			}
		})
	}
}

func TestQuotaRecoveryFetchDiagnosticClassifiesHTTPClientDeadline(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer upstream.Close()
	service := quotaRecoveryTestQuotaService(upstream.URL, &quotaRecoveryOAuthStub{})
	service.client.Timeout = 20 * time.Millisecond
	snapshot, reason := service.fetchSnapshotWithoutHistory(t.Context(), store.Token{ID: 4, AccessToken: "access"})
	if snapshot != nil {
		t.Fatalf("deadline must not poison quota cache: %#v", snapshot)
	}
	if reason != quotaRecoveryCheckErrorDeadlineExceeded {
		t.Fatalf("diagnostic reason = %q, want %q", reason, quotaRecoveryCheckErrorDeadlineExceeded)
	}
}

func TestQuotaRecoveryFreshPersistedErrorSkipIsObservable(t *testing.T) {
	now := time.Now().UTC()
	errorText := "HTTP 401: authentication_token_invalidated"
	snapshot := codexQuotaSnapshot{FetchedAt: now, Error: &errorText, Windows: []codexQuotaWindow{}}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fetchedAt := now
	candidate := store.QuotaRecoveryCandidate{
		TokenID: 8, OwnerUserID: 9, CooldownUntil: now.Add(time.Hour), SourceEventID: 10,
		QuotaSnapshot: raw, QuotaFetchedAt: &fetchedAt,
	}
	fake := &fakeQuotaRecoveryStore{candidates: []store.QuotaRecoveryCandidate{candidate}}
	worker := &quotaRecoveryWorker{
		cfg:   config.QuotaRecoveryConfig{BatchSize: 1, Concurrency: 1, QuotaMaxAge: time.Minute},
		store: fake, quota: &adminQuotaService{}, nextCheck: map[int64]time.Time{}, nextProbe: map[int64]time.Time{},
	}
	worker.scan(t.Context())
	stats := worker.Stats()
	if stats.FreshQuotaErrorSkips != 1 || stats.FreshQuotaErrorSkipsByReason[string(quotaRecoveryCheckErrorFreshSnapshotUnknown)] != 1 {
		t.Fatalf("fresh error skip stats = %+v", stats)
	}
}

func TestQuotaRecoveryProcessCandidateClassifiesPreflightFailures(t *testing.T) {
	now := time.Now().UTC()
	newCandidate := func() store.QuotaRecoveryCandidate {
		candidate := recoveryTestCandidate(t, now, 30, 20)
		candidate.QuotaSnapshot = nil
		candidate.QuotaFetchedAt = nil
		return candidate
	}
	t.Run("lease error", func(t *testing.T) {
		worker := &quotaRecoveryWorker{
			cfg:   config.QuotaRecoveryConfig{QuotaMaxAge: time.Minute},
			store: &fakeQuotaRecoveryStore{leaseErr: errors.New("lease failed")},
			quota: quotaRecoveryTestQuotaService("https://example.invalid", &quotaRecoveryOAuthStub{}),
		}
		worker.processCandidate(t.Context(), newCandidate())
		stats := worker.Stats()
		if stats.QuotaCheckErrors != 1 || stats.QuotaCheckErrorsByReason[string(quotaRecoveryCheckErrorCheckLease)] != 1 {
			t.Fatalf("lease error stats = %+v", stats)
		}
	})
	t.Run("lease contention is not an error", func(t *testing.T) {
		worker := &quotaRecoveryWorker{
			cfg:   config.QuotaRecoveryConfig{QuotaMaxAge: time.Minute},
			store: &fakeQuotaRecoveryStore{leaseContended: true},
			quota: quotaRecoveryTestQuotaService("https://example.invalid", &quotaRecoveryOAuthStub{}),
		}
		worker.processCandidate(t.Context(), newCandidate())
		if got := worker.Stats().QuotaCheckErrors; got != 0 {
			t.Fatalf("lease contention counted as error: %d", got)
		}
	})
	t.Run("token disappeared", func(t *testing.T) {
		worker := &quotaRecoveryWorker{
			cfg:   config.QuotaRecoveryConfig{QuotaMaxAge: time.Minute},
			store: &fakeQuotaRecoveryStore{nilToken: true},
			quota: quotaRecoveryTestQuotaService("https://example.invalid", &quotaRecoveryOAuthStub{}),
		}
		worker.processCandidate(t.Context(), newCandidate())
		stats := worker.Stats()
		if stats.QuotaCheckErrors != 1 || stats.QuotaCheckErrorsByReason[string(quotaRecoveryCheckErrorTokenLoad)] != 1 {
			t.Fatalf("token load error stats = %+v", stats)
		}
	})
	t.Run("canceled context", func(t *testing.T) {
		candidate := newCandidate()
		worker := &quotaRecoveryWorker{
			cfg:   config.QuotaRecoveryConfig{QuotaMaxAge: time.Minute},
			store: &fakeQuotaRecoveryStore{token: store.Token{ID: candidate.TokenID, AccessToken: "access", IsActive: true}},
			quota: quotaRecoveryTestQuotaService("https://example.invalid", &quotaRecoveryOAuthStub{}),
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		worker.processCandidate(ctx, candidate)
		stats := worker.Stats()
		if stats.QuotaCheckErrors != 1 || stats.QuotaCheckErrorsByReason[string(quotaRecoveryCheckErrorContextCanceled)] != 1 {
			t.Fatalf("context cancellation stats = %+v", stats)
		}
	})
}

func TestQuotaRecoveryDiagnosticReasonIsNotSerialized(t *testing.T) {
	snapshot := quotaErrorSnapshotForRecovery(time.Now().UTC(), "fixture-secret", quotaRecoveryCheckErrorCredentialRefreshRejected)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), string(quotaRecoveryCheckErrorCredentialRefreshRejected)) || strings.Contains(string(raw), "recoveryErrorReason") {
		t.Fatalf("internal diagnostic leaked into quota payload: %s", raw)
	}
}

func quotaRecoveryTestQuotaService(usageURL string, oauthClient oauth.Client) *adminQuotaService {
	return &adminQuotaService{
		client: &http.Client{Timeout: time.Second}, oauthClient: oauthClient, usageURL: usageURL,
		userAgent: "test", ttl: time.Minute, concurrency: 1, sem: make(chan struct{}, 1),
		cache: map[int64]cachedQuotaSnapshot{}, pending: map[int64]struct{}{},
	}
}

type quotaRecoveryOAuthStub struct {
	result oauth.RefreshResult
	err    error
}

func (s *quotaRecoveryOAuthStub) Refresh(context.Context, string) (oauth.RefreshResult, error) {
	return s.result, s.err
}

func TestQuotaRecoveryScanPrioritizesFreshCapacityOverEarlierZeroSnapshot(t *testing.T) {
	now := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n"))
	}))
	defer upstream.Close()

	zero := recoveryTestCandidate(t, now, 30, 0)
	positive := recoveryTestCandidate(t, now, 30, 20)
	positive.TokenID = zero.TokenID + 1
	positive.SourceEventID = zero.SourceEventID + 1
	fake := &fakeQuotaRecoveryStore{
		candidates: []store.QuotaRecoveryCandidate{zero, positive},
		token: store.Token{
			ID:            positive.TokenID,
			OwnerUserID:   positive.OwnerUserID,
			AccessToken:   "access",
			IsActive:      true,
			CooldownUntil: &positive.CooldownUntil,
		},
	}
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	worker := &quotaRecoveryWorker{
		app: app,
		cfg: config.QuotaRecoveryConfig{
			BatchSize:          1,
			Concurrency:        1,
			QuotaMaxAge:        time.Minute,
			RecheckInterval:    time.Minute,
			ProbeRetryInterval: time.Minute,
		},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: map[int64]time.Time{},
		nextProbe: map[int64]time.Time{},
	}
	app.recovery = worker
	worker.scan(t.Context())

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.claimCalls != 1 || fake.completeCalls != 1 {
		t.Fatalf("fresh positive candidate was starved by earlier zero snapshot: claims=%d completes=%d", fake.claimCalls, fake.completeCalls)
	}
}

func TestAutomaticQuotaRecoveryRequiresStrictCompletedProbe(t *testing.T) {
	now := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n"))
	}))
	defer upstream.Close()

	candidate := recoveryTestCandidate(t, now, 30, 20)
	fake := &fakeQuotaRecoveryStore{
		token: store.Token{ID: candidate.TokenID, OwnerUserID: candidate.OwnerUserID, AccessToken: "access", IsActive: true, CooldownUntil: &candidate.CooldownUntil},
	}
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	worker := &quotaRecoveryWorker{
		app:       app,
		cfg:       config.QuotaRecoveryConfig{ProbeRetryInterval: time.Minute, QuotaMaxAge: time.Minute},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: map[int64]time.Time{},
		nextProbe: map[int64]time.Time{},
	}
	app.recovery = worker
	worker.processCandidate(t.Context(), candidate)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.claimCalls != 1 || fake.completeCalls != 1 || fake.recordCalls != 0 {
		t.Fatalf("unexpected state transitions: claims=%d completes=%d records=%d", fake.claimCalls, fake.completeCalls, fake.recordCalls)
	}
	if fake.completedFence.AccessToken != "access" {
		t.Fatalf("completion was not fenced to probed credentials: %+v", fake.completedFence)
	}
	if got := worker.Stats(); got.Reactivated != 1 || got.ProbeCompleted != 1 {
		t.Fatalf("worker stats = %+v", got)
	}
}

func TestRejectedDurableClaimUsesDatabaseAbsoluteRetryTime(t *testing.T) {
	now := time.Now().UTC()
	candidate := recoveryTestCandidate(t, now, 30, 20)
	nextEligibleAt := now.Add(7 * time.Minute)
	fake := &fakeQuotaRecoveryStore{rejectClaims: true, nextEligibleAt: &nextEligibleAt}
	nextCheck := map[int64]time.Time{candidate.TokenID: now.Add(15 * time.Minute)}
	worker := &quotaRecoveryWorker{
		cfg: config.QuotaRecoveryConfig{
			ProbeRetryInterval: 15 * time.Minute,
			QuotaMaxAge:        time.Minute,
		},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: nextCheck,
		nextProbe: map[int64]time.Time{},
	}
	worker.processCandidate(t.Context(), candidate)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.claimCalls != 1 {
		t.Fatalf("durable claim calls=%d, want 1", fake.claimCalls)
	}
	if got := worker.nextProbe[candidate.TokenID]; !got.Equal(nextEligibleAt) {
		t.Fatalf("probe retry = %s, want database time %s", got, nextEligibleAt)
	}
	if got := worker.nextCheck[candidate.TokenID]; !got.Equal(nextEligibleAt) {
		t.Fatalf("quota recheck = %s, want database time %s", got, nextEligibleAt)
	}
}

func TestQuotaRecoveryReusesFreshInMemoryQuotaSnapshot(t *testing.T) {
	now := time.Now().UTC()
	candidate := recoveryTestCandidate(t, now, 30, 20)
	var snapshot codexQuotaSnapshot
	if err := json.Unmarshal(candidate.QuotaSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	candidate.QuotaSnapshot = nil
	candidate.QuotaFetchedAt = nil
	quota := &adminQuotaService{
		cache: map[int64]cachedQuotaSnapshot{
			candidate.TokenID: {snapshot: &snapshot, expiresAt: now.Add(time.Minute)},
		},
	}
	worker := &quotaRecoveryWorker{
		cfg:   config.QuotaRecoveryConfig{QuotaMaxAge: time.Minute},
		quota: quota,
	}
	got, fresh := worker.snapshotForCandidate(candidate, now)
	if !fresh || !quotaSnapshotHasCapacity(got) {
		t.Fatalf("fresh in-memory quota result was not reused: fresh=%v snapshot=%+v", fresh, got)
	}
}

func TestAutomaticQuotaRecoveryProbesRawPointFourPercent(t *testing.T) {
	now := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n"))
	}))
	defer upstream.Close()

	candidate := recoveryTestCandidate(t, now, 0.4, 12)
	fake := &fakeQuotaRecoveryStore{
		token: store.Token{ID: candidate.TokenID, OwnerUserID: candidate.OwnerUserID, AccessToken: "access", IsActive: true, CooldownUntil: &candidate.CooldownUntil},
	}
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	worker := &quotaRecoveryWorker{
		app:       app,
		cfg:       config.QuotaRecoveryConfig{ProbeRetryInterval: time.Minute, QuotaMaxAge: time.Minute},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: map[int64]time.Time{},
		nextProbe: map[int64]time.Time{},
	}
	app.recovery = worker
	worker.processCandidate(t.Context(), candidate)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.claimCalls != 1 || fake.completeCalls != 1 {
		t.Fatalf("raw 0.4%% capacity was not strictly probed and recovered: claims=%d completes=%d", fake.claimCalls, fake.completeCalls)
	}
}

func TestAutomaticQuotaRecoveryFailsClosedWhenStreamEndsBeforeCompletion(t *testing.T) {
	now := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.created\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.created","response":{"status":"in_progress"}}` + "\n\n"))
	}))
	defer upstream.Close()

	candidate := recoveryTestCandidate(t, now, 30, 20)
	fake := &fakeQuotaRecoveryStore{
		token: store.Token{ID: candidate.TokenID, OwnerUserID: candidate.OwnerUserID, AccessToken: "access", IsActive: true, CooldownUntil: &candidate.CooldownUntil},
	}
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	worker := &quotaRecoveryWorker{
		app:       app,
		cfg:       config.QuotaRecoveryConfig{ProbeRetryInterval: time.Minute, QuotaMaxAge: time.Minute},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: map[int64]time.Time{},
		nextProbe: map[int64]time.Time{},
	}
	app.recovery = worker
	worker.processCandidate(t.Context(), candidate)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.completeCalls != 0 || fake.recordCalls != 1 {
		t.Fatalf("incomplete stream changed token state: completes=%d records=%d", fake.completeCalls, fake.recordCalls)
	}
}

func TestAutomaticQuotaRecoveryDoesNotProbeWhenAnyRawWindowIsZero(t *testing.T) {
	now := time.Now().UTC()
	candidate := recoveryTestCandidate(t, now, 30, 0)
	fake := &fakeQuotaRecoveryStore{}
	worker := &quotaRecoveryWorker{
		cfg:       config.QuotaRecoveryConfig{ProbeRetryInterval: time.Minute, QuotaMaxAge: time.Minute},
		store:     fake,
		quota:     &adminQuotaService{},
		nextCheck: map[int64]time.Time{},
		nextProbe: map[int64]time.Time{},
	}
	worker.processCandidate(t.Context(), candidate)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.claimCalls != 0 || fake.completeCalls != 0 {
		t.Fatalf("zero blocking window must not start probe: claims=%d completes=%d", fake.claimCalls, fake.completeCalls)
	}
}

func recoveryTestCandidate(t *testing.T, now time.Time, fiveHourRemaining, weeklyRemaining float64) store.QuotaRecoveryCandidate {
	t.Helper()
	snapshot := codexQuotaSnapshot{
		FetchedAt: now,
		Windows: []codexQuotaWindow{
			{ID: "code-5h", RemainingPercent: float64Pointer(fiveHourRemaining), Exhausted: fiveHourRemaining <= 0},
			{ID: "code-7d", RemainingPercent: float64Pointer(weeklyRemaining), Exhausted: weeklyRemaining <= 0},
		},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	fetchedAt := now
	return store.QuotaRecoveryCandidate{
		TokenID:        17,
		OwnerUserID:    9,
		CooldownUntil:  now.Add(time.Hour),
		SourceEventID:  31,
		QuotaSnapshot:  raw,
		QuotaFetchedAt: &fetchedAt,
	}
}

func float64Pointer(value float64) *float64 {
	return &value
}

type fakeQuotaRecoveryStore struct {
	mu             sync.Mutex
	candidates     []store.QuotaRecoveryCandidate
	token          store.Token
	rejectClaims   bool
	nextEligibleAt *time.Time
	claimCalls     int
	completeCalls  int
	usageCalls     int
	recordCalls    int
	completedFence store.QuotaRecoveryCredentialFence
	leaseErr       error
	leaseContended bool
	nilToken       bool
	getTokenErr    error
}

func (s *fakeQuotaRecoveryStore) ListQuotaRecoveryCandidates(context.Context) ([]store.QuotaRecoveryCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.QuotaRecoveryCandidate(nil), s.candidates...), nil
}

func (s *fakeQuotaRecoveryStore) TryQuotaRecoveryCheckLease(context.Context, int64) (*store.QuotaRecoveryCheckLease, bool, error) {
	if s.leaseErr != nil {
		return nil, false, s.leaseErr
	}
	if s.leaseContended {
		return nil, false, nil
	}
	return &store.QuotaRecoveryCheckLease{}, true, nil
}

func (s *fakeQuotaRecoveryStore) BeginQuotaRecoveryProbe(_ context.Context, candidate store.QuotaRecoveryCandidate, _ time.Duration, _ time.Duration) (*store.QuotaRecoveryClaim, *time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	if s.rejectClaims {
		return nil, s.nextEligibleAt, nil
	}
	return &store.QuotaRecoveryClaim{
		TokenID:       candidate.TokenID,
		OwnerUserID:   candidate.OwnerUserID,
		CooldownUntil: candidate.CooldownUntil,
		SourceEventID: candidate.SourceEventID,
		ProbeEventID:  44,
		StartedAt:     time.Now().UTC(),
	}, nil, nil
}

func (s *fakeQuotaRecoveryStore) GetToken(context.Context, int64) (*store.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getTokenErr != nil {
		return nil, s.getTokenErr
	}
	if s.nilToken {
		return nil, nil
	}
	copy := s.token
	return &copy, nil
}

func (s *fakeQuotaRecoveryStore) CompleteQuotaRecovery(_ context.Context, _ store.QuotaRecoveryClaim, fence store.QuotaRecoveryCredentialFence, _ map[string]any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeCalls++
	s.completedFence = fence
	return true, nil
}

func (s *fakeQuotaRecoveryStore) ApplyQuotaRecoveryUsageLimit(context.Context, store.QuotaRecoveryClaim, store.QuotaRecoveryCredentialFence, *time.Time, time.Duration, string, map[string]any) (bool, *time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usageCalls++
	return true, nil, nil
}

func (s *fakeQuotaRecoveryStore) RecordQuotaRecoveryResult(context.Context, store.QuotaRecoveryClaim, store.QuotaRecoveryResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordCalls++
	return nil
}
