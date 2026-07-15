package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
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
	claimCalls     int
	completeCalls  int
	usageCalls     int
	recordCalls    int
	completedFence store.QuotaRecoveryCredentialFence
}

func (s *fakeQuotaRecoveryStore) ListQuotaRecoveryCandidates(context.Context) ([]store.QuotaRecoveryCandidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.QuotaRecoveryCandidate(nil), s.candidates...), nil
}

func (s *fakeQuotaRecoveryStore) TryQuotaRecoveryCheckLease(context.Context, int64) (*store.QuotaRecoveryCheckLease, bool, error) {
	return &store.QuotaRecoveryCheckLease{}, true, nil
}

func (s *fakeQuotaRecoveryStore) BeginQuotaRecoveryProbe(_ context.Context, candidate store.QuotaRecoveryCandidate, _ time.Duration, _ time.Duration) (*store.QuotaRecoveryClaim, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	return &store.QuotaRecoveryClaim{
		TokenID:       candidate.TokenID,
		OwnerUserID:   candidate.OwnerUserID,
		CooldownUntil: candidate.CooldownUntil,
		SourceEventID: candidate.SourceEventID,
		ProbeEventID:  44,
		StartedAt:     time.Now().UTC(),
	}, true, nil
}

func (s *fakeQuotaRecoveryStore) GetToken(context.Context, int64) (*store.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
