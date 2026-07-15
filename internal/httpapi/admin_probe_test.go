package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

func TestProbeTokenUsesCurrentAccessTokenAndStreamingPayload(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("oauth refresh should not be called by token probe")
	}))
	defer oauthServer.Close()

	var upstreamSeen bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer current-access-token" {
			t.Fatalf("authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Fatalf("accept = %q", got)
		}
		if got := r.Header.Get("Version"); got != "" {
			t.Fatalf("version header should be absent, got %q", got)
		}
		if got := r.Header.Get("User-Agent"); got != codexUserAgent {
			t.Fatalf("user agent = %q", got)
		}
		if got := r.Header.Get("Session_id"); !strings.HasPrefix(got, "oaix-admin-probe-7") {
			t.Fatalf("session id = %q", got)
		}
		if got := r.Header.Get("Chatgpt-Account-Id"); got != "acct_current" {
			t.Fatalf("account id = %q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["model"] != "gpt-5.5" || payload["stream"] != true || payload["store"] != false || payload["instructions"] != "" {
			t.Fatalf("unexpected probe payload: %#v", payload)
		}
		upstreamSeen = true
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.5"}}` + "\n\n"))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:  upstream.URL,
			OAuthTokenURL: oauthServer.URL,
			OAuthScope:    "openid profile email",
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: 5 * time.Minute},
	}}
	accountID := "acct_current"
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           7,
		AccessToken:  "current-access-token",
		RefreshToken: "rt-fixture",
		AccountID:    &accountID,
	}, "gpt-5.5")

	if !upstreamSeen {
		t.Fatal("upstream probe was not called")
	}
	if result["outcome"] != "reactivated" || result["status_code"] != http.StatusOK {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result["response_model"] != "gpt-5.5" {
		t.Fatalf("response model = %#v", result["response_model"])
	}
}

func TestProbeTokenIgnoresInvalidRefreshTokenWhenCurrentAccessTokenWorks(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("oauth refresh should not be called by token probe")
	}))
	defer oauthServer.Close()
	var upstreamSeen bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer still-valid-access-token" {
			t.Fatalf("authorization = %q", got)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["model"] != "gpt-5.4-mini" {
			t.Fatalf("default probe model = %#v", payload["model"])
		}
		upstreamSeen = true
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.5"}}` + "\n\n"))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           9,
		AccessToken:  "still-valid-access-token",
		RefreshToken: "rt-invalid",
	}, "")

	if !upstreamSeen {
		t.Fatal("upstream probe was not called")
	}
	if result["outcome"] != "reactivated" || result["status_code"] != http.StatusOK {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestProbeTokenReturnsRawBodyForInconclusiveNonSuccess(t *testing.T) {
	upstreamBody := `{"error":{"code":"temporary_gateway_error","message":"upstream unavailable","type":"gateway_error"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:          11,
		AccessToken: "still-valid-access-token",
	}, "gpt-5.4-mini")

	if result["outcome"] != "inconclusive" || result["status_code"] != http.StatusBadGateway {
		t.Fatalf("unexpected result: %#v", result)
	}
	if detail := fmt.Sprint(result["detail"]); !strings.Contains(detail, "upstream unavailable") {
		t.Fatalf("detail = %#v", result["detail"])
	}
}

func TestProbeTokenRefreshesOAuthOnceAfterAuthRejection(t *testing.T) {
	var refreshCalls int
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls++
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("refresh_token") != "refresh-current" {
			t.Fatalf("refresh token = %q", r.Form.Get("refresh_token"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access-refreshed","refresh_token":"refresh-rotated","expires_in":3600}`))
	}))
	defer oauthServer.Close()

	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Header.Get("Authorization") == "Bearer access-current" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"expired"}}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer access-refreshed" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n"))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           19,
		AccessToken:  "access-current",
		RefreshToken: "refresh-current",
	}, defaultAdminProbeModel)
	if result["outcome"] != "reactivated" {
		t.Fatalf("probe result = %#v", result)
	}
	if refreshCalls != 1 || upstreamCalls != 2 {
		t.Fatalf("refresh calls=%d upstream calls=%d", refreshCalls, upstreamCalls)
	}
}

func TestProbeTokenKeepsStateWhenAuthStillRejectedAfterRefresh(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access-refreshed","refresh_token":"refresh-rotated"}`))
	}))
	defer oauthServer.Close()
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"type":"authentication_error","message":"rejected"}}`))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           21,
		AccessToken:  "access-current",
		RefreshToken: "refresh-current",
	}, defaultAdminProbeModel)
	if result["outcome"] != "inconclusive" || result["status_code"] != http.StatusUnauthorized {
		t.Fatalf("probe result = %#v", result)
	}
	if upstreamCalls != 2 {
		t.Fatalf("upstream calls = %d, want 2", upstreamCalls)
	}
}

func TestProbeTokenDoesNotTreatOrdinary429AsUsageLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_exceeded","message":"concurrency limit exceeded"}}`))
	}))
	defer upstream.Close()
	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{ID: 23, AccessToken: "access"}, defaultAdminProbeModel)
	if result["outcome"] != "inconclusive" {
		t.Fatalf("ordinary 429 changed token state: %#v", result)
	}
}

func TestManualProbeUsageLimitWithoutResetPreservesLongCooldown(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	previous := now.Add(7 * 24 * time.Hour)
	got := manualProbeUsageLimitCooldown(&previous, nil, now, 5*time.Minute)
	if got == nil || !got.Equal(previous) {
		t.Fatalf("cooldown = %v, want preserved %s", got, previous)
	}
}

func TestManualProbeUsageLimitPastResetUsesFutureFallback(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	got := manualProbeUsageLimitCooldown(nil, &past, now, 5*time.Minute)
	if want := now.Add(5 * time.Minute); got == nil || !got.Equal(want) {
		t.Fatalf("cooldown = %v, want %s", got, want)
	}
}

func TestStrictProbeStreamTerminalMatrix(t *testing.T) {
	now := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		body    string
		outcome tokenProbeOutcome
	}{
		{
			name:    "completed terminal",
			body:    "event: response.completed\n" + `data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n",
			outcome: tokenProbeCompleted,
		},
		{
			name:    "completed event missing completed response status",
			body:    "event: response.completed\n" + `data: {"type":"response.completed","response":{"model":"gpt-5.4-mini"}}` + "\n\n",
			outcome: tokenProbeInconclusive,
		},
		{
			name:    "created then eof",
			body:    "event: response.created\n" + `data: {"type":"response.created","response":{"status":"in_progress"}}` + "\n\n",
			outcome: tokenProbeInconclusive,
		},
		{
			name:    "done sentinel without completed",
			body:    "data: [DONE]\n\n",
			outcome: tokenProbeInconclusive,
		},
		{
			name:    "malformed event",
			body:    "event: response.completed\ndata: {bad-json}\n\n",
			outcome: tokenProbeInconclusive,
		},
		{
			name:    "explicit usage limit",
			body:    "event: response.failed\n" + `data: {"type":"response.failed","response":{"error":{"type":"usage_limit_reached","resets_in_seconds":90}}}` + "\n\n",
			outcome: tokenProbeUsageLimited,
		},
		{
			name:    "ordinary rate limit is not usage limit",
			body:    "event: response.failed\n" + `data: {"type":"response.failed","response":{"error":{"type":"rate_limit_exceeded","message":"concurrency limit exceeded"}}}` + "\n\n",
			outcome: tokenProbeInconclusive,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := readStrictProbeStream(t.Context(), http.StatusOK, strings.NewReader(test.body), now)
			if got.Outcome != test.outcome {
				t.Fatalf("outcome = %s, want %s; detail=%q", got.Outcome, test.outcome, got.Detail)
			}
			if got.Outcome == tokenProbeUsageLimited {
				if !got.UsageLimit.ExplicitKind || got.UsageLimit.ResetAt == nil || !got.UsageLimit.ResetAt.Equal(now.Add(90*time.Second)) {
					t.Fatalf("usage limit metadata = %+v", got.UsageLimit)
				}
			}
		})
	}
}

func TestStrictProbeStreamReadErrorCannotReactivate(t *testing.T) {
	reader := io.MultiReader(
		bytes.NewBufferString("event: response.created\n"+`data: {"type":"response.created"}`+"\n\n"),
		probeErrorReader{err: errors.New("connection reset")},
	)
	got := readStrictProbeStream(context.Background(), http.StatusOK, reader, time.Now().UTC())
	if got.Outcome != tokenProbeInconclusive {
		t.Fatalf("read error outcome = %s, want inconclusive", got.Outcome)
	}
}

type probeErrorReader struct {
	err error
}

func (r probeErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}
