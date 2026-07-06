package httpapi

import (
	"encoding/json"
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
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"model":"gpt-5.5"}}` + "\n\n"))
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
		upstreamSeen = true
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"model":"gpt-5.5"}}` + "\n\n"))
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

func TestExtractProbeStreamResultPreservesFailureRawPayload(t *testing.T) {
	body := []byte(strings.Join([]string{
		"event: response.failed",
		`data: {"type":"response.failed","response":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":120}}}`,
		"",
	}, "\n"))

	detail, raw, model := extractProbeStreamResult(t.Context(), body)
	if detail == "" || !strings.Contains(detail, "usage limit reached") {
		t.Fatalf("detail = %q", detail)
	}
	if model != "" {
		t.Fatalf("model = %q", model)
	}
	if !strings.Contains(raw, `"resets_in_seconds":120`) {
		t.Fatalf("raw failure payload lost reset metadata: %q", raw)
	}
}
