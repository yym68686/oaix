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

func TestProbeTokenRefreshesLatestAccessTokenAndUsesStreamingPayload(t *testing.T) {
	var oauthRefreshSeen bool
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("oauth method = %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "rt-fixture" {
			t.Fatalf("unexpected oauth form: %v", r.Form)
		}
		oauthRefreshSeen = true
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":  "fresh-access-token",
			"refresh_token": "rt-next",
			"expires_in":    3600,
			"account_id":    "acct_refreshed",
			"email":         "probe@example.com",
			"plan_type":     "pro",
		})
	}))
	defer oauthServer.Close()

	var upstreamSeen bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer fresh-access-token" {
			t.Fatalf("authorization = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Fatalf("accept = %q", got)
		}
		if got := r.Header.Get("Session_id"); !strings.HasPrefix(got, "oaix-admin-probe-7") {
			t.Fatalf("session id = %q", got)
		}
		if got := r.Header.Get("Chatgpt-Account-Id"); got != "acct_refreshed" {
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
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           7,
		AccessToken:  "stale-access-token",
		RefreshToken: "rt-fixture",
	}, "gpt-5.5")

	if !oauthRefreshSeen {
		t.Fatal("oauth refresh was not called")
	}
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

func TestProbeTokenDisablesPermanentlyInvalidRefreshToken(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"code":    "app_session_terminated",
				"message": "Your session has ended. Please log in again.",
			},
		})
	}))
	defer oauthServer.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called when refresh token is permanently invalid")
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           9,
		AccessToken:  "stale-access-token",
		RefreshToken: "rt-invalid",
	}, "")

	if result["outcome"] != "disabled" || result["status_code"] != http.StatusBadRequest {
		t.Fatalf("unexpected result: %#v", result)
	}
	if !strings.Contains(result["message"].(string), "refresh token 已失效") {
		t.Fatalf("message = %q", result["message"])
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
