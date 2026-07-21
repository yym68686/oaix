package httpapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
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

func TestProbeTokenUsesAgentIdentityAssertion(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-ready")
	credentialStore := &fakeAgentIdentityProbeStore{credentials: credentials}
	var upstreamSeen bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if taskID := decodeProbeAgentAssertionTask(t, r.Header.Get("Authorization")); taskID != credentials.TaskID {
			t.Fatalf("assertion task = %q, want %q", taskID, credentials.TaskID)
		}
		if got := r.Header.Get("ChatGPT-Account-ID"); got != credentials.AccountID {
			t.Fatalf("account id = %q", got)
		}
		if got := r.Header.Get("X-OpenAI-FedRAMP"); got != "true" {
			t.Fatalf("fedramp header = %q", got)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Fatalf("accept = %q", got)
		}
		upstreamSeen = true
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\n")
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.6-sol"}}`+"\n\n")
	}))
	defer upstream.Close()

	app := &App{
		cfg:                config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}},
		probeIdentityStore: credentialStore,
	}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           14536,
		OwnerUserID:  1,
		RefreshToken: credentials.IdentityToken(),
	}, "gpt-5.6-sol")

	if !upstreamSeen {
		t.Fatal("agent identity probe did not reach upstream")
	}
	if result["outcome"] != "reactivated" || result["status_code"] != http.StatusOK {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result["credential_mode"] != probeCredentialModeAgentIdentity || result["upstream_attempted"] != true {
		t.Fatalf("probe metadata = %#v", result)
	}
	if result["probe_stage"] != probeStageUpstreamResponse {
		t.Fatalf("probe stage = %#v", result["probe_stage"])
	}
}

func TestProbeTokenRecoversInvalidAgentIdentityTaskExactlyOnce(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-old")
	credentialStore := &fakeAgentIdentityProbeStore{credentials: credentials}
	var registrationCalls int
	registration := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registrationCalls++
		_, _ = io.WriteString(w, `{"task_id":"task-new"}`)
	}))
	defer registration.Close()

	var upstreamCalls int
	var tasks []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		tasks = append(tasks, decodeProbeAgentAssertionTask(t, r.Header.Get("Authorization")))
		if upstreamCalls == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_task_id"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\n")
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}`+"\n\n")
	}))
	defer upstream.Close()

	app := &App{
		cfg: config.Config{Upstream: config.UpstreamConfig{
			ResponsesURL:            upstream.URL,
			AgentIdentityAuthAPIURL: registration.URL,
		}},
		probeIdentityStore: credentialStore,
	}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           72,
		OwnerUserID:  8,
		RefreshToken: credentials.IdentityToken(),
	}, defaultAdminProbeModel)

	if result["outcome"] != "reactivated" {
		t.Fatalf("probe result = %#v", result)
	}
	if upstreamCalls != 2 || registrationCalls != 1 || credentialStore.updateCalls != 1 {
		t.Fatalf("calls: upstream=%d registration=%d updates=%d", upstreamCalls, registrationCalls, credentialStore.updateCalls)
	}
	if len(tasks) != 2 || tasks[0] != "task-old" || tasks[1] != "task-new" {
		t.Fatalf("assertion tasks = %v", tasks)
	}
}

func TestProbeTokenAgentIdentityPreservesUpstream402AndDisables(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-ready")
	upstreamBody := `{"error":{"code":"deactivated_workspace","message":"workspace disabled"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:            14536,
		RefreshToken:  credentials.IdentityToken(),
		AgentIdentity: &credentials,
	}, defaultAdminProbeModel)

	if result["outcome"] != "disabled" || result["status_code"] != http.StatusPaymentRequired {
		t.Fatalf("probe result = %#v", result)
	}
	if result["error_code"] != "deactivated_workspace" || result["raw_response"] != upstreamBody {
		t.Fatalf("upstream evidence was not preserved: %#v", result)
	}
	if result["credential_mode"] != probeCredentialModeAgentIdentity || result["upstream_attempted"] != true {
		t.Fatalf("probe metadata = %#v", result)
	}
}

func TestProbeTokenAgentIdentityAuthRejectionUsesCredentialSpecificMessage(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-ready")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"forbidden"}}`)
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:            73,
		RefreshToken:  credentials.IdentityToken(),
		AgentIdentity: &credentials,
	}, defaultAdminProbeModel)

	if result["outcome"] != "disabled" || result["error_code"] != "agent_identity_auth_rejected" {
		t.Fatalf("probe result = %#v", result)
	}
	if message := fmt.Sprint(result["message"]); !strings.Contains(message, "Agent Identity 鉴权被上游拒绝") {
		t.Fatalf("message = %q", message)
	}
}

func TestProbeTokenReportsLocalCredentialFailureWithoutPretendingUpstreamResponded(t *testing.T) {
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()
	var logOutput bytes.Buffer
	app := &App{
		cfg:    config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}},
		logger: slog.New(slog.NewJSONHandler(&logOutput, nil)),
	}
	result := app.probeTokenWithAccess(t.Context(), store.Token{ID: 14536, OwnerUserID: 1}, defaultAdminProbeModel)

	if upstreamCalls != 0 {
		t.Fatalf("local credential failure made %d upstream calls", upstreamCalls)
	}
	if result["outcome"] != "inconclusive" || result["status_code"] != http.StatusBadRequest {
		t.Fatalf("probe result = %#v", result)
	}
	if result["upstream_attempted"] != false || result["probe_stage"] != probeStageCredentialPreparation || result["error_code"] != "missing_access_token" {
		t.Fatalf("probe metadata = %#v", result)
	}
	if message := fmt.Sprint(result["message"]); !strings.Contains(message, "未向上游发送请求") {
		t.Fatalf("message = %q", message)
	}
	logLine := logOutput.String()
	for _, fragment := range []string{`"token_id":14536`, `"credential_mode":"oauth"`, `"probe_stage":"credential_preparation"`, `"upstream_attempted":false`, `"error_code":"missing_access_token"`} {
		if !strings.Contains(logLine, fragment) {
			t.Fatalf("structured probe log missing %s: %s", fragment, logLine)
		}
	}
}

func TestProbeTokenRefreshesOAuthWhenOnlyRefreshTokenIsStored(t *testing.T) {
	var refreshCalls int
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls++
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := r.Form.Get("refresh_token"); got != "refresh-only" {
			t.Fatalf("refresh token = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"access-refreshed","refresh_token":"refresh-rotated"}`)
	}))
	defer oauthServer.Close()
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if got := r.Header.Get("Authorization"); got != "Bearer access-refreshed" {
			t.Fatalf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\n")
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}`+"\n\n")
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           100,
		RefreshToken: "refresh-only",
	}, defaultAdminProbeModel)

	if refreshCalls != 1 || upstreamCalls != 1 {
		t.Fatalf("calls: refresh=%d upstream=%d", refreshCalls, upstreamCalls)
	}
	if result["outcome"] != "reactivated" || result["upstream_attempted"] != true {
		t.Fatalf("probe result = %#v", result)
	}
}

func TestProbeAcceptsStrictCompletedStreamWhenContentTypeHeaderIsMissing(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Some valid ChatGPT Codex responses omit Content-Type entirely. A nil
		// value suppresses net/http's automatic content sniffing in this fixture.
		w.Header()["Content-Type"] = nil
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"status":"completed","model":"gpt-5.4-mini"}}` + "\n\n"))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	attempt := app.executeTokenProbe(t.Context(), store.Token{ID: 8, AccessToken: "access"}, defaultAdminProbeModel)
	if attempt.Outcome != tokenProbeCompleted || attempt.StatusCode != http.StatusOK {
		t.Fatalf("missing Content-Type rejected a strict completed stream: %+v", attempt)
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

func TestProbeTokenDoesNotRefreshAccessTokenOnlyAfterAuthRejection(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("access-token-only probe must not call OAuth refresh")
	}))
	defer oauthServer.Close()
	upstreamBody := `{"error":{"code":"no_matching_rule","message":"Unauthorized"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           10,
		AccessToken:  "access-only-token",
		RefreshToken: store.AccessTokenOnlyRefreshTokenPrefix + "fixture",
	}, defaultAdminProbeModel)

	if result["outcome"] != "inconclusive" || result["status_code"] != http.StatusUnauthorized {
		t.Fatalf("probe result = %#v", result)
	}
	if result["error_code"] != "oauth_refresh_unavailable" {
		t.Fatalf("error code = %#v", result["error_code"])
	}
	if result["raw_response"] != upstreamBody {
		t.Fatalf("raw response = %#v", result["raw_response"])
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
	if result["raw_response"] != upstreamBody {
		t.Fatalf("raw_response = %#v", result["raw_response"])
	}
	if detail := fmt.Sprint(result["detail"]); !strings.Contains(detail, "upstream unavailable") {
		t.Fatalf("detail = %#v", result["detail"])
	}
}

func TestProbeTokenReturnsRawStreamForUntrustedIncompleteSuccess(t *testing.T) {
	upstreamBody := "event: response.created\n" + `data: {"type":"response.created","response":{"status":"in_progress"}}` + "\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:          12,
		AccessToken: "still-valid-access-token",
	}, "gpt-5.4-mini")

	if result["outcome"] != "inconclusive" || result["status_code"] != http.StatusOK {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result["message"] != "测试未收到可信的完整成功事件，当前状态未改变。" {
		t.Fatalf("message = %#v", result["message"])
	}
	if result["raw_response"] != upstreamBody {
		t.Fatalf("raw_response = %#v", result["raw_response"])
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

func TestProbeTokenDisablesExplicitTokenInvalidatedWithoutOAuthRefresh(t *testing.T) {
	oauthServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("token_invalidated must not trigger an OAuth refresh")
	}))
	defer oauthServer.Close()

	upstreamBody := `{"error":{"message":"Your authentication token has been invalidated. Please try signing in again.","type":"invalid_request_error","code":"token_invalidated","param":null},"status":401}`
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	app := &App{cfg: config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:  upstream.URL,
		OAuthTokenURL: oauthServer.URL,
	}}}
	result := app.probeTokenWithAccess(t.Context(), store.Token{
		ID:           22,
		AccessToken:  "invalidated-access-token",
		RefreshToken: "refresh-token",
	}, defaultAdminProbeModel)

	if upstreamCalls != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstreamCalls)
	}
	if result["outcome"] != "disabled" || result["status_code"] != http.StatusUnauthorized {
		t.Fatalf("probe result = %#v", result)
	}
	if result["raw_response"] != upstreamBody {
		t.Fatalf("raw_response = %#v", result["raw_response"])
	}
	if message := fmt.Sprint(result["message"]); !strings.Contains(message, "token_invalidated") {
		t.Fatalf("message = %q", message)
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
			if got.RawResponse != test.body {
				t.Fatalf("raw response = %q, want %q", got.RawResponse, test.body)
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
	if !strings.Contains(got.RawResponse, `{"type":"response.created"}`) {
		t.Fatalf("partial raw response was lost: %q", got.RawResponse)
	}
}

type probeErrorReader struct {
	err error
}

type fakeAgentIdentityProbeStore struct {
	mu          sync.Mutex
	credentials agentidentity.Credentials
	updateCalls int
}

func (s *fakeAgentIdentityProbeStore) GetAgentIdentityCredentials(context.Context, int64) (agentidentity.Credentials, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.credentials, nil
}

func (s *fakeAgentIdentityProbeStore) UpdateAgentIdentityTask(_ context.Context, _ int64, expectedTaskID string, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(s.credentials.TaskID) != strings.TrimSpace(expectedTaskID) {
		return store.ErrAgentIdentityTaskChanged
	}
	s.credentials.TaskID = strings.TrimSpace(taskID)
	s.updateCalls++
	return nil
}

func probeAgentIdentityCredentials(t *testing.T, taskID string) agentidentity.Credentials {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return agentidentity.Credentials{
		RuntimeID:   "runtime-probe",
		PrivateKey:  base64.StdEncoding.EncodeToString(der),
		TaskID:      taskID,
		AccountID:   "workspace-probe",
		UserID:      "user-probe",
		WorkspaceID: "workspace-probe",
		FedRAMP:     true,
	}
}

func decodeProbeAgentAssertionTask(t *testing.T, authorization string) string {
	t.Helper()
	if !strings.HasPrefix(authorization, "AgentAssertion ") {
		t.Fatalf("authorization = %q", authorization)
	}
	encoded := strings.TrimPrefix(authorization, "AgentAssertion ")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.TaskID
}

func (r probeErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}
