package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func TestCodexFingerprintSessionModeDerivations(t *testing.T) {
	accountID := "acct-shared"
	claim := fingerprintTestClaim(7, &accountID)
	contextA := &CodexFingerprintContext{ClientSessionID: "client-a", TurnID: "turn-a", TurnStartedAt: 123}
	contextB := &CodexFingerprintContext{ClientSessionID: "client-b", TurnID: "turn-b", TurnStartedAt: 456}

	idsA := resolveCodexFingerprintIDs(claim, contextA)
	idsAAgain := resolveCodexFingerprintIDs(claim, contextA)
	idsB := resolveCodexFingerprintIDs(claim, contextB)
	if idsA == nil || idsB == nil {
		t.Fatal("expected fingerprint IDs")
	}
	if idsA.InstallationID != idsAAgain.InstallationID || idsA.SessionID != idsAAgain.SessionID || idsA.ThreadID != idsAAgain.ThreadID {
		t.Fatalf("same account/session was not stable: first=%+v second=%+v", idsA, idsAAgain)
	}
	if idsA.InstallationID != idsB.InstallationID || idsA.SessionID != idsB.SessionID {
		t.Fatalf("account-level IDs changed across clients: a=%+v b=%+v", idsA, idsB)
	}
	if idsA.ThreadID == idsB.ThreadID {
		t.Fatalf("different client sessions shared thread ID %q", idsA.ThreadID)
	}
	if idsA.TurnID != "turn-a" || idsB.TurnID != "turn-b" {
		t.Fatalf("request-level turn IDs were not preserved: a=%q b=%q", idsA.TurnID, idsB.TurnID)
	}
	for name, value := range map[string]string{
		"installation": idsA.InstallationID,
		"session":      idsA.SessionID,
		"thread":       idsA.ThreadID,
	} {
		assertUUIDVersion(t, name, value, '4')
	}
}

func TestCodexFingerprintFallsBackToSessionThreadWithoutClientSession(t *testing.T) {
	claim := fingerprintTestClaim(9, nil)
	ids := resolveCodexFingerprintIDs(claim, &CodexFingerprintContext{TurnID: "turn", TurnStartedAt: 1})
	if ids == nil {
		t.Fatal("expected fingerprint IDs")
	}
	if ids.ThreadID != ids.SessionID || ids.WindowID != ids.SessionID+":0" {
		t.Fatalf("unexpected missing-client fallback: %+v", ids)
	}
}

func TestCodexFingerprintLogFieldsUseActualAccountSession(t *testing.T) {
	claim := fingerprintTestClaim(10, nil)
	context := &CodexFingerprintContext{ClientSessionID: "client", TurnID: "turn", TurnStartedAt: 1}
	ids := resolveCodexFingerprintIDs(claim, context)
	hash, source := codexFingerprintLogFields(claim, context)
	if hash == nil || *hash != shortHash(ids.SessionID, 64) {
		t.Fatalf("logged session hash does not match upstream session: got=%v want=%q", hash, shortHash(ids.SessionID, 64))
	}
	if source == nil || *source != "codex_account_session" {
		t.Fatalf("logged session source = %v", source)
	}
}

func TestCodexFingerprintHeadersAndBodyStayConsistent(t *testing.T) {
	claim := fingerprintTestClaim(11, nil)
	context := &CodexFingerprintContext{ClientSessionID: "client-session", TurnID: "turn-request", TurnStartedAt: 789}
	ids := resolveCodexFingerprintIDs(claim, context)
	headers := http.Header{}
	headers.Set("x-codex-turn-metadata", `{"installation_id":"old","session_id":"old","thread_id":"old","turn_id":"old","window_id":"old:0","sandbox":"seccomp"}`)
	applyCodexFingerprintHeaders(headers, ids)

	body, err := applyCodexFingerprintBody([]byte(`{"model":"gpt-5.6","input":[],"client_metadata":{"x-codex-turn-metadata":"{\"installation_id\":\"old\",\"session_id\":\"old\",\"thread_id\":\"old\",\"turn_id\":\"old\",\"window_id\":\"old:0\",\"sandbox\":\"seccomp\"}"}}`), ids)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	metadata := payload["client_metadata"].(map[string]any)
	if headers.Get("session-id") != ids.SessionID || headers.Get("session_id") != ids.SessionID {
		t.Fatalf("session headers not converged: %v", headers)
	}
	if headers.Get("thread-id") != ids.ThreadID || headers.Get("x-client-request-id") != ids.ThreadID {
		t.Fatalf("thread headers not converged: %v", headers)
	}
	if metadata["x-codex-installation-id"] != ids.InstallationID || metadata["session_id"] != ids.SessionID || metadata["thread_id"] != ids.ThreadID || metadata["turn_id"] != ids.TurnID {
		t.Fatalf("body metadata does not match resolved IDs: %+v", metadata)
	}

	var headerTurnMetadata map[string]any
	if err := json.Unmarshal([]byte(headers.Get("x-codex-turn-metadata")), &headerTurnMetadata); err != nil {
		t.Fatal(err)
	}
	var bodyTurnMetadata map[string]any
	if err := json.Unmarshal([]byte(metadata["x-codex-turn-metadata"].(string)), &bodyTurnMetadata); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"installation_id", "session_id", "thread_id", "turn_id", "window_id", "turn_started_at_unix_ms", "sandbox"} {
		if headerTurnMetadata[key] != bodyTurnMetadata[key] {
			t.Fatalf("turn metadata field %q differs: header=%v body=%v", key, headerTurnMetadata[key], bodyTurnMetadata[key])
		}
	}
}

func TestCodexFingerprintContextUsesFreshTurnPerRequest(t *testing.T) {
	headers := http.Header{}
	headers.Set("session-id", "client-session")
	intent := RequestIntent{Endpoint: "/v1/responses"}
	first := buildCodexFingerprintContext(headers, intent)
	second := buildCodexFingerprintContext(headers, intent)
	if first == nil || second == nil {
		t.Fatal("expected contexts")
	}
	if first.ClientSessionID != "client-session" || second.ClientSessionID != "client-session" {
		t.Fatalf("client session was not captured: first=%+v second=%+v", first, second)
	}
	if first.TurnID == second.TurnID {
		t.Fatalf("turn ID was reused across requests: %q", first.TurnID)
	}
	assertUUIDVersion(t, "turn", first.TurnID, '7')
	if compact := buildCodexFingerprintContext(headers, RequestIntent{Endpoint: "/v1/responses/compact", Compact: true}); compact != nil {
		t.Fatalf("compact request unexpectedly received fingerprint context: %+v", compact)
	}
	if image := buildCodexFingerprintContext(headers, RequestIntent{Endpoint: "/v1/images/generations", UpstreamEndpoint: "/v1/responses"}); image == nil {
		t.Fatal("Responses-backed image request did not receive fingerprint context")
	}
}

func TestDoAttemptAppliesDefaultSessionFingerprintToWireRequest(t *testing.T) {
	type observedRequest struct {
		Headers http.Header
		Body    map[string]any
	}
	observed := make(chan observedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode upstream body: %v", err)
		}
		observed <- observedRequest{Headers: r.Header.Clone(), Body: payload}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp-fingerprint","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:              upstream.URL,
		NonStreamMaxResponseBytes: 1 << 20,
		DisableCompression:        true,
	}}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	pipeline := &Pipeline{cfg: cfg, transport: client}
	accountID := "acct-wire"
	claim := fingerprintTestClaim(21, &accountID)
	claim.Token.Token.AccessToken = "upstream-token"
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6","input":[]}`))
	request.Header.Set("session-id", "client-wire-session")
	request.Header.Set("session_id", "client-wire-underscore")
	request.Header.Set("x-codex-turn-metadata", `{"installation_id":"old","session_id":"old","thread_id":"old","turn_id":"old","window_id":"old:0"}`)
	requestContext := buildCodexFingerprintContext(request.Header, RequestIntent{Endpoint: "/v1/responses"})

	result, err := pipeline.doAttempt(httptest.NewRecorder(), request, Attempt{
		RequestID:        "fingerprint-wire",
		Intent:           RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.6"},
		Claim:            claim,
		Body:             []byte(`{"model":"gpt-5.6","input":[]}`),
		PromptCache:      &PromptCacheContext{SessionID: "prompt-cache-session"},
		CodexFingerprint: requestContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.Status != http.StatusOK {
		t.Fatalf("unexpected attempt result: %+v", result)
	}
	wire := <-observed
	ids := resolveCodexFingerprintIDs(claim, requestContext)
	if wire.Headers.Get("session-id") != ids.SessionID || wire.Headers.Get("session_id") != ids.SessionID {
		t.Fatalf("wire session headers were not account stable: %v", wire.Headers)
	}
	if wire.Headers.Get("session_id") == "prompt-cache-session" {
		t.Fatal("prompt-cache session won over default Codex account session")
	}
	if wire.Headers.Get("thread-id") != ids.ThreadID || wire.Headers.Get("x-client-request-id") != ids.ThreadID {
		t.Fatalf("wire thread headers were not client-derived: %v", wire.Headers)
	}
	metadata := wire.Body["client_metadata"].(map[string]any)
	if metadata["session_id"] != ids.SessionID || metadata["thread_id"] != ids.ThreadID || metadata["turn_id"] != ids.TurnID {
		t.Fatalf("wire body metadata does not match headers: %+v", metadata)
	}
}

func fingerprintTestClaim(tokenID int64, accountID *string) *tokens.Claim {
	return &tokens.Claim{Token: &tokens.RuntimeToken{Token: store.Token{ID: tokenID, AccountID: accountID}}}
}

func assertUUIDVersion(t *testing.T, name, value string, version byte) {
	t.Helper()
	parts := strings.Split(value, "-")
	if len(parts) != 5 || len(parts[2]) != 4 || parts[2][0] != version {
		t.Fatalf("%s ID is not UUIDv%c: %q", name, version, value)
	}
}
