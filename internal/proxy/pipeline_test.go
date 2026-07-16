package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func TestClassifyUpstreamFailures(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{name: "429", status: http.StatusTooManyRequests, want: OutcomeUpstream429Cooldown},
		{name: "401", status: http.StatusUnauthorized, want: OutcomeUpstream401Invalid},
		{name: "403", status: http.StatusForbidden, want: OutcomeUpstream403Invalid},
		{name: "5xx", status: http.StatusBadGateway, want: OutcomeUpstream5xx},
		{name: "transport", status: http.StatusBadGateway, err: errors.New("dial failed"), want: OutcomeUpstream5xx},
		{name: "client canceled", status: 499, err: context.Canceled, want: OutcomeClientCanceled},
		{name: "success", status: http.StatusOK, want: OutcomeSuccess},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classify(tt.status, tt.err); got != tt.want {
				t.Fatalf("classify(%d, %v) = %s, want %s", tt.status, tt.err, got, tt.want)
			}
		})
	}
}

func TestRequiresNonFreeTokenPlan(t *testing.T) {
	tests := []struct {
		model string
		want  bool
	}{
		{model: "gpt-5.6-sol", want: true},
		{model: " GPT-5.6-SOL ", want: true},
		{model: "gpt-5.6-sol-2026-07-01", want: true},
		{model: "gpt-5.6-terra", want: false},
		{model: "gpt-5.5", want: false},
		{model: "gpt-5.6-solar", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			if got := requiresNonFreeTokenPlan(tt.model); got != tt.want {
				t.Fatalf("requiresNonFreeTokenPlan(%q) = %v, want %v", tt.model, got, tt.want)
			}
		})
	}
}

func TestIsDeactivatedWorkspaceFailure(t *testing.T) {
	if !isDeactivatedWorkspaceFailure(http.StatusPaymentRequired, []byte(`{"error":{"code":"deactivated_workspace"}}`), nil) {
		t.Fatal("expected deactivated workspace body to be terminal")
	}
	if !isDeactivatedWorkspaceFailure(http.StatusPaymentRequired, nil, errors.New("map[code:deactivated_workspace]")) {
		t.Fatal("expected deactivated workspace error text to be terminal")
	}
	if isDeactivatedWorkspaceFailure(http.StatusPaymentRequired, []byte(`{"error":{"code":"usage_limit_reached"}}`), nil) {
		t.Fatal("ordinary 402 should not be terminal")
	}
	if isDeactivatedWorkspaceFailure(http.StatusTooManyRequests, []byte(`{"error":{"code":"deactivated_workspace"}}`), nil) {
		t.Fatal("non-402 deactivated marker should not match")
	}
}

func TestWriteSSEError(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeSSEError(recorder, http.StatusServiceUnavailable, "no token")
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("content-type = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "no token") {
		t.Fatalf("unexpected SSE body: %q", body)
	}
}

func TestWriteFinalErrorResponseDoesNotRewriteStartedStreamHeader(t *testing.T) {
	recorder := &countingResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
	recorder.Header().Set("Content-Type", "text/event-stream")
	recorder.WriteHeader(http.StatusOK)

	writeFinalErrorResponse(recorder, true, true, http.StatusServiceUnavailable, "no token")

	if recorder.writeHeaderCount != 1 {
		t.Fatalf("WriteHeader called %d times, want 1", recorder.writeHeaderCount)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want original 200", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "no token") {
		t.Fatalf("unexpected SSE body: %q", body)
	}
}

func TestWriteFinalErrorResponsePreservesStartedResponsesFailure(t *testing.T) {
	payload := []byte(`{"type":"response.failed","sequence_number":3,"response":{"id":"resp_ctx","object":"response","status":"failed","error":{"code":"context_length_exceeded","message":"input is too long"}}}`)
	failure := &responsesFailureTerminal{
		data:         payload,
		eventOrdinal: 2,
		status:       http.StatusBadRequest,
		message:      "input is too long",
	}
	recorder := &countingResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
	recorder.Header().Set("Content-Type", "text/event-stream")
	recorder.WriteHeader(http.StatusOK)
	trace := store.NewStreamDeliveryTrace("conn-failed")
	trace.Upstream.FirstResponseFailed = &store.StreamDeliveryCompletedUpstream{}

	if err := writeResponsesFailureResponse(recorder, true, failure, trace); err != nil {
		t.Fatal(err)
	}

	if recorder.writeHeaderCount != 1 || recorder.Code != http.StatusOK {
		t.Fatalf("started response header changed: writes=%d status=%d", recorder.writeHeaderCount, recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: response.failed") || !strings.Contains(body, `"code":"context_length_exceeded"`) {
		t.Fatalf("structured failure was not preserved: %q", body)
	}
	if strings.Contains(body, "event: error") || strings.Contains(body, "oaix_gateway_error") || strings.Contains(body, "[DONE]") {
		t.Fatalf("structured failure was downgraded or followed by a fake terminator: %q", body)
	}
	if trace.State() != "terminal_flushed" || trace.End.Reason != "upstream_response_failed_terminal_delivered" || trace.Downstream.FinalBodyProducedAt == nil {
		t.Fatalf("unexpected delivered failure trace: %+v", trace)
	}
}

func TestWriteFinalErrorResponseUsesJSONBeforeStreamStart(t *testing.T) {
	payload := []byte(`{"type":"response.failed","sequence_number":1,"response":{"id":"resp_ctx","object":"response","status":"failed","error":{"code":"context_length_exceeded","message":"input is too long"}}}`)
	failure := &responsesFailureTerminal{
		data:    payload,
		status:  http.StatusBadRequest,
		message: "input is too long",
	}
	recorder := httptest.NewRecorder()
	trace := store.NewStreamDeliveryTrace("conn-http-error")
	trace.Upstream.FirstResponseFailed = &store.StreamDeliveryCompletedUpstream{}

	if err := writeResponsesFailureResponse(recorder, false, failure, trace); err != nil {
		t.Fatal(err)
	}

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("content-type = %q", got)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	errorObject, _ := body["error"].(map[string]any)
	if errorObject["code"] != "context_length_exceeded" || errorObject["message"] != "input is too long" {
		t.Fatalf("unexpected error body: %#v", body)
	}
	if trace.End.Reason != "upstream_response_failed_http_error_delivered" || trace.Downstream.FinalBodyProducedAt == nil {
		t.Fatalf("unexpected HTTP error trace: %+v", trace)
	}
	if trace.State() != "http_error_delivered" {
		t.Fatalf("HTTP error state = %q", trace.State())
	}
}

func TestWriteFinalResponsesFailureDoesNotAppendFallbackAfterFlushError(t *testing.T) {
	payload := []byte(`{"type":"response.failed","sequence_number":3,"response":{"id":"resp_ctx","status":"failed","error":{"code":"context_length_exceeded","message":"input is too long"}}}`)
	failure := &responsesFailureTerminal{data: payload, status: http.StatusBadRequest, message: "input is too long"}
	flushErr := errors.New("forced terminal flush failure")
	writer := &flushErrorResponseWriter{header: make(http.Header), flushErr: flushErr}
	trace := store.NewStreamDeliveryTrace("conn-flush-failed")
	trace.Upstream.FirstResponseFailed = &store.StreamDeliveryCompletedUpstream{}

	err := writeResponsesFailureResponse(writer, true, failure, trace)

	if !errors.Is(err, flushErr) {
		t.Fatalf("error = %v, want flush failure", err)
	}
	body := string(writer.body.Bytes())
	if strings.Count(body, "event: response.failed") != 1 || strings.Contains(body, "event: error") {
		t.Fatalf("flush failure must not append a second terminal: %q", body)
	}
	if trace.State() != "terminal_flush_failed" || trace.End.Reason != "downstream_terminal_flush_error" || trace.End.Error == "" {
		t.Fatalf("unexpected failed flush trace: %+v", trace)
	}
}

func TestCopyAndFlushStopsOnClientWriteError(t *testing.T) {
	writer := failingWriter{failAfter: 5}
	written, err := copyAndFlush(writer, strings.NewReader("hello world"), nil)
	if err == nil {
		t.Fatal("expected write error")
	}
	if written == 0 {
		t.Fatal("expected partial write count")
	}
}

func TestReadProxyRequestBodyDecodesZstd(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":"hello"}`)
	compressed := zstdEncodeForTest(t, body)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(compressed))
	req.Header.Set("Content-Encoding", "zstd")

	got, status, message, err := readProxyRequestBody(req, 1<<20)
	if err != nil {
		t.Fatalf("readProxyRequestBody returned status=%d message=%q err=%v", status, message, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("decoded body = %q, want %q", got, body)
	}
}

func TestReadProxyRequestBodyRejectsInvalidZstd(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("not zstd"))
	req.Header.Set("Content-Encoding", "zstd")

	_, status, message, err := readProxyRequestBody(req, 1<<20)
	if err == nil {
		t.Fatal("expected invalid zstd error")
	}
	if status != http.StatusBadRequest || message != "invalid zstd body" {
		t.Fatalf("status/message = %d %q", status, message)
	}
}

func TestReadProxyRequestBodyRejectsUnsupportedEncoding(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("body"))
	req.Header.Set("Content-Encoding", "gzip")

	_, status, message, err := readProxyRequestBody(req, 1<<20)
	if err == nil {
		t.Fatal("expected unsupported encoding error")
	}
	if status != http.StatusUnsupportedMediaType || !strings.Contains(message, "unsupported content encoding: gzip") {
		t.Fatalf("status/message = %d %q", status, message)
	}
}

func TestReadProxyRequestBodyRejectsOversizedDecodedZstd(t *testing.T) {
	compressed := zstdEncodeForTest(t, []byte("0123456789"))
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(compressed))
	req.Header.Set("Content-Encoding", "zstd")

	_, status, message, err := readProxyRequestBody(req, 5)
	if err == nil {
		t.Fatal("expected oversized body error")
	}
	if status != http.StatusRequestEntityTooLarge || message != "request body too large" {
		t.Fatalf("status/message = %d %q", status, message)
	}
}

func TestReadProxyRequestBodyAllowsUnlimitedBodyWhenLimitZero(t *testing.T) {
	body := []byte("0123456789")
	compressed := zstdEncodeForTest(t, body)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(compressed))
	req.Header.Set("Content-Encoding", "zstd")

	got, status, message, err := readProxyRequestBody(req, 0)
	if err != nil {
		t.Fatalf("readProxyRequestBody returned status=%d message=%q err=%v", status, message, err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("decoded body = %q, want %q", got, body)
	}
}

func TestProxyUsesRequestBodyLimitForIngress(t *testing.T) {
	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			MaxRequestBodyBytes:       5,
			NonStreamMaxResponseBytes: 1 << 20,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	pipeline := New(cfg, logger, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("0123456789"))
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses"})

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "request body too large") {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestCopyProxyHeadersStripsBodyEncodingHeaders(t *testing.T) {
	src := http.Header{}
	src.Set("Content-Encoding", "zstd")
	src.Set("Accept-Encoding", "zstd")
	src.Set("Authorization", "Bearer client")
	src.Set("X-API-Key", "client-api-key")
	src.Set("Proxy-Authorization", "Basic client-proxy")
	src.Set("Cookie", "session=client")
	src.Set("ChatGPT-Account-ID", "client-account")
	src.Set("X-Custom", "ok")
	dst := http.Header{}

	copyProxyHeaders(dst, src)

	if got := dst.Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding forwarded as %q", got)
	}
	if got := dst.Get("Accept-Encoding"); got != "" {
		t.Fatalf("Accept-Encoding forwarded as %q", got)
	}
	if got := dst.Get("Authorization"); got != "" {
		t.Fatalf("Authorization forwarded as %q", got)
	}
	if got := dst.Get("X-API-Key"); got != "" {
		t.Fatalf("X-API-Key forwarded as %q", got)
	}
	if got := dst.Get("Proxy-Authorization"); got != "" {
		t.Fatalf("Proxy-Authorization forwarded as %q", got)
	}
	if got := dst.Get("Cookie"); got != "" {
		t.Fatalf("Cookie forwarded as %q", got)
	}
	if got := dst.Get("ChatGPT-Account-ID"); got != "" {
		t.Fatalf("ChatGPT-Account-ID forwarded as %q", got)
	}
	if got := dst.Get("X-Custom"); got != "ok" {
		t.Fatalf("X-Custom = %q", got)
	}
}

func TestCopyResponseHeadersStripsUpstreamCookies(t *testing.T) {
	src := http.Header{}
	src.Add("Set-Cookie", "first=must-not-leak")
	src.Add("Set-Cookie", "second=must-not-leak")
	src.Add("Set-Cookie2", "legacy=must-not-leak")
	src.Set("Content-Length", "123")
	src.Set("Connection", "close")
	src.Set("Content-Type", "application/json")
	src.Set("Retry-After", "5")
	dst := http.Header{}

	copyResponseHeaders(dst, src)

	if got := dst.Values("Set-Cookie"); len(got) != 0 {
		t.Fatalf("Set-Cookie forwarded as %v", got)
	}
	if got := dst.Values("Set-Cookie2"); len(got) != 0 {
		t.Fatalf("Set-Cookie2 forwarded as %v", got)
	}
	if got := dst.Get("Content-Length"); got != "" {
		t.Fatalf("Content-Length forwarded as %q", got)
	}
	if got := dst.Get("Connection"); got != "" {
		t.Fatalf("Connection forwarded as %q", got)
	}
	if got := dst.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := dst.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q", got)
	}
}

func TestAlphaSearchUpstreamURLUsesResponsesSibling(t *testing.T) {
	tests := []struct {
		base string
		want string
	}{
		{
			base: "https://chatgpt.com/backend-api/codex/responses",
			want: "https://chatgpt.com/backend-api/codex/alpha/search",
		},
		{
			base: "https://provider.example/v1/responses/compact?region=us",
			want: "https://provider.example/v1/alpha/search?region=us",
		},
		{
			base: "https://provider.example/v1/alpha/search",
			want: "https://provider.example/v1/alpha/search",
		},
	}
	for _, tt := range tests {
		got, err := alphaSearchUpstreamURL(tt.base)
		if err != nil {
			t.Fatalf("alphaSearchUpstreamURL(%q): %v", tt.base, err)
		}
		if got != tt.want {
			t.Fatalf("alphaSearchUpstreamURL(%q) = %q, want %q", tt.base, got, tt.want)
		}
	}
	if _, err := alphaSearchUpstreamURL("https://provider.example/v1/chat/completions"); err == nil {
		t.Fatal("expected an error for a non-responses CODEX_BASE_URL")
	}
}

func TestAlphaSearchPayloadAndResponseValidation(t *testing.T) {
	validRequest := []byte(`{"id":"session-1","model":"gpt-5.4","commands":{"search_query":[{"q":"news"}]}}`)
	if err := validateAlphaSearchPayload(validRequest); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for _, body := range [][]byte{
		[]byte(`{"model":"gpt-5.4"}`),
		[]byte(`{"id":"session-1"}`),
		[]byte(`[]`),
	} {
		if err := validateAlphaSearchPayload(body); err == nil {
			t.Fatalf("invalid request accepted: %s", body)
		}
	}

	if err := validateAlphaSearchResponse([]byte(`{"encrypted_output":null,"output":"ok","future":true}`)); err != nil {
		t.Fatalf("valid response rejected: %v", err)
	}
	for _, body := range [][]byte{
		[]byte(`{"output":["not-a-string"]}`),
		[]byte(`{"id":"resp_1","object":"response","output":[]}`),
		[]byte(`{"output":"ok","encrypted_output":42}`),
	} {
		if err := validateAlphaSearchResponse(body); err == nil {
			t.Fatalf("invalid response accepted: %s", body)
		}
	}
}

func TestProxyAlphaSearchPreservesWireContractAndTokenAffinity(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var bodies []string
	var authorizations []string
	var xAPIKeys []string
	var accountIDs []string
	var accepts []string
	upstreamResponse := `{"encrypted_output":null,"output":"result with turn0search0","future":true}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, string(raw))
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		xAPIKeys = append(xAPIKeys, r.Header.Get("X-API-Key"))
		accountIDs = append(accountIDs, r.Header.Get("ChatGPT-Account-ID"))
		accepts = append(accepts, r.Header.Get("Accept"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Add("Set-Cookie", "upstream_session=must-not-leak; Secure; HttpOnly")
		w.Header().Add("Set-Cookie2", "legacy_upstream_session=must-not-leak; Secure; HttpOnly")
		_, _ = io.WriteString(w, upstreamResponse)
	}))
	defer upstream.Close()

	accountA := "account-a"
	accountB := "account-b"
	now := time.Now().UTC()
	fakes := &fakeProxyStore{tokens: []store.Token{
		{ID: 1, AccessToken: "upstream-token-a", AccountID: &accountA, IsActive: true, CreatedAt: now, UpdatedAt: now},
		{ID: 2, AccessToken: "upstream-token-b", AccountID: &accountB, IsActive: true, CreatedAt: now, UpdatedAt: now},
	}}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL+"/backend-api/codex/responses", 3, fakes)
	body := `{"id":"session-contract-001","model":"gpt-5.4","input":[{"reasoning_content":"keep-me"}],"commands":{"search_query":[{"q":"news"}]},"settings":{"external_web_access":true},"max_output_tokens":2500,"stream":true}`
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, alphaSearchEndpoint, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer caller-secret")
		req.Header.Set("X-API-Key", "caller-x-api-key")
		req.Header.Set("ChatGPT-Account-ID", "client-controlled-account")
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		pipeline.Proxy(recorder, req, RequestIntent{
			Endpoint:            alphaSearchEndpoint,
			Method:              http.MethodPost,
			UpstreamContentType: "application/json",
			UpstreamAccept:      "application/json",
		})
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
		}
		if recorder.Body.String() != upstreamResponse {
			t.Fatalf("response body changed: %q", recorder.Body.String())
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q, want no-store", got)
		}
		if got := recorder.Header().Values("Set-Cookie"); len(got) != 0 {
			t.Fatalf("upstream Set-Cookie leaked downstream: %v", got)
		}
		if got := recorder.Header().Values("Set-Cookie2"); len(got) != 0 {
			t.Fatalf("upstream Set-Cookie2 leaked downstream: %v", got)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 || paths[0] != "/backend-api/codex/alpha/search" || paths[1] != paths[0] {
		t.Fatalf("upstream paths = %v", paths)
	}
	if len(bodies) != 2 || bodies[0] != body || bodies[1] != body {
		t.Fatalf("upstream bodies changed: %v", bodies)
	}
	if len(authorizations) != 2 || authorizations[0] == "Bearer caller-secret" || authorizations[0] != authorizations[1] {
		t.Fatalf("upstream authorization was not replaced and pinned: %v", authorizations)
	}
	if len(xAPIKeys) != 2 || xAPIKeys[0] != "" || xAPIKeys[1] != "" {
		t.Fatalf("caller X-API-Key leaked upstream: %v", xAPIKeys)
	}
	if len(accountIDs) != 2 || accountIDs[0] == "client-controlled-account" || accountIDs[0] != accountIDs[1] || accountIDs[0] == "" {
		t.Fatalf("upstream account id was not replaced and pinned: %v", accountIDs)
	}
	if (authorizations[0] == "Bearer upstream-token-a" && accountIDs[0] != accountA) ||
		(authorizations[0] == "Bearer upstream-token-b" && accountIDs[0] != accountB) {
		t.Fatalf("upstream token/account mismatch: auth=%q account=%q", authorizations[0], accountIDs[0])
	}
	if len(accepts) != 2 || accepts[0] != "application/json" || accepts[1] != "application/json" {
		t.Fatalf("upstream accept headers = %v", accepts)
	}
}

func TestProxyAlphaSearchDoesNotFailOverAcrossAccounts(t *testing.T) {
	var calls atomic.Int64
	upstreamError := `{"error":{"message":"rate limited"}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "5")
		w.Header().Add("Set-Cookie", "upstream_error_session=must-not-leak; Secure; HttpOnly")
		w.Header().Add("Set-Cookie2", "legacy_upstream_error_session=must-not-leak; Secure; HttpOnly")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, upstreamError)
	}))
	defer upstream.Close()

	now := time.Now().UTC()
	accountA := "account-a"
	accountB := "account-b"
	fakes := &fakeProxyStore{tokens: []store.Token{
		{ID: 1, AccessToken: "upstream-token-a", AccountID: &accountA, IsActive: true, CreatedAt: now, UpdatedAt: now},
		{ID: 2, AccessToken: "upstream-token-b", AccountID: &accountB, IsActive: true, CreatedAt: now, UpdatedAt: now},
	}}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL+"/v1/responses", 3, fakes)
	req := httptest.NewRequest(http.MethodPost, alphaSearchEndpoint, strings.NewReader(`{"id":"session-rate-limit","model":"gpt-5.4","commands":{"search_query":[{"q":"news"}]}}`))
	req.Header.Set("Authorization", "Bearer caller-secret")
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: alphaSearchEndpoint, UpstreamAccept: "application/json"})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 pinned account attempt", got)
	}
	if recorder.Body.String() != upstreamError {
		t.Fatalf("upstream error body changed: %q", recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := recorder.Header().Values("Set-Cookie"); len(got) != 0 {
		t.Fatalf("upstream error Set-Cookie leaked downstream: %v", got)
	}
	if got := recorder.Header().Values("Set-Cookie2"); len(got) != 0 {
		t.Fatalf("upstream error Set-Cookie2 leaked downstream: %v", got)
	}
	fakes.mu.Lock()
	defer fakes.mu.Unlock()
	if len(fakes.attempts) != 1 {
		t.Fatalf("recorded attempts = %d, want 1", len(fakes.attempts))
	}
}

func TestProxyDecodesZstdRequestBeforeForwarding(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":"hello","stream":false}`)
	compressed := zstdEncodeForTest(t, body)
	var upstreamBody []byte
	var upstreamContentEncoding string
	var upstreamAcceptEncoding string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamContentEncoding = r.Header.Get("Content-Encoding")
		upstreamAcceptEncoding = r.Header.Get("Accept-Encoding")
		var err error
		upstreamBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{tokens: []store.Token{{
		ID:          1,
		AccessToken: "upstream-token",
		IsActive:    true,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}}}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(compressed))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	req.Header.Set("Accept-Encoding", "zstd")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses"})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if upstreamContentEncoding != "" {
		t.Fatalf("upstream Content-Encoding = %q", upstreamContentEncoding)
	}
	if upstreamAcceptEncoding != "" {
		t.Fatalf("upstream Accept-Encoding = %q", upstreamAcceptEncoding)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstreamBody, &payload); err != nil {
		t.Fatalf("upstream body was not decoded JSON: %v body=%q", err, upstreamBody)
	}
	input, ok := payload["input"].([]any)
	if !ok || len(input) != 1 {
		t.Fatalf("unexpected upstream payload: %v", payload)
	}
	message, ok := input[0].(map[string]any)
	if !ok || message["role"] != "user" || message["content"] != "hello" {
		t.Fatalf("unexpected upstream payload: %v", payload)
	}
	if payload["model"] != "gpt-5.5" {
		t.Fatalf("unexpected upstream payload: %v", payload)
	}
}

func TestProxyRoutesFastRequestOnlyToFastCapablePlan(t *testing.T) {
	var upstreamAuthorization string
	var upstreamServiceTier string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamAuthorization = r.Header.Get("Authorization")
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		upstreamServiceTier, _ = payload["service_tier"].(string)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_fast","object":"response","model":"gpt-5.5","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	free := "free"
	k12 := "k12"
	pro := "pro"
	now := time.Now().UTC()
	fakes := &fakeProxyStore{tokens: []store.Token{
		{ID: 1, OwnerUserID: 42, AccessToken: "free-token", PlanType: &free, IsActive: true, CreatedAt: now, UpdatedAt: now},
		{ID: 2, OwnerUserID: 42, AccessToken: "k12-token", PlanType: &k12, IsActive: true, CreatedAt: now, UpdatedAt: now},
		{ID: 3, OwnerUserID: 42, AccessToken: "pro-token", PlanType: &pro, IsActive: true, CreatedAt: now, UpdatedAt: now},
	}}
	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool:  config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{QueueMaxSize: 10, BatchSize: 1, Concurrency: 1},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.RefreshOwner(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	validUntil := time.Now().UTC().Add(time.Hour)
	manager.SetPlanModelCapabilities(42, free, "0.144.0", nil, validUntil)
	manager.SetPlanModelCapabilities(42, k12, "0.144.0", nil, validUntil)
	manager.SetPlanModelCapabilities(42, pro, "0.144.0", []string{"gpt-5.5"}, validUntil)
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	for _, body := range []string{
		`{"model":"gpt-5.5","input":"hello","service_tier":"priority"}`,
		`{"model":"gpt-5.5","input":"hello","service_tier":{},"service_tier":"priority"}`,
	} {
		upstreamAuthorization = ""
		upstreamServiceTier = ""
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", OwnerUserID: 42})

		if recorder.Code != http.StatusOK {
			t.Fatalf("body %s: status = %d response=%s", body, recorder.Code, recorder.Body.String())
		}
		if upstreamAuthorization != "Bearer pro-token" {
			t.Fatalf("body %s: upstream Authorization = %q, want Pro token", body, upstreamAuthorization)
		}
		if upstreamServiceTier != "priority" {
			t.Fatalf("body %s: upstream service_tier = %q", body, upstreamServiceTier)
		}
	}
}

func TestNormalizeIntentRecognizesOnlyExactPriorityServiceTier(t *testing.T) {
	intent := normalizeIntent(RequestIntent{Endpoint: "/v1/responses"}, []byte(`{"model":"gpt-5.5","service_tier":"priority"}`))
	if !intent.RequireFast || intent.ServiceTier != "priority" {
		t.Fatalf("service_tier priority did not require Fast: %#v", intent)
	}
	for _, tier := range []string{"", "default", "auto", "fast", "PRIORITY", " priority "} {
		intent := normalizeIntent(RequestIntent{Endpoint: "/v1/responses"}, []byte(fmt.Sprintf(`{"model":"gpt-5.5","service_tier":%q}`, tier)))
		if intent.RequireFast {
			t.Fatalf("service_tier %q unexpectedly required Fast", tier)
		}
	}
	for _, endpoint := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions"} {
		duplicate := normalizeIntent(
			RequestIntent{Endpoint: endpoint},
			[]byte(`{"model":"gpt-5.5","service_tier":{},"service_tier":"priority"}`),
		)
		if !duplicate.RequireFast || duplicate.ServiceTier != "priority" {
			t.Fatalf("endpoint %s duplicate service_tier intent = %#v, want priority Fast", endpoint, duplicate)
		}
	}
	for _, test := range []struct {
		body     string
		wantTier string
	}{
		{body: `{"service_tier":"priority","service_tier":"default"}`, wantTier: "default"},
		{body: `{"service_tier":"priority","service_tier":{}}`, wantTier: ""},
	} {
		intent := normalizeIntent(RequestIntent{Endpoint: "/v1/responses"}, []byte(test.body))
		if intent.RequireFast || intent.ServiceTier != test.wantTier {
			t.Fatalf("body %s intent = %#v, want non-Fast tier %q", test.body, intent, test.wantTier)
		}
	}
}

func TestPrepareUpstreamPayloadPreservesPriorityServiceTier(t *testing.T) {
	for _, test := range []struct {
		endpoint string
		tier     string
	}{
		{endpoint: "/v1/responses", tier: "priority"},
		{endpoint: "/v1/chat/completions", tier: "priority"},
	} {
		body := []byte(fmt.Sprintf(`{"model":"gpt-5.5","input":"hello","service_tier":%q}`, test.tier))
		intent := normalizeIntent(RequestIntent{Endpoint: test.endpoint}, body)
		prepared, preparedIntent, status, err := prepareUpstreamPayload(
			httptest.NewRequest(http.MethodPost, test.endpoint, nil),
			body,
			intent,
		)
		if err != nil || status != http.StatusOK {
			t.Fatalf("tier %q: status=%d err=%v", test.tier, status, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(prepared, &payload); err != nil {
			t.Fatal(err)
		}
		if got := payload["service_tier"]; got != "priority" {
			t.Fatalf("tier %q forwarded as %v", test.tier, got)
		}
		if preparedIntent.ServiceTier != "priority" || !preparedIntent.RequireFast {
			t.Fatalf("tier %q intent = %#v", test.tier, preparedIntent)
		}
	}
}

func TestPrepareUpstreamPayloadRejectsFastAlias(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions"} {
		body := []byte(`{"model":"gpt-5.5","service_tier":"fast"}`)
		intent := normalizeIntent(RequestIntent{Endpoint: endpoint}, body)
		prepared, preparedIntent, status, err := prepareUpstreamPayload(
			httptest.NewRequest(http.MethodPost, endpoint, nil),
			body,
			intent,
		)
		if err == nil || status != http.StatusBadRequest {
			t.Fatalf("endpoint %s: status=%d err=%v", endpoint, status, err)
		}
		if preparedIntent.RequireFast || preparedIntent.ServiceTier != "fast" {
			t.Fatalf("endpoint %s alias was upgraded: %#v", endpoint, preparedIntent)
		}
		if string(prepared) != string(body) {
			t.Fatalf("endpoint %s alias payload was rewritten: %s", endpoint, prepared)
		}
	}
}

func TestProxyRetriesAfterMalformedCompactionEncryptedContent400(t *testing.T) {
	var upstreamBodies [][]byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		upstreamBodies = append(upstreamBodies, body)
		if len(upstreamBodies) == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: error`,
				`data: {"error":{"code":"oaix_gateway_error","message":"The encrypted content plain handoff text could not be verified. Reason: Encrypted content could not be decrypted or parsed.","status":400,"type":"gateway_error"}}`,
				``,
			}, "\n"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_retry","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{tokens: []store.Token{{
		ID:          1,
		AccessToken: "upstream-token",
		IsActive:    true,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}}}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model": "gpt-5.5",
		"stream": true,
		"input": [
			{"type": "reasoning", "encrypted_content": "plain reasoning is not touched"},
			{"type": "compaction", "encrypted_content": "plain handoff text"},
			{"type": "compaction", "encrypted_content": "gAAAAABvalid-encrypted-content"},
			{"role": "user", "content": "say test"}
		]
	}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses"})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if len(upstreamBodies) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(upstreamBodies))
	}
	var retriedPayload map[string]any
	if err := json.Unmarshal(upstreamBodies[1], &retriedPayload); err != nil {
		t.Fatalf("retried body was not JSON: %v body=%q", err, upstreamBodies[1])
	}
	input := retriedPayload["input"].([]any)
	if len(input) != 3 {
		t.Fatalf("retried input length = %d, want 3: %v", len(input), input)
	}
	hasReasoning := false
	hasEncryptedCompaction := false
	for _, item := range input {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if object["type"] == "compaction" && object["encrypted_content"] == "plain handoff text" {
			t.Fatalf("malformed compaction was forwarded on retry: %v", input)
		}
		if object["type"] == "reasoning" && object["encrypted_content"] == "plain reasoning is not touched" {
			hasReasoning = true
		}
		if object["type"] == "compaction" && object["encrypted_content"] == "gAAAAABvalid-encrypted-content" {
			hasEncryptedCompaction = true
		}
	}
	if !hasReasoning || !hasEncryptedCompaction {
		t.Fatalf("retry removed unrelated input items: %v", input)
	}
	if !strings.Contains(recorder.Body.String(), "response.completed") {
		t.Fatalf("expected successful stream body, got %q", recorder.Body.String())
	}
}

func TestProxyRetriesRejectedReasoningEncryptedContentUntilSuccess(t *testing.T) {
	var upstreamBodies [][]byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		upstreamBodies = append(upstreamBodies, body)
		w.Header().Set("Content-Type", "text/event-stream")
		switch len(upstreamBodies) {
		case 1:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: error`,
				`data: {"error":{"code":"oaix_gateway_error","message":"The encrypted content gAAA...6g== could not be verified. Reason: Encrypted content could not be decrypted or parsed.","status":400,"type":"gateway_error"}}`,
				``,
			}, "\n"))
		case 2:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: error`,
				`data: {"error":{"code":"oaix_gateway_error","message":"The encrypted content gAAA...BHs= could not be verified. Reason: Encrypted content could not be decrypted or parsed.","status":400,"type":"gateway_error"}}`,
				``,
			}, "\n"))
		default:
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: response.completed`,
				`data: {"type":"response.completed","response":{"id":"resp_retry","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
				``,
			}, "\n"))
		}
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{tokens: []store.Token{{
		ID:          1,
		AccessToken: "upstream-token",
		IsActive:    true,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}}}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model": "gpt-5.5",
		"stream": true,
		"input": [
			{"type": "reasoning", "encrypted_content": "gAAAAABfirst-token-6g==", "summary": []},
			{"role": "user", "content": "say test"},
			{"type": "reasoning", "encrypted_content": "gAAAAABsecond-token-BHs=", "summary": []},
			{"type": "reasoning", "encrypted_content": "gAAAAABvalid-reasoning-token", "summary": []}
		]
	}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses"})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if len(upstreamBodies) != 3 {
		t.Fatalf("upstream calls = %d, want 3", len(upstreamBodies))
	}
	var retriedPayload map[string]any
	if err := json.Unmarshal(upstreamBodies[2], &retriedPayload); err != nil {
		t.Fatalf("retried body was not JSON: %v body=%q", err, upstreamBodies[2])
	}
	input := retriedPayload["input"].([]any)
	if len(input) != 2 {
		t.Fatalf("retried input length = %d, want 2: %v", len(input), input)
	}
	hasUser := false
	hasUnrejectedReasoning := false
	for _, item := range input {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if object["encrypted_content"] == "gAAAAABfirst-token-6g==" || object["encrypted_content"] == "gAAAAABsecond-token-BHs=" {
			t.Fatalf("rejected reasoning was forwarded on retry: %v", input)
		}
		if object["role"] == "user" && object["content"] == "say test" {
			hasUser = true
		}
		if object["type"] == "reasoning" && object["encrypted_content"] == "gAAAAABvalid-reasoning-token" {
			hasUnrejectedReasoning = true
		}
	}
	if !hasUser || !hasUnrejectedReasoning {
		t.Fatalf("retry removed unrelated input items: %v", input)
	}
	if !strings.Contains(recorder.Body.String(), "response.completed") {
		t.Fatalf("expected successful stream body, got %q", recorder.Body.String())
	}
}

func TestProxyUsageLimitUsesOfficialCooldown(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"type":"usage_limit_reached","resets_in_seconds":3600,"message":"usage limit reached"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:          1,
			AccessToken: "upstream-token",
			IsActive:    true,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	start := time.Now().UTC()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case until := <-fakes.tokenErrorCh:
		if until == nil {
			t.Fatal("expected cooldown")
		}
		if got := until.Sub(start); got < 3590*time.Second {
			t.Fatalf("cooldown = %s, want official reset near 3600s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for token error commit")
	}
}

func TestProxyDeactivatesAndRetriesAfterDeactivatedWorkspace402(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		attempt := len(authHeaders)
		mu.Unlock()
		if attempt == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusPaymentRequired)
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: error`,
				`data: {"error":{"code":"oaix_gateway_error","message":"map[code:deactivated_workspace]","status":402,"type":"gateway_error"}}`,
				``,
			}, "\n"))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_retry","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, OwnerUserID: 10, AccessToken: "good-token", IsActive: true, CreatedAt: now, UpdatedAt: now},
			{ID: 2, OwnerUserID: 20, AccessToken: "bad-token", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorEvents: make(chan tokenErrorEvent, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	affinityStore := affinity.NewMemoryStore()
	affinityStore.Put("prompt", affinity.Lane{PrimaryTokenID: 2}, time.Hour)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinityStore)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	mu.Lock()
	gotAuthHeaders := append([]string(nil), authHeaders...)
	mu.Unlock()
	if len(gotAuthHeaders) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(gotAuthHeaders))
	}
	if gotAuthHeaders[0] != "Bearer bad-token" || gotAuthHeaders[1] != "Bearer good-token" {
		t.Fatalf("auth headers = %v", gotAuthHeaders)
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		if event.TokenID != 2 {
			t.Fatalf("token_id = %d, want 2", event.TokenID)
		}
		if !event.Deactivate {
			t.Fatal("expected token to be deactivated")
		}
		if event.CooldownUntil != nil {
			t.Fatalf("cooldown = %v, want nil", event.CooldownUntil)
		}
		if !strings.Contains(event.Message, "deactivated_workspace") {
			t.Fatalf("message = %q", event.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for token deactivate commit")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if manager.Snapshot().ByID[2] == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bad token remained in refreshed snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := affinityStore.Get("prompt"); ok {
		t.Fatal("prompt affinity still contains deactivated token")
	}
}

func TestProxyRefreshesOAuthAccessTokenBeforeDeactivating401(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		auth := r.Header.Get("Authorization")
		mu.Unlock()
		if auth == "Bearer old-token" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":"no_matching_rule","message":"Unauthorized"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_retry","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:           1,
			OwnerUserID:  10,
			AccessToken:  "old-token",
			RefreshToken: "refresh-token",
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
		}},
		tokenErrorEvents: make(chan tokenErrorEvent, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())
	pipeline.SetOAuthClient(fakeOAuthClient{result: oauth.RefreshResult{AccessToken: "new-token", RefreshToken: "refresh-token-2", ExpiresIn: 3600}})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	mu.Lock()
	gotAuthHeaders := append([]string(nil), authHeaders...)
	mu.Unlock()
	if len(gotAuthHeaders) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(gotAuthHeaders))
	}
	if gotAuthHeaders[0] != "Bearer old-token" || gotAuthHeaders[1] != "Bearer new-token" {
		t.Fatalf("auth headers = %v", gotAuthHeaders)
	}
	fakes.mu.Lock()
	token := fakes.tokens[0]
	fakes.mu.Unlock()
	if token.AccessToken != "new-token" || token.RefreshToken != "refresh-token-2" {
		t.Fatalf("token secret was not updated: access=%q refresh=%q", token.AccessToken, token.RefreshToken)
	}
	if !token.IsActive || token.DisabledAt != nil {
		t.Fatalf("token was disabled after refresh: %+v", token)
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		t.Fatalf("unexpected token error event after refresh: %+v", event)
	default:
	}
}

func TestProxyDoesNotDeactivateOAuthTokenWhen401PersistsAfterRefresh(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"code":"no_matching_rule","message":"Unauthorized"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:           1,
			OwnerUserID:  10,
			AccessToken:  "old-token",
			RefreshToken: "refresh-token",
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
		}},
		tokenErrorEvents: make(chan tokenErrorEvent, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())
	pipeline.SetOAuthClient(fakeOAuthClient{result: oauth.RefreshResult{AccessToken: "new-token", RefreshToken: "refresh-token-2", ExpiresIn: 3600}})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		if event.TokenID != 1 {
			t.Fatalf("token_id = %d, want 1", event.TokenID)
		}
		if event.Deactivate {
			t.Fatalf("token should not be deactivated after refresh-backed 401: %+v", event)
		}
		if event.CooldownUntil == nil {
			t.Fatalf("expected short cooldown for inconclusive auth failure: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for non-terminal token error commit")
	}
	fakes.mu.Lock()
	token := fakes.tokens[0]
	fakes.mu.Unlock()
	if !token.IsActive || token.DisabledAt != nil {
		t.Fatalf("token was disabled after inconclusive auth failure: %+v", token)
	}
}

func TestProxyDeactivatesOAuthTokenWhenRefreshTokenIsPermanentlyInvalid(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"code":"no_matching_rule","message":"Unauthorized"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:           1,
			OwnerUserID:  10,
			AccessToken:  "old-token",
			RefreshToken: "refresh-token",
			IsActive:     true,
			CreatedAt:    now,
			UpdatedAt:    now,
		}},
		tokenErrorEvents: make(chan tokenErrorEvent, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())
	pipeline.SetOAuthClient(fakeOAuthClient{err: errors.New(`oauth refresh failed with status 400: {"error":"invalid_grant"}`)})

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		if event.TokenID != 1 || !event.Deactivate {
			t.Fatalf("token error event = %+v, want token 1 deactivated", event)
		}
		if !strings.Contains(event.Message, "refresh token invalid") {
			t.Fatalf("message = %q", event.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for token deactivate commit")
	}
}

func TestProxyTargetTokenDoesNotFallbackAcrossOwnerPool(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"invalid_api_key","message":"invalid"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, OwnerUserID: 10, AccessToken: "target-token", IsActive: true, CreatedAt: now, UpdatedAt: now},
			{ID: 2, OwnerUserID: 10, AccessToken: "other-owner-token", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorEvents: make(chan tokenErrorEvent, 2),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", OwnerUserID: 10, TargetTokenID: 1})

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	mu.Lock()
	gotAuthHeaders := append([]string(nil), authHeaders...)
	mu.Unlock()
	if len(gotAuthHeaders) != 1 {
		t.Fatalf("upstream calls = %d, want only target token attempt", len(gotAuthHeaders))
	}
	if gotAuthHeaders[0] != "Bearer target-token" {
		t.Fatalf("auth headers = %v, want target token only", gotAuthHeaders)
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		if event.TokenID != 1 || !event.Deactivate {
			t.Fatalf("token error event = %+v, want token 1 deactivated", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for target token deactivate commit")
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		t.Fatalf("unexpected second token error event: %+v", event)
	default:
	}
}

func TestProxyPaymentRequiredWithoutDeactivatedWorkspaceDoesNotDeactivateToken(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = io.WriteString(w, `{"error":{"code":"billing_required","message":"payment required"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	now := time.Now().UTC()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:          1,
			AccessToken: "upstream-token",
			IsActive:    true,
			CreatedAt:   now,
			UpdatedAt:   now,
		}},
		tokenErrorEvents: make(chan tokenErrorEvent, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})

	if recorder.Code != http.StatusPaymentRequired {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case event := <-fakes.tokenErrorEvents:
		t.Fatalf("unexpected token error commit: %+v", event)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestProxyNonRetryableUpstream4xxDoesNotCooldownToken(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"encrypted content could not be verified"}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                1,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakes := &fakeProxyStore{
		tokens: []store.Token{{
			ID:          1,
			AccessToken: "upstream-token",
			IsActive:    true,
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case until := <-fakes.tokenErrorCh:
		t.Fatalf("unexpected token cooldown commit: %v", until)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestProxyPreservesContextLengthFailureAfterKeepalive(t *testing.T) {
	var callsMu sync.Mutex
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsMu.Lock()
		upstreamCalls++
		callsMu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.created`,
			`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_ctx","object":"response","model":"gpt-5.5","status":"in_progress"}}`,
			``,
			`event: response.failed`,
			`data: {"type":"response.failed","sequence_number":1,"response":{"id":"resp_ctx","object":"response","model":"gpt-5.5","status":"failed","error":{"code":"context_length_exceeded","message":"Your input exceeds the context window of this model."}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now().UTC()
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
			{ID: 2, AccessToken: "upstream-token-2", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	if recorder.Code != http.StatusOK {
		t.Fatalf("wire status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	callsMu.Lock()
	gotCalls := upstreamCalls
	callsMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("upstream calls = %d, want one non-retryable attempt", gotCalls)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: keepalive") {
		t.Fatalf("expected precommit keepalive: %q", body)
	}
	if strings.Count(body, "event: response.failed") != 1 || !strings.Contains(body, `"code":"context_length_exceeded"`) {
		t.Fatalf("context failure terminal was not preserved exactly once: %q", body)
	}
	if strings.Contains(body, "event: error") || strings.Contains(body, "oaix_gateway_error") || strings.Contains(body, "[DONE]") {
		t.Fatalf("context failure was downgraded or followed by a fake terminator: %q", body)
	}
	select {
	case until := <-fakes.tokenErrorCh:
		t.Fatalf("context failure must not cool down the token: %v", until)
	case <-time.After(200 * time.Millisecond):
	}
	fakes.mu.Lock()
	defer fakes.mu.Unlock()
	if len(fakes.attempts) != 1 {
		t.Fatalf("gateway attempts = %d, want 1", len(fakes.attempts))
	}
	attempt := fakes.attempts[0]
	if attempt.ErrorCode == nil || *attempt.ErrorCode != "context_length_exceeded" {
		t.Fatalf("attempt error code = %v", attempt.ErrorCode)
	}
	trace := attempt.StreamDeliveryTrace
	if trace == nil || trace.Upstream.FirstResponseFailed == nil || trace.Downstream.FirstResponseFailed == nil {
		t.Fatalf("response.failed delivery trace missing: %+v", trace)
	}
	if trace.State() != "terminal_flushed" || trace.End.Reason != "upstream_response_failed_terminal_delivered" || trace.End.Error != "" {
		t.Fatalf("unexpected response.failed trace: %+v state=%q", trace, trace.State())
	}
}

func TestProxyNormalizesProviderDeclaredContextErrorAfterKeepalive(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.created`,
			`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_provider_ctx","object":"response","model":"internal-model","status":"in_progress"}}`,
			``,
			`event: response.in_progress`,
			`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_provider_ctx","status":"in_progress"}}`,
			``,
			`event: error`,
			`data: {"type":"error","error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"Your input exceeds the context window of this model. Please adjust your input and try again.","param":"input"},"sequence_number":2}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	now := time.Now().UTC()
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
			{ID: 2, AccessToken: "upstream-token-2", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 3, fakes)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"public-model","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{
		Endpoint:           "/v1/responses",
		Model:              "internal-model",
		ResponseModelAlias: "public-model",
		Stream:             true,
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("wire status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want one non-retryable attempt", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "event: keepalive") || strings.Count(body, "event: response.failed") != 1 {
		t.Fatalf("normalized provider failure stream is malformed: %q", body)
	}
	if strings.Contains(body, "event: error") || strings.Contains(body, "oaix_gateway_error") || strings.Contains(body, "[DONE]") || strings.Contains(body, "internal-model") {
		t.Fatalf("provider error leaked or was followed by a fallback terminator: %q", body)
	}

	var terminal map[string]any
	parseErr := sse.NewParser(1<<20).Parse(context.Background(), strings.NewReader(body), func(event sse.Event) error {
		if event.Event != "response.failed" {
			return nil
		}
		return json.Unmarshal(event.Data, &terminal)
	})
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	response, _ := terminal["response"].(map[string]any)
	errorObject, _ := response["error"].(map[string]any)
	if terminal["type"] != "response.failed" || terminal["sequence_number"] != float64(2) {
		t.Fatalf("unexpected canonical terminal envelope: %v", terminal)
	}
	if response["id"] != "resp_provider_ctx" || response["object"] != "response" || response["model"] != "public-model" || response["status"] != "failed" {
		t.Fatalf("unexpected canonical response metadata: %v", response)
	}
	if errorObject["type"] != "invalid_request_error" || errorObject["code"] != "context_length_exceeded" || errorObject["param"] != "input" || errorObject["message"] != "Your input exceeds the context window of this model. Please adjust your input and try again." {
		t.Fatalf("provider error semantics were not preserved: %v", errorObject)
	}

	select {
	case until := <-fakes.tokenErrorCh:
		t.Fatalf("context failure must not cool down the token: %v", until)
	case <-time.After(200 * time.Millisecond):
	}
	fakes.mu.Lock()
	defer fakes.mu.Unlock()
	if len(fakes.attempts) != 1 {
		t.Fatalf("gateway attempts = %d, want 1", len(fakes.attempts))
	}
	attempt := fakes.attempts[0]
	if attempt.StatusCode == nil || *attempt.StatusCode != http.StatusBadRequest || attempt.Success == nil || *attempt.Success || attempt.Retry == nil || *attempt.Retry || attempt.Outcome != string(OutcomeUpstream4xx) || attempt.ErrorCode == nil || *attempt.ErrorCode != "context_length_exceeded" || attempt.CooldownUntil != nil {
		t.Fatalf("unexpected normalized provider failure attempt: %+v", attempt)
	}
	trace := attempt.StreamDeliveryTrace
	if trace == nil || trace.Upstream.FirstResponseFailed != nil || trace.Upstream.ResponseFailedCount != 0 || trace.Downstream.FirstResponseFailed == nil {
		t.Fatalf("normalized provider failure trace lost raw-vs-downstream provenance: %+v", trace)
	}
	if trace.State() != "terminal_flushed" || trace.End.Reason != "upstream_provider_error_normalized_failure_delivered" || trace.End.Error != "" {
		t.Fatalf("unexpected normalized provider failure trace: %+v state=%q", trace, trace.State())
	}
}

func TestProxyDiscardsRetriedFailureWhenLaterAttemptSucceeds(t *testing.T) {
	var callsMu sync.Mutex
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsMu.Lock()
		upstreamCalls++
		call := upstreamCalls
		callsMu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if call == 1 {
			_, _ = io.WriteString(w, strings.Join([]string{
				`event: response.created`,
				`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_rate","status":"in_progress"}}`,
				``,
				`event: response.failed`,
				`data: {"type":"response.failed","sequence_number":1,"response":{"id":"resp_rate","status":"failed","error":{"code":"rate_limit_exceeded","message":"retry later"}}}`,
				``,
			}, "\n"))
			return
		}
		_, _ = io.WriteString(w, strings.Join([]string{
			`event: response.created`,
			`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_ok","status":"in_progress"}}`,
			``,
			`event: response.completed`,
			`data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_ok","object":"response","model":"gpt-5.5","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n"))
	}))
	defer upstream.Close()

	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstream.URL,
			MaxRetries:                2,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Now().UTC()
	fakes := &fakeProxyStore{tokens: []store.Token{
		{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
		{ID: 2, AccessToken: "upstream-token-2", IsActive: true, CreatedAt: now, UpdatedAt: now},
	}}
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	callsMu.Lock()
	gotCalls := upstreamCalls
	callsMu.Unlock()
	if gotCalls != 2 {
		t.Fatalf("upstream calls = %d, want retry followed by success", gotCalls)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if strings.Contains(body, "response.failed") || strings.Contains(body, "retry later") {
		t.Fatalf("failed attempt leaked into successful stream: %q", body)
	}
	if !strings.Contains(body, "response.completed") || !strings.Contains(body, "resp_ok") {
		t.Fatalf("successful retry terminal missing: %q", body)
	}
}

func TestProxyPreservesRetryableFailureWhenTokensAreExhausted(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(append(
			sse.Encode("response.created", []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_rate","status":"in_progress"}}`)),
			sse.Encode("response.failed", []byte(`{"type":"response.failed","sequence_number":1,"response":{"id":"resp_rate","status":"failed","error":{"code":"rate_limit_exceeded","message":"retry later"}}}`))...,
		))
	}))
	defer upstream.Close()

	now := time.Now().UTC()
	fakes := &fakeProxyStore{tokens: []store.Token{
		{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
	}}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 3, fakes)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	if upstreamCalls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1 before token exhaustion", upstreamCalls.Load())
	}
	body := recorder.Body.String()
	if strings.Count(body, "event: response.failed") != 1 || !strings.Contains(body, `"code":"rate_limit_exceeded"`) {
		t.Fatalf("last structured failure was not preserved: %q", body)
	}
	if strings.Contains(body, "event: error") || strings.Contains(body, "no available token") || strings.Contains(body, "[DONE]") {
		t.Fatalf("token exhaustion replaced the structured failure: %q", body)
	}
}

func TestProxyCommittedResponsesFailureProtectsTokenHealth(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(append(
			sse.Encode("response.output_text.delta", []byte(`{"type":"response.output_text.delta","sequence_number":1,"delta":"partial"}`)),
			sse.Encode("response.failed", []byte(`{"type":"response.failed","sequence_number":2,"response":{"id":"resp_rate_after_output","status":"failed","error":{"code":"rate_limit_exceeded","message":"retry later"}}}`))...,
		))
	}))
	defer upstream.Close()

	now := time.Now().UTC()
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 3, fakes)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	body := recorder.Body.String()
	if !strings.Contains(body, "response.output_text.delta") || strings.Count(body, "event: response.failed") != 1 {
		t.Fatalf("committed failure stream is malformed: %q", body)
	}
	select {
	case until := <-fakes.tokenErrorCh:
		if until == nil || !until.After(now) {
			t.Fatalf("committed rate-limit failure did not set a future cooldown: %v", until)
		}
	case <-time.After(time.Second):
		t.Fatal("committed rate-limit failure did not update token health")
	}
	fakes.mu.Lock()
	defer fakes.mu.Unlock()
	if len(fakes.attempts) != 1 || fakes.attempts[0].CooldownUntil == nil || fakes.attempts[0].StatusCode == nil || *fakes.attempts[0].StatusCode != http.StatusTooManyRequests {
		t.Fatalf("unexpected committed failure attempt: %+v", fakes.attempts)
	}
}

func TestProxyCommittedFailureFlushErrorKeepsSemanticFailureAndTokenHealth(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(append(
			sse.Encode("response.output_text.delta", []byte(`{"type":"response.output_text.delta","sequence_number":1,"delta":"partial"}`)),
			sse.Encode("response.failed", []byte(`{"type":"response.failed","sequence_number":2,"response":{"id":"resp_rate_flush_failure","status":"failed","error":{"code":"rate_limit_exceeded","message":"retry later"}}}`))...,
		))
	}))
	defer upstream.Close()

	now := time.Now().UTC()
	fakes := &fakeProxyStore{
		tokens: []store.Token{
			{ID: 1, AccessToken: "upstream-token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
		},
		tokenErrorCh: make(chan *time.Time, 1),
	}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 3, fakes)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	flushErr := errors.New("forced response.failed flush failure")
	writer := &failNthFlushResponseWriter{header: make(http.Header), failAt: 2, flushErr: flushErr}

	pipeline.Proxy(writer, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true})

	body := writer.body.String()
	if strings.Count(body, "event: response.failed") != 1 || strings.Contains(body, "event: error") {
		t.Fatalf("delivery failure appended or lost the semantic terminal: %q", body)
	}
	select {
	case until := <-fakes.tokenErrorCh:
		if until == nil || !until.After(now) {
			t.Fatalf("failed terminal flush did not protect token health: %v", until)
		}
	case <-time.After(time.Second):
		t.Fatal("failed terminal flush did not update token health")
	}
	fakes.mu.Lock()
	defer fakes.mu.Unlock()
	if len(fakes.attempts) != 1 {
		t.Fatalf("attempt count = %d", len(fakes.attempts))
	}
	attempt := fakes.attempts[0]
	if attempt.ErrorCode == nil || *attempt.ErrorCode != "rate_limit_exceeded" || attempt.CooldownUntil == nil {
		t.Fatalf("semantic attempt data was lost: %+v", attempt)
	}
	trace := attempt.StreamDeliveryTrace
	if trace == nil || trace.State() != "terminal_flush_failed" || trace.End.Reason != "downstream_terminal_flush_error" || trace.End.Error == "" {
		t.Fatalf("unexpected failed delivery trace: %+v", trace)
	}
}

func newProxyPipelineTestHarness(t *testing.T, upstreamURL string, maxRetries int, fakes *fakeProxyStore) *Pipeline {
	t.Helper()
	cfg := config.Config{
		Upstream: config.UpstreamConfig{
			ResponsesURL:              upstreamURL,
			MaxRetries:                maxRetries,
			NonStreamMaxResponseBytes: 1 << 20,
			DisableCompression:        true,
		},
		TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{
			QueueMaxSize: 10,
			BatchSize:    1,
			Concurrency:  1,
		},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
	return New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())
}

func TestProxyGpt56SolExcludesFreeTokensBeforeSelection(t *testing.T) {
	tests := []struct {
		name          string
		selectionMode string
		ownerUserID   int64
	}{
		{name: "default"},
		{name: "marketplace priced", selectionMode: "marketplace-priced", ownerUserID: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var upstreamAuthorization string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamAuthorization = r.Header.Get("Authorization")
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, strings.Join([]string{
					`event: response.completed`,
					`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[]}}`,
					``,
				}, "\n"))
			}))
			defer upstream.Close()

			cfg := config.Config{
				Upstream: config.UpstreamConfig{
					ResponsesURL:              upstream.URL,
					MaxRetries:                1,
					NonStreamMaxResponseBytes: 1 << 20,
					DisableCompression:        true,
				},
				TokenPool: config.TokenPoolConfig{DefaultCooldown: time.Second},
				RequestLog: config.RequestLogConfig{
					QueueMaxSize: 10,
					BatchSize:    1,
					Concurrency:  1,
				},
			}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			pro := "pro"
			free := "free"
			proPrice := 250
			freePrice := 100
			now := time.Now().UTC()
			fakes := &fakeProxyStore{tokens: []store.Token{
				{
					ID:                  1,
					OwnerUserID:         10,
					AccessToken:         "pro-token",
					PlanType:            &pro,
					IsActive:            true,
					ShareEnabled:        true,
					ShareStatus:         "active",
					MarketplacePriceBPS: &proPrice,
					CreatedAt:           now,
					UpdatedAt:           now,
				},
				{
					ID:                  2,
					OwnerUserID:         20,
					AccessToken:         "free-token",
					PlanType:            &free,
					IsActive:            true,
					ShareEnabled:        true,
					ShareStatus:         "active",
					MarketplacePriceBPS: &freePrice,
					CreatedAt:           now,
					UpdatedAt:           now,
				},
			}}
			manager := tokens.NewManager(fakes, logger, time.Minute, time.Minute, 1)
			if err := manager.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			client := transport.New(cfg.Upstream)
			defer client.CloseIdleConnections()
			writer := logs.NewWriter(fakes, logger, cfg.RequestLog)
			pipeline := New(cfg, logger, manager, client, writer, fakes, affinity.NewMemoryStore())

			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol","input":"hello"}`))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			pipeline.Proxy(recorder, req, RequestIntent{
				Endpoint:      "/v1/responses",
				SelectionMode: tt.selectionMode,
				OwnerUserID:   tt.ownerUserID,
			})

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
			}
			if upstreamAuthorization != "Bearer pro-token" {
				t.Fatalf("upstream authorization = %q, want pro token", upstreamAuthorization)
			}
		})
	}
}

func TestPromptCacheContextInjectsKeyAndStableSession(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","instructions":"You are terse.","input":[{"role":"user","content":"Explain build"}]}`)
	headers := http.Header{}
	headers.Set("Authorization", "Bearer client-a")
	ctx, upstream := buildPromptCacheContext(headers, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.4"}, body, defaultPromptCacheConfig())
	if ctx == nil {
		t.Fatal("expected prompt cache context")
	}
	if ctx.Source != "derived_responses" || ctx.PromptCacheKey == "" || ctx.PromptCacheKeyHash == "" {
		t.Fatalf("unexpected context: %+v", ctx)
	}
	if ctx.PromptCacheKey != "oaix:resp:gpt-5.4:e54e99d9fb1c5799e493bd106fec10ed" {
		t.Fatalf("prompt cache key changed from Python baseline: %q", ctx.PromptCacheKey)
	}
	if ctx.PromptCacheKeyHash != "602fb23b23e76c398d6610a4c24d4c98" {
		t.Fatalf("prompt cache key hash changed from Python baseline: %q", ctx.PromptCacheKeyHash)
	}
	if ctx.SessionID != "a132c6bf-bafc-4e05-b3dc-fade4a8cb297" {
		t.Fatalf("session id changed from Python baseline: %q", ctx.SessionID)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["prompt_cache_key"] != ctx.PromptCacheKey {
		t.Fatalf("prompt_cache_key was not injected: %v", payload)
	}
	ctx2, _ := buildPromptCacheContext(headers, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.4"}, body, defaultPromptCacheConfig())
	if ctx.SessionID == "" || ctx.SessionID != ctx2.SessionID {
		t.Fatalf("session id is not stable: %q vs %q", ctx.SessionID, ctx2.SessionID)
	}
}

func TestPromptCacheContextOwnerScopesAffinityOnly(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","previous_response_id":"resp_shared","input":"hello"}`)
	ctxA, upstreamA := buildPromptCacheContext(http.Header{}, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.4", OwnerUserID: 10}, body, defaultPromptCacheConfig())
	ctxB, upstreamB := buildPromptCacheContext(http.Header{}, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.4", OwnerUserID: 11}, body, defaultPromptCacheConfig())
	if ctxA == nil || ctxB == nil {
		t.Fatal("expected prompt cache contexts")
	}
	if ctxA.AffinityKey == "" || ctxA.AffinityKey == ctxB.AffinityKey {
		t.Fatalf("affinity key must be owner scoped: %q vs %q", ctxA.AffinityKey, ctxB.AffinityKey)
	}
	if ctxA.PreviousResponseID == ctxB.PreviousResponseID || !strings.Contains(ctxA.PreviousResponseID, "resp_shared") {
		t.Fatalf("previous response owner key must be owner scoped: %q vs %q", ctxA.PreviousResponseID, ctxB.PreviousResponseID)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstreamA, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["previous_response_id"] != "resp_shared" {
		t.Fatalf("upstream previous_response_id should stay raw, got %v", payload["previous_response_id"])
	}
	if !bytes.Equal(upstreamA, upstreamB) {
		t.Fatal("owner namespace should not change upstream payload")
	}
}

func TestPromptCacheContextStripsRetentionFromUpstream(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","input":"hello","prompt_cache_retention":"ephemeral"}`)
	ctx, upstream := buildPromptCacheContext(http.Header{}, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.4"}, body, defaultPromptCacheConfig())
	if ctx == nil {
		t.Fatal("expected prompt cache context")
	}
	if ctx.PromptCacheRetentionRequested != "ephemeral" {
		t.Fatalf("retention requested = %q", ctx.PromptCacheRetentionRequested)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["prompt_cache_retention"]; ok {
		t.Fatalf("prompt_cache_retention should not be sent upstream: %v", payload)
	}
}

func TestPrepareResponsesPayloadAppliesCodexDefaults(t *testing.T) {
	body := []byte(`{
		"model": "gpt-5.5",
		"input": [{"type":"reasoning","id":"rs_1","summary":[],"reasoning_content":"hidden"}],
		"stream": true,
		"max_output_tokens": 10,
		"response_format": {"type":"json_object"},
		"safety_identifier": "client",
		"prompt_cache_retention": "ephemeral"
	}`)
	intent := RequestIntent{Endpoint: "/v1/responses", Stream: true}
	upstream, err := prepareResponsesPayload(body, &intent)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["instructions"] != "" {
		t.Fatalf("instructions default = %v", payload["instructions"])
	}
	if payload["store"] != false {
		t.Fatalf("store must be false, got %v", payload["store"])
	}
	for _, key := range []string{"max_output_tokens", "response_format", "safety_identifier", "prompt_cache_retention"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("%s should be stripped: %v", key, payload)
		}
	}
	if containsReasoningContent(payload) {
		t.Fatalf("reasoning_content should be stripped: %v", payload)
	}
	input := payload["input"].([]any)
	if _, ok := input[0].(map[string]any)["id"]; ok {
		t.Fatalf("store=false reasoning input id should be stripped: %v", input[0])
	}
}

func TestPrepareResponsesPayloadNormalizesStringInput(t *testing.T) {
	body := []byte(`{
		"model": "gpt-5.5",
		"input": "Calculate and respond with ONLY the number, nothing else.\n\nQ: 3 + 5 = ?\nA:",
		"instructions": "You are a channel health-check endpoint. Answer the arithmetic challenge exactly and briefly.",
		"stream": false
	}`)
	intent := RequestIntent{Endpoint: "/v1/responses"}
	upstream, err := prepareResponsesPayload(body, &intent)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	input, ok := payload["input"].([]any)
	if !ok {
		t.Fatalf("input should be normalized to a list: %T", payload["input"])
	}
	if len(input) != 1 {
		t.Fatalf("input length = %d, want 1", len(input))
	}
	message, ok := input[0].(map[string]any)
	if !ok {
		t.Fatalf("input item should be an object: %T", input[0])
	}
	if message["role"] != "user" {
		t.Fatalf("input role = %v", message["role"])
	}
	content, _ := message["content"].(string)
	if !strings.Contains(content, "Q: 3 + 5") {
		t.Fatalf("input content not preserved: %q", content)
	}
	if payload["instructions"] == "" {
		t.Fatalf("instructions should be preserved: %v", payload)
	}
	if payload["stream"] != true {
		t.Fatalf("non-compact responses should force upstream streaming, got %v", payload["stream"])
	}
	if payload["store"] != false {
		t.Fatalf("store must be false, got %v", payload["store"])
	}
	if intent.UpstreamAccept != "text/event-stream" {
		t.Fatalf("upstream accept = %q", intent.UpstreamAccept)
	}
}

func TestPrepareResponsesPayloadTranslatesGPTImage2(t *testing.T) {
	body := []byte(`{
		"model": "gpt-image-2",
		"input": [{"role":"user","content":[{"type":"input_text","text":"draw"},{"type":"input_image","image_url":"https://example.test/a.png"}]}],
		"size": "1024x1024",
		"output_compression": 80,
		"tool_choice": "required"
	}`)
	intent := RequestIntent{Endpoint: "/v1/responses", Stream: true}
	upstream, err := prepareResponsesPayload(body, &intent)
	if err != nil {
		t.Fatal(err)
	}
	if intent.ResponseModelAlias != "gpt-image-2" {
		t.Fatalf("response model alias = %q", intent.ResponseModelAlias)
	}
	if intent.UpstreamAccept != "text/event-stream" {
		t.Fatalf("upstream accept = %q", intent.UpstreamAccept)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "gpt-5.5" {
		t.Fatalf("upstream model = %v", payload["model"])
	}
	if payload["store"] != false || payload["instructions"] != "" || payload["parallel_tool_calls"] != true {
		t.Fatalf("missing image defaults: %v", payload)
	}
	if _, ok := payload["tool_choice"]; ok {
		t.Fatalf("tool_choice should be removed: %v", payload)
	}
	include := payload["include"].([]any)
	if len(include) != 1 || include[0] != "reasoning.encrypted_content" {
		t.Fatalf("include defaults = %v", include)
	}
	tool := payload["tools"].([]any)[0].(map[string]any)
	if tool["type"] != "image_generation" || tool["model"] != "gpt-image-2" || tool["action"] != "auto" {
		t.Fatalf("bad image tool: %v", tool)
	}
	if tool["size"] != "1024x1024" || tool["output_compression"].(float64) != 80 {
		t.Fatalf("image tool fields were not moved: %v", tool)
	}
	if _, ok := payload["size"]; ok {
		t.Fatalf("top-level image field should be moved: %v", payload)
	}
}

func TestPrepareImageGenerationPayloadTargetsResponses(t *testing.T) {
	intent := RequestIntent{Endpoint: "/v1/images/generations"}
	upstream, err := prepareImageGenerationPayload([]byte(`{
		"model": "gpt-image-2",
		"prompt": "draw a test",
		"response_format": "url",
		"stream": false,
		"size": "1024x1024"
	}`), &intent)
	if err != nil {
		t.Fatal(err)
	}
	if intent.UpstreamEndpoint != "/v1/responses" || intent.UpstreamContentType != "application/json" || intent.UpstreamAccept != "text/event-stream" {
		t.Fatalf("bad upstream routing: %+v", intent)
	}
	if intent.Model != "gpt-image-2" || intent.ImageResponseFormat != "url" || intent.ImageStreamPrefix != "image_generation" {
		t.Fatalf("bad image intent: %+v", intent)
	}
	var payload map[string]any
	if err := json.Unmarshal(upstream, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "gpt-5.5" || payload["stream"] != true || payload["store"] != false {
		t.Fatalf("bad image upstream payload: %v", payload)
	}
	tool := payload["tools"].([]any)[0].(map[string]any)
	if tool["action"] != "generate" || tool["model"] != "gpt-image-2" || tool["size"] != "1024x1024" {
		t.Fatalf("bad image generation tool: %v", tool)
	}
	content := payload["input"].([]any)[0].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["text"] != "draw a test" {
		t.Fatalf("prompt was not converted to input_text: %v", content)
	}
}

func TestStreamResponsesPreflightFailureDoesNotCommit(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.5"}}`,
		``,
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"status":"failed","model":"upstream-model","error":{"code":"rate_limit_exceeded","type":"rate_limit_error","param":"input","message":"Concurrency limit exceeded for account, please retry later"}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: true, ResponseModelAlias: "public-model"}})
	if err == nil {
		t.Fatal("expected retryable preflight error")
	}
	if result.Committed || !result.Retry || result.Status != http.StatusTooManyRequests {
		t.Fatalf("unexpected result: %+v err=%v", result, err)
	}
	if !result.StreamState.DownstreamStarted || !result.StreamState.KeepaliveSent {
		t.Fatalf("response.created should start downstream with keepalive: %+v", result.StreamState)
	}
	if result.ResponsesFailure == nil || !bytes.Contains(result.ResponsesFailure.data, []byte(`"type":"response.failed"`)) {
		t.Fatalf("structured response.failed terminal was not retained: %+v", result.ResponsesFailure)
	}
	if len(result.ErrorBody) == 0 || &result.ErrorBody[0] != &result.ResponsesFailure.data[0] {
		t.Fatal("response.failed metadata and terminal must share one owned payload buffer")
	}
	for _, expected := range []string{`"model":"public-model"`, `"code":"rate_limit_exceeded"`, `"type":"rate_limit_error"`, `"param":"input"`} {
		if !bytes.Contains(result.ResponsesFailure.data, []byte(expected)) {
			t.Fatalf("preserved terminal missing %s: %q", expected, result.ResponsesFailure.data)
		}
	}
	if result.StreamDeliveryTrace == nil || result.StreamDeliveryTrace.Upstream.FirstResponseFailed == nil || result.StreamDeliveryTrace.End.Reason != "upstream_response_failed_terminal_buffered" || result.StreamDeliveryTrace.End.Error != "" {
		t.Fatalf("unexpected buffered failure trace: %+v", result.StreamDeliveryTrace)
	}
	output := recorder.Body.String()
	if !strings.Contains(output, "event: keepalive") || !strings.Contains(output, `"type":"keepalive"`) {
		t.Fatalf("keepalive should be written immediately: %q", output)
	}
	if strings.Contains(output, "response.created") || strings.Contains(output, "response.failed") {
		t.Fatalf("preflight failure should not flush buffered upstream events: %q", output)
	}
}

func TestStreamResponsesBuffersNormalizedProviderErrorWithRawProvenance(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_provider_error","object":"response","model":"internal-model","status":"in_progress"}}`,
		``,
		`event: error`,
		`data: {"type":"error","sequence_number":2,"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"input is too long","param":"input"}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{Intent: RequestIntent{
		Endpoint:           "/v1/responses",
		Model:              "internal-model",
		ResponseModelAlias: "public-model",
		Stream:             true,
	}})
	if err == nil || result.Committed || result.Retry || result.Status != http.StatusBadRequest {
		t.Fatalf("unexpected normalized provider error result: %+v err=%v", result, err)
	}
	if !result.StreamState.DownstreamStarted || !result.StreamState.KeepaliveSent {
		t.Fatalf("response.created should start only the keepalive: %+v", result.StreamState)
	}
	if result.ResponseID != "resp_provider_error" || result.ResponsesFailure == nil || result.ResponsesFailure.sourceEventType != "error" {
		t.Fatalf("normalized provider failure metadata was not retained: %+v", result)
	}
	if len(result.ErrorBody) == 0 || &result.ErrorBody[0] != &result.ResponsesFailure.data[0] {
		t.Fatal("normalized provider failure must retain one owned canonical payload")
	}
	for _, expected := range []string{`"id":"resp_provider_error"`, `"model":"public-model"`, `"sequence_number":2`, `"code":"context_length_exceeded"`} {
		if !bytes.Contains(result.ResponsesFailure.data, []byte(expected)) {
			t.Fatalf("normalized provider terminal missing %s: %q", expected, result.ResponsesFailure.data)
		}
	}
	trace := result.StreamDeliveryTrace
	if trace == nil || trace.Upstream.ParserEventCount != 2 || trace.Upstream.LastEventOrdinal != 2 || trace.Upstream.LastPayloadSequenceNumber == nil || *trace.Upstream.LastPayloadSequenceNumber != 2 {
		t.Fatalf("provider error parser provenance missing: %+v", trace)
	}
	if trace.Upstream.ResponseFailedCount != 0 || trace.Upstream.FirstResponseFailed != nil || trace.Downstream.FirstResponseFailed != nil || trace.End.Reason != "upstream_provider_error_normalized_failure_buffered" || trace.End.Error != "" {
		t.Fatalf("raw provider error was misrepresented as an upstream response.failed: %+v", trace)
	}
	output := recorder.Body.String()
	if !strings.Contains(output, "event: keepalive") || strings.Contains(output, "response.created") || strings.Contains(output, "response.failed") || strings.Contains(output, "event: error") {
		t.Fatalf("preflight provider error should leave only the keepalive on wire: %q", output)
	}
}

func TestResponseFailureStatusContextLengthExceededWithoutTypeIsBadRequest(t *testing.T) {
	status, message, failed := responseFailureStatus(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"status": "failed",
			"error": map[string]any{
				"code":    "context_length_exceeded",
				"message": "input is too long",
			},
		},
	})
	if !failed || status != http.StatusBadRequest || message != "input is too long" {
		t.Fatalf("status=%d message=%q failed=%v", status, message, failed)
	}
}

func TestStreamResponsesNormalizesMissingJSONFailureType(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := sse.Encode("response.failed", []byte(`{"sequence_number":5,"response":{"id":"resp_missing_type","status":"failed","error":{"code":"context_length_exceeded","message":"input is too long"}}}`))
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	result, err := pipeline.streamResponsesWithPreflight(httptest.NewRecorder(), resp, Attempt{
		Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true},
	})
	if err == nil || result.Status != http.StatusBadRequest || result.Retry || result.ResponsesFailure == nil {
		t.Fatalf("unexpected normalized failure result: %+v err=%v", result, err)
	}
	if !bytes.Contains(result.ResponsesFailure.data, []byte(`"type":"response.failed"`)) {
		t.Fatalf("missing normalized discriminator: %q", result.ResponsesFailure.data)
	}
}

func TestStreamResponsesNormalizesFailureAfterSemanticOutput(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := append(
		sse.Encode("response.output_text.delta", []byte(`{"type":"response.output_text.delta","sequence_number":1,"delta":"partial"}`)),
		sse.Encode("response.failed", []byte(`{"sequence_number":2,"response":{"id":"resp_committed_failure","status":"failed","error":{"code":"context_length_exceeded","message":"input is too long"}}}`))...,
	)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{
		Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true},
	})
	if err == nil || result.Status != http.StatusBadRequest || !result.Committed || result.Retry {
		t.Fatalf("unexpected committed failure result: %+v err=%v", result, err)
	}
	bodyText := recorder.Body.String()
	if !strings.Contains(bodyText, "response.output_text.delta") || strings.Count(bodyText, "event: response.failed") != 1 || !strings.Contains(bodyText, `"type":"response.failed"`) {
		t.Fatalf("committed failure was not normalized exactly once: %q", bodyText)
	}
	trace := result.StreamDeliveryTrace
	if trace == nil || trace.Downstream.FirstResponseFailed == nil || trace.State() != "terminal_flushed" || trace.End.Reason != "upstream_response_failed_terminal_delivered" {
		t.Fatalf("unexpected committed failure trace: %+v", trace)
	}
}

func TestStreamResponsesNormalizesProviderErrorAfterSemanticOutput(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := append(
		sse.Encode("response.output_text.delta", []byte(`{"type":"response.output_text.delta","sequence_number":1,"delta":"partial"}`)),
		sse.Encode("error", []byte(`{"type":"error","sequence_number":2,"error":{"type":"invalid_request_error","code":"context_length_exceeded","message":"input is too long","param":"input"}}`))...,
	)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{
		Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true},
	})
	if err == nil || result.Status != http.StatusBadRequest || !result.Committed || result.Retry || result.ResponsesFailure == nil {
		t.Fatalf("unexpected committed provider failure result: %+v err=%v", result, err)
	}
	bodyText := recorder.Body.String()
	if !strings.Contains(bodyText, "response.output_text.delta") || strings.Count(bodyText, "event: response.failed") != 1 || !strings.Contains(bodyText, `"code":"context_length_exceeded"`) {
		t.Fatalf("committed provider error was not normalized exactly once: %q", bodyText)
	}
	if strings.Contains(bodyText, "event: error") || strings.Contains(bodyText, "[DONE]") {
		t.Fatalf("raw provider error or fake terminator leaked downstream: %q", bodyText)
	}
	trace := result.StreamDeliveryTrace
	if trace == nil || trace.Upstream.FirstResponseFailed != nil || trace.Downstream.FirstResponseFailed == nil || trace.State() != "terminal_flushed" || trace.End.Reason != "upstream_provider_error_normalized_failure_delivered" {
		t.Fatalf("unexpected committed provider failure trace: %+v", trace)
	}
}

func TestCanonicalResponsesFailureTerminalNormalizesCompatiblePayloads(t *testing.T) {
	validResponse := map[string]any{
		"status": "failed",
		"error": map[string]any{
			"code":    "context_length_exceeded",
			"message": "input is too long",
		},
	}
	tests := []struct {
		name    string
		payload map[string]any
		want    bool
	}{
		{name: "valid", payload: map[string]any{"type": "response.failed", "response": validResponse}, want: true},
		{name: "missing type is compatible", payload: map[string]any{"response": validResponse}, want: true},
		{name: "top level error", payload: map[string]any{"type": "error", "error": validResponse["error"]}},
		{name: "contradictory type", payload: map[string]any{"type": "error", "response": validResponse}},
		{name: "non-string type", payload: map[string]any{"type": 1, "response": validResponse}},
		{name: "completed response", payload: map[string]any{"type": "response.failed", "response": map[string]any{"status": "completed", "error": validResponse["error"]}}},
		{name: "non-string status", payload: map[string]any{"type": "response.failed", "response": map[string]any{"status": true, "error": validResponse["error"]}}},
		{name: "missing error code is compatible", payload: map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{"message": "input is too long"}}}, want: true},
		{name: "empty error is compatible", payload: map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]any{}}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canonicalResponsesFailureTerminal(tt.payload, "response.failed", "", 1, nil) != nil
			if got != tt.want {
				t.Fatalf("canonical terminal = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanonicalProviderDeclaredResponsesErrorTerminalRequiresExplicitSemanticError(t *testing.T) {
	sequenceNumber := int64(2)
	validPayload := map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "invalid_request_error",
			"code":    "context_length_exceeded",
			"message": "input is too long",
			"param":   "input",
		},
	}
	metadata := map[string]any{
		"id":     "resp_provider_error",
		"object": "response",
		"model":  "internal-model",
	}
	if got := eventType(sse.Event{}, validPayload); got != "error" {
		t.Fatalf("data-only provider error effective type = %q", got)
	}
	failure := canonicalProviderDeclaredResponsesErrorTerminal(validPayload, "error", "public-model", 3, &sequenceNumber, metadata)
	if failure == nil || failure.sourceEventType != "error" || failure.status != http.StatusBadRequest || failure.responseID != "resp_provider_error" {
		t.Fatalf("unexpected canonical provider failure: %+v", failure)
	}
	for _, expected := range []string{
		`"type":"response.failed"`,
		`"sequence_number":2`,
		`"id":"resp_provider_error"`,
		`"object":"response"`,
		`"model":"public-model"`,
		`"status":"failed"`,
		`"type":"invalid_request_error"`,
		`"code":"context_length_exceeded"`,
		`"message":"input is too long"`,
		`"param":"input"`,
	} {
		if !bytes.Contains(failure.data, []byte(expected)) {
			t.Fatalf("canonical provider failure missing %s: %q", expected, failure.data)
		}
	}
	if bytes.Contains(failure.data, []byte("internal-model")) {
		t.Fatalf("upstream model leaked through public alias: %q", failure.data)
	}

	codeOnly := canonicalProviderDeclaredResponsesErrorTerminal(map[string]any{
		"type":  "error",
		"error": map[string]any{"code": "context_length_exceeded"},
	}, "error", "", 1, nil, nil)
	if codeOnly == nil || codeOnly.status != http.StatusBadRequest {
		t.Fatalf("explicit provider error code should be sufficient semantic evidence: %+v", codeOnly)
	}

	tests := []struct {
		name          string
		payload       map[string]any
		effectiveType string
	}{
		{name: "nil payload", effectiveType: "error"},
		{name: "non error effective type", payload: validPayload, effectiveType: "response.failed"},
		{name: "missing top-level type", payload: map[string]any{"error": validPayload["error"]}, effectiveType: "error"},
		{name: "conflicting top-level type", payload: map[string]any{"type": "response.failed", "error": validPayload["error"]}, effectiveType: "error"},
		{name: "non-string top-level type", payload: map[string]any{"type": 1, "error": validPayload["error"]}, effectiveType: "error"},
		{name: "scalar error", payload: map[string]any{"type": "error", "error": "input is too long"}, effectiveType: "error"},
		{name: "empty error", payload: map[string]any{"type": "error", "error": map[string]any{}}, effectiveType: "error"},
		{name: "blank semantic fields", payload: map[string]any{"type": "error", "error": map[string]any{"message": " ", "code": "", "type": "\t"}}, effectiveType: "error"},
		{name: "non-string semantic fields", payload: map[string]any{"type": "error", "error": map[string]any{"message": 1, "code": true}}, effectiveType: "error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canonicalProviderDeclaredResponsesErrorTerminal(tt.payload, tt.effectiveType, "", 1, nil, nil); got != nil {
				t.Fatalf("unexpected provider error promotion: %+v", got)
			}
		})
	}
}

func TestCanonicalProviderDeclaredResponsesErrorTerminalBoundsRetainedPayload(t *testing.T) {
	failure := canonicalProviderDeclaredResponsesErrorTerminal(map[string]any{
		"type": "error",
		"error": map[string]any{
			"code":    "context_length_exceeded",
			"message": strings.Repeat("x", 2<<20),
			"ignored": strings.Repeat("y", 2<<20),
		},
	}, "error", "public-model", 1, nil, nil)
	if failure == nil {
		t.Fatal("expected bounded canonical provider failure")
	}
	if len(failure.data) >= maxResponsesFailurePayloadBytes || len(failure.message) > maxResponsesFailureMessageBytes {
		t.Fatalf("provider failure retention is not bounded: payload=%d message=%d", len(failure.data), len(failure.message))
	}
	if bytes.Contains(failure.data, []byte(`"ignored"`)) || !bytes.Contains(failure.data, []byte(`"code":"context_length_exceeded"`)) {
		t.Fatalf("provider failure retained untrusted object graph or lost semantics: %q", failure.data)
	}
}

func TestCanonicalResponsesFailureTerminalBoundsRetainedPayload(t *testing.T) {
	message := strings.Repeat("x", 2<<20)
	failure := canonicalResponsesFailureTerminal(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"status": "failed",
			"error": map[string]any{
				"code":    "context_length_exceeded",
				"message": message,
			},
		},
	}, "response.failed", "public-model", 3, nil)
	if failure == nil {
		t.Fatal("expected bounded canonical terminal")
	}
	if len(failure.data) >= maxResponsesFailurePayloadBytes || len(failure.message) > maxResponsesFailureMessageBytes {
		t.Fatalf("failure retention is not bounded: payload=%d message=%d", len(failure.data), len(failure.message))
	}
	if !bytes.Contains(failure.data, []byte(`"type":"response.failed"`)) || !bytes.Contains(failure.data, []byte(`"code":"context_length_exceeded"`)) {
		t.Fatalf("bounded terminal lost semantic discriminator: %q", failure.data)
	}
}

func TestCanonicalResponsesFailureTerminalNormalizesRecognizedCode(t *testing.T) {
	failure := canonicalResponsesFailureTerminal(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"status": "failed",
			"error": map[string]any{
				"code":      "  CONTEXT_LENGTH_EXCEEDED  ",
				"message":   "input is too long",
				"resets_at": "not-an-integer",
			},
		},
	}, "response.failed", "", 1, nil)
	if failure == nil || failure.status != http.StatusBadRequest {
		t.Fatalf("unexpected normalized failure: %+v", failure)
	}
	if !bytes.Contains(failure.data, []byte(`"code":"context_length_exceeded"`)) {
		t.Fatalf("Codex discriminator was not normalized: %q", failure.data)
	}
	if bytes.Contains(failure.data, []byte(`"resets_at"`)) {
		t.Fatalf("malformed known field must not poison Codex error decoding: %q", failure.data)
	}
}

func TestCanonicalResponsesFailureTerminalDropsInvalidCodeType(t *testing.T) {
	failure := canonicalResponsesFailureTerminal(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"status": "failed",
			"error": map[string]any{
				"code":    123,
				"message": "unknown failure",
			},
		},
	}, "response.failed", "", 1, nil)
	if failure == nil {
		t.Fatal("expected generic structured failure")
	}
	if bytes.Contains(failure.data, []byte(`"code"`)) {
		t.Fatalf("non-string code must not enter canonical payload: %q", failure.data)
	}
}

func TestStreamResponsesPreflightUsesStructuredKeepalive(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.5"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{Intent: RequestIntent{Model: "gpt-5.5", Stream: true}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed {
		t.Fatalf("expected committed stream: %+v", result)
	}
	output := recorder.Body.String()
	if strings.HasPrefix(output, ": keepalive") || strings.Contains(output, "\n: keepalive") {
		t.Fatalf("comment keepalive should not be emitted: %q", output)
	}
	if !strings.Contains(output, "event: keepalive") || !strings.Contains(output, `"type":"keepalive"`) {
		t.Fatalf("structured keepalive missing: %q", output)
	}
}

func TestStreamResponsesPreflightDoesNotDuplicateKeepaliveAfterRetry(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_2","model":"gpt-5.5"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{
		Intent: RequestIntent{Model: "gpt-5.5", Stream: true},
		StreamState: StreamAttemptState{
			DownstreamStarted: true,
			KeepaliveSent:     true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed {
		t.Fatalf("expected committed stream: %+v", result)
	}
	output := recorder.Body.String()
	if strings.Contains(output, "event: keepalive") {
		t.Fatalf("keepalive should not be duplicated after retry: %q", output)
	}
	if !strings.Contains(output, "response.created") || !strings.Contains(output, "response.output_text.delta") {
		t.Fatalf("buffered upstream events should flush on semantic output: %q", output)
	}
}

func TestStreamResponsesDeliveryTraceRecordsTerminalFrame(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	createdData := []byte(`{"type":"response.created","sequence_number":6,"response":{"id":"resp_1","model":"upstream-model"}}`)
	completedData := []byte(`{"type":"response.completed","sequence_number":7,"response":{"id":"resp_1","model":"upstream-model","status":"completed"}}`)
	body := append(sse.Encode("response.created", createdData), sse.Encode("response.completed", completedData)...)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.streamResponsesWithPreflight(recorder, resp, Attempt{
		Intent:                 RequestIntent{Endpoint: "/v1/responses", Model: "upstream-model", Stream: true, ResponseModelAlias: "public-model"},
		DownstreamConnectionID: "oaixc-test-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed {
		t.Fatalf("expected committed terminal stream: %+v", result)
	}
	trace := result.StreamDeliveryTrace
	if trace == nil {
		t.Fatal("missing stream delivery trace")
	}
	if trace.SchemaVersion != store.StreamDeliveryTraceSchemaVersion || trace.Semantics != "oaix_local_write_flush_only" {
		t.Fatalf("unexpected trace envelope: %+v", trace)
	}
	if trace.Downstream.ConnectionID != "oaixc-test-1" {
		t.Fatalf("connection id = %q", trace.Downstream.ConnectionID)
	}
	upstream := trace.Upstream.FirstResponseCompleted
	if upstream == nil {
		t.Fatal("missing upstream response.completed trace")
	}
	if trace.Upstream.ParserEventCount != 2 || trace.Upstream.ResponseCompletedCount != 1 || upstream.EventOrdinal != 2 {
		t.Fatalf("unexpected upstream counters: %+v", trace.Upstream)
	}
	if upstream.PayloadSequenceNumber == nil || *upstream.PayloadSequenceNumber != 7 {
		t.Fatalf("upstream sequence number = %v", upstream.PayloadSequenceNumber)
	}
	if upstream.DataBytes != len(completedData) || upstream.DataSHA256 != fmt.Sprintf("%x", sha256.Sum256(completedData)) {
		t.Fatalf("unexpected upstream terminal bytes/hash: %+v", upstream)
	}
	if upstream.ReceivedAt.IsZero() || trace.Upstream.EOFAt == nil {
		t.Fatalf("missing upstream timestamps: %+v", trace.Upstream)
	}
	expectedWire := sse.Encode("response.completed", []byte(`{"response":{"id":"resp_1","model":"public-model","status":"completed"},"sequence_number":7,"type":"response.completed"}`))
	downstream := trace.Downstream.FirstResponseCompleted
	if downstream == nil {
		t.Fatal("missing downstream response.completed trace")
	}
	if downstream.EventOrdinal != 2 || downstream.PayloadSequenceNumber == nil || *downstream.PayloadSequenceNumber != 7 {
		t.Fatalf("unexpected downstream terminal identity: %+v", downstream)
	}
	if downstream.WireBytes != len(expectedWire) || downstream.WireSHA256 != fmt.Sprintf("%x", sha256.Sum256(expectedWire)) {
		t.Fatalf("unexpected downstream terminal bytes/hash: %+v", downstream)
	}
	if downstream.WrittenBytes != len(expectedWire) || downstream.WriteResult != "succeeded" || downstream.FlushResult != "succeeded" {
		t.Fatalf("unexpected downstream write/flush result: %+v", downstream)
	}
	if downstream.WriteAttemptedAt.IsZero() || downstream.WriteCompletedAt.IsZero() || downstream.FlushAttemptedAt == nil || downstream.FlushCompletedAt == nil {
		t.Fatalf("missing downstream timestamps: %+v", downstream)
	}
	if trace.Downstream.FinalBodyProducedAt == nil || trace.End.At.IsZero() || trace.End.Reason != "upstream_eof_after_terminal" {
		t.Fatalf("unexpected stream end: downstream=%+v end=%+v", trace.Downstream, trace.End)
	}
	if trace.State() != "terminal_flushed" {
		t.Fatalf("state = %q", trace.State())
	}
	if !bytes.Contains(recorder.Body.Bytes(), expectedWire) {
		t.Fatalf("alias-rewritten terminal frame missing from output: %q", recorder.Body.Bytes())
	}
}

func TestStreamResponsesDeliveryTraceBubblesFlushError(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	completedData := []byte(`{"type":"response.completed","sequence_number":3,"response":{"id":"resp_flush","status":"completed"}}`)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(sse.Encode("response.completed", completedData))),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	flushErr := errors.New("forced flush failure")
	writer := &flushErrorResponseWriter{header: make(http.Header), flushErr: flushErr}
	result, err := pipeline.streamResponsesWithPreflight(writer, resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true}})
	if !errors.Is(err, flushErr) {
		t.Fatalf("error = %v, want wrapped flush error", err)
	}
	if !result.Committed || result.StreamDeliveryTrace == nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	completed := result.StreamDeliveryTrace.Downstream.FirstResponseCompleted
	if completed == nil || completed.WriteResult != "succeeded" || completed.FlushResult != "failed" || completed.FlushError != flushErr.Error() {
		t.Fatalf("unexpected terminal delivery trace: %+v", completed)
	}
	if completed.FlushAttemptedAt == nil || completed.FlushCompletedAt == nil {
		t.Fatalf("missing flush timestamps: %+v", completed)
	}
	if result.StreamDeliveryTrace.End.Reason != "downstream_terminal_flush_error" || result.StreamDeliveryTrace.State() != "terminal_flush_failed" {
		t.Fatalf("unexpected trace end/state: %+v state=%q", result.StreamDeliveryTrace.End, result.StreamDeliveryTrace.State())
	}
	if result.StreamDeliveryTrace.Downstream.FinalBodyProducedAt != nil {
		t.Fatalf("failed flush must not claim final body completion: %+v", result.StreamDeliveryTrace.Downstream)
	}
}

func TestStreamResponsesKeepaliveFlushErrorIsCommittedAndNotRetryable(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	createdData := []byte(`{"type":"response.created","sequence_number":1,"response":{"id":"resp_keepalive"}}`)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(sse.Encode("response.created", createdData))),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	flushErr := errors.New("forced keepalive flush failure")
	writer := &flushErrorResponseWriter{header: make(http.Header), flushErr: flushErr}
	result, err := pipeline.streamResponsesWithPreflight(writer, resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true}})
	if !errors.Is(err, flushErr) {
		t.Fatalf("error = %v, want wrapped flush error", err)
	}
	if !result.Committed || result.Retry || !result.StreamState.DownstreamStarted {
		t.Fatalf("started downstream delivery error must be terminal and non-retryable: %+v", result)
	}
	if result.Status != http.StatusOK || result.StreamDeliveryTrace == nil {
		t.Fatalf("healthy upstream status/trace must be preserved: %+v", result)
	}
	if result.StreamDeliveryTrace.End.Reason != "downstream_flush_error" {
		t.Fatalf("end reason = %q", result.StreamDeliveryTrace.End.Reason)
	}
}

func TestStreamResponsesDeliveryTraceRecordsUnsupportedFlush(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	completedData := []byte(`{"type":"response.completed","sequence_number":9,"response":{"id":"resp_no_flush","status":"completed"}}`)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(sse.Encode("response.completed", completedData))),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	writer := &flushErrorResponseWriter{header: make(http.Header), flushErr: http.ErrNotSupported}
	result, err := pipeline.streamResponsesWithPreflight(writer, resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true}})
	if !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("error = %v, want http.ErrNotSupported", err)
	}
	if !result.Committed || result.StreamDeliveryTrace == nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	completed := result.StreamDeliveryTrace.Downstream.FirstResponseCompleted
	if completed == nil || completed.WriteResult != "succeeded" || completed.FlushResult != "not_supported" {
		t.Fatalf("unexpected unsupported flush trace: %+v", completed)
	}
	if result.StreamDeliveryTrace.End.Reason != "downstream_terminal_flush_not_supported" || result.StreamDeliveryTrace.State() != "terminal_flush_not_supported" {
		t.Fatalf("unexpected trace end/state: %+v state=%q", result.StreamDeliveryTrace.End, result.StreamDeliveryTrace.State())
	}
	if result.StreamDeliveryTrace.Downstream.FinalBodyProducedAt != nil {
		t.Fatalf("unsupported flush must not claim final body completion: %+v", result.StreamDeliveryTrace.Downstream)
	}
}

func TestStreamResponsesDeliveryTraceRecordsPartialTerminalWrite(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	completedData := []byte(`{"type":"response.completed","sequence_number":4,"response":{"id":"resp_partial","status":"completed"}}`)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(sse.Encode("response.completed", completedData))),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	writeErr := errors.New("forced partial write")
	writer := &partialResponseWriter{header: make(http.Header), writeErr: writeErr}
	result, err := pipeline.streamResponsesWithPreflight(writer, resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true}})
	if !errors.Is(err, writeErr) {
		t.Fatalf("error = %v, want wrapped write error", err)
	}
	if !result.Committed || result.StreamDeliveryTrace == nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	completed := result.StreamDeliveryTrace.Downstream.FirstResponseCompleted
	if completed == nil || completed.WriteResult != "partial" || completed.WrittenBytes <= 0 || completed.WrittenBytes >= completed.WireBytes {
		t.Fatalf("unexpected partial write trace: %+v", completed)
	}
	if completed.FlushResult != "not_attempted" || completed.FlushAttemptedAt != nil || completed.FlushCompletedAt != nil {
		t.Fatalf("flush should not be attempted after partial write: %+v", completed)
	}
	if result.StreamDeliveryTrace.End.Reason != "downstream_terminal_write_error" || result.StreamDeliveryTrace.State() != "terminal_write_partial" {
		t.Fatalf("unexpected trace end/state: %+v state=%q", result.StreamDeliveryTrace.End, result.StreamDeliveryTrace.State())
	}
	if result.StreamDeliveryTrace.Downstream.FinalBodyProducedAt != nil {
		t.Fatalf("partial write must not claim final body completion: %+v", result.StreamDeliveryTrace.Downstream)
	}
}

func TestStreamResponsesDeliveryTracePreservesTerminalMissingSuccess(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	deltaData := []byte(`{"type":"response.output_text.delta","sequence_number":1,"delta":"ok"}`)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(sse.Encode("response.output_text.delta", deltaData))),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	result, err := pipeline.streamResponsesWithPreflight(httptest.NewRecorder(), resp, Attempt{Intent: RequestIntent{Endpoint: "/v1/responses", Stream: true}})
	if err != nil {
		t.Fatalf("terminal-missing clean EOF must preserve existing success semantics: %v", err)
	}
	if !result.Committed || result.Status != http.StatusOK || result.Retry {
		t.Fatalf("unexpected result: %+v", result)
	}
	trace := result.StreamDeliveryTrace
	if trace == nil || trace.Upstream.FirstResponseCompleted != nil || trace.Downstream.FirstResponseCompleted != nil {
		t.Fatalf("unexpected terminal trace: %+v", trace)
	}
	if trace.Upstream.EOFAt == nil || trace.Downstream.FinalBodyProducedAt == nil || trace.End.Reason != "upstream_eof_before_terminal" || trace.End.Error != "" {
		t.Fatalf("unexpected clean EOF trace: %+v", trace)
	}
	if trace.State() != "terminal_missing" {
		t.Fatalf("state = %q", trace.State())
	}
}

func TestChatCompletionStreamDoesNotEmitResponsesTerminalState(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
	}
	result, err := pipeline.streamResponsesWithPreflight(httptest.NewRecorder(), resp, Attempt{
		Intent: RequestIntent{Endpoint: "/v1/chat/completions", Stream: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.StreamDeliveryTrace != nil {
		t.Fatalf("chat stream must keep existing behavior without Responses terminal trace: %+v", result)
	}
}

func TestProxyExposesConnectionIDBeforeEarlyError(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{MaxRequestBodyBytes: 1}}}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("too large"))
	req = req.WithContext(observability.ContextWithConnectionID(req.Context(), "oaixc-test-early"))
	recorder := httptest.NewRecorder()
	pipeline.Proxy(recorder, req, RequestIntent{Endpoint: "/v1/responses"})
	if got := recorder.Header().Get("X-OAIX-Connection-ID"); got != "oaixc-test-early" {
		t.Fatalf("connection header = %q", got)
	}
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestRecordGatewayAttemptPersistsStreamDeliveryTrace(t *testing.T) {
	fakeStore := &fakeProxyStore{}
	pipeline := &Pipeline{store: fakeStore}
	trace := store.NewStreamDeliveryTrace("oaixc-attempt")
	pipeline.recordGatewayAttempt(context.Background(), Attempt{
		Index:     1,
		RequestID: "req-trace",
		Intent:    RequestIntent{Endpoint: "/v1/responses"},
		StartedAt: time.Now().UTC(),
	}, AttemptResult{Status: http.StatusOK, StreamDeliveryTrace: trace}, nil, OutcomeSuccess, false, false, nil)
	fakeStore.mu.Lock()
	defer fakeStore.mu.Unlock()
	if len(fakeStore.attempts) != 1 || fakeStore.attempts[0].StreamDeliveryTrace != trace {
		t.Fatalf("attempt trace not persisted: %+v", fakeStore.attempts)
	}
}

func TestWriteResponsesJSONFromSSECollectsTextPlainUpstream(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_1","object":"response","model":"gpt-5.5","status":"in_progress","output":[]}}`,
		``,
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","item":{"type":"message","status":"completed","content":[{"type":"output_text","text":"test"}],"role":"assistant"},"output_index":0}`,
		``,
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.5","status":"completed","output":[],"usage":{"input_tokens":8,"output_tokens":5,"total_tokens":13}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.writeResponsesJSONFromSSE(recorder, resp, Attempt{Intent: RequestIntent{Model: "gpt-5.5", RequireFast: true}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.ResponseID != "resp_1" || result.Usage == nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertCost(t, result.Usage.BaseCostUSD, 0.00019)
	assertCost(t, result.Usage.EstimatedCostUSD, 0.000475)
	if result.Usage.BillingMultiplier != 2.5 {
		t.Fatalf("Fast multiplier = %v, want 2.5", result.Usage.BillingMultiplier)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if recorder.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content-type should be application/json, got %q", recorder.Header().Get("Content-Type"))
	}
	output := payload["output"].([]any)
	content := output[0].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["text"] != "test" {
		t.Fatalf("output_item.done fallback was not patched: %v", payload)
	}
}

func TestDoAttemptCapturesFastUsageForNonStreamResponseModes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/compact") {
			_, _ = io.WriteString(w, `{"id":"resp_compact","response":{"usage":{"input_tokens":100,"input_tokens_details":{"cache_write_tokens":20,"cached_tokens":30},"output_tokens":10,"total_tokens":110}}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"chatcmpl_fast","usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`)
	}))
	defer upstream.Close()

	cfg := config.Config{Upstream: config.UpstreamConfig{
		ResponsesURL:              upstream.URL,
		ChatCompletionsURL:        upstream.URL,
		NonStreamMaxResponseBytes: 1 << 20,
		DisableCompression:        true,
	}}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	pipeline := &Pipeline{cfg: cfg, transport: client}
	claim := &tokens.Claim{Token: &tokens.RuntimeToken{Token: store.Token{ID: 1, AccessToken: "token"}}}
	tests := []struct {
		name      string
		intent    RequestIntent
		baseCost  float64
		finalCost float64
	}{
		{
			name:      "chat completions JSON",
			intent:    RequestIntent{Endpoint: "/v1/chat/completions", Model: "gpt-5.5", RequireFast: true},
			baseCost:  0.0008,
			finalCost: 0.002,
		},
		{
			name:      "responses compact JSON",
			intent:    RequestIntent{Endpoint: "/v1/responses/compact", Model: "gpt-5.6-luna", Compact: true, RequireFast: true},
			baseCost:  0.000138,
			finalCost: 0.000345,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.intent.Endpoint, strings.NewReader(`{"model":"`+test.intent.Model+`"}`))
			result, err := pipeline.doAttempt(recorder, request, Attempt{
				RequestID: "fast-non-stream",
				Intent:    test.intent,
				Claim:     claim,
				Body:      []byte(`{"model":"` + test.intent.Model + `"}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Committed || result.Usage == nil {
				t.Fatalf("usage was not captured: %+v", result)
			}
			assertCost(t, result.Usage.BaseCostUSD, test.baseCost)
			assertCost(t, result.Usage.EstimatedCostUSD, test.finalCost)
			if result.Usage.BillingMultiplier != fastCostMultiplierForPricingModel(result.Usage.PricingModel) {
				t.Fatalf("unexpected Fast usage: %+v", result.Usage)
			}
		})
	}
}

func TestDoAttemptUsageCaptureNeverTruncatesLargeDownstreamBody(t *testing.T) {
	largeBody := `{"id":"chatcmpl_large","usage":{"prompt_tokens":100,"completion_tokens":10},"padding":"` + strings.Repeat("x", 512) + `"}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, largeBody)
	}))
	defer upstream.Close()

	cfg := config.Config{Upstream: config.UpstreamConfig{
		ChatCompletionsURL:        upstream.URL,
		NonStreamMaxResponseBytes: 64,
		DisableCompression:        true,
	}}
	client := transport.New(cfg.Upstream)
	defer client.CloseIdleConnections()
	pipeline := &Pipeline{cfg: cfg, transport: client}
	claim := &tokens.Claim{Token: &tokens.RuntimeToken{Token: store.Token{ID: 1, AccessToken: "token"}}}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.5"}`))
	result, err := pipeline.doAttempt(recorder, request, Attempt{
		RequestID: "large-non-stream",
		Intent: RequestIntent{
			Endpoint:    "/v1/chat/completions",
			Model:       "gpt-5.5",
			RequireFast: true,
		},
		Claim: claim,
		Body:  []byte(`{"model":"gpt-5.5"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.Usage != nil {
		t.Fatalf("truncated observation must not guess usage: %+v", result)
	}
	if got := recorder.Body.String(); got != largeBody {
		t.Fatalf("downstream body was truncated: got %d bytes, want %d", len(got), len(largeBody))
	}
}

func TestStreamResponsesCapturesFastUsageForChatAndResponses(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	tests := []struct {
		name      string
		endpoint  string
		model     string
		body      string
		baseCost  float64
		finalCost float64
	}{
		{
			name:     "chat completions SSE",
			endpoint: "/v1/chat/completions",
			model:    "gpt-5.5",
			body: strings.Join([]string{
				`data: {"id":"chatcmpl_fast","usage":{"prompt_tokens":100,"completion_tokens":10,"total_tokens":110}}`,
				``,
				`data: [DONE]`,
				``,
			}, "\n"),
			baseCost:  0.0008,
			finalCost: 0.002,
		},
		{
			name:     "responses SSE",
			endpoint: "/v1/responses",
			model:    "gpt-5.6-luna",
			body: strings.Join([]string{
				`event: response.completed`,
				`data: {"type":"response.completed","response":{"id":"resp_fast","status":"completed","usage":{"input_tokens":100,"input_tokens_details":{"cache_write_tokens":20,"cached_tokens":30},"output_tokens":10,"total_tokens":110}}}`,
				``,
			}, "\n"),
			baseCost:  0.000138,
			finalCost: 0.000345,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(test.body)),
				Request:    httptest.NewRequest(http.MethodPost, test.endpoint, nil),
			}
			result, err := pipeline.streamResponsesWithPreflight(httptest.NewRecorder(), resp, Attempt{Intent: RequestIntent{
				Endpoint:    test.endpoint,
				Model:       test.model,
				Stream:      true,
				RequireFast: true,
			}})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Committed || result.Usage == nil {
				t.Fatalf("usage was not captured: %+v", result)
			}
			assertCost(t, result.Usage.BaseCostUSD, test.baseCost)
			assertCost(t, result.Usage.EstimatedCostUSD, test.finalCost)
		})
	}
}

func TestWriteImageJSONResponseFromSSE(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"created_at":1710000000,"output":[{"type":"image_generation_call","result":"QUJD","output_format":"png","size":"1024x1024","quality":"high","revised_prompt":"drawn"}],"tool_usage":{"image_gen":{"input_tokens":3}}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.writeImageJSONResponse(recorder, resp, Attempt{Intent: RequestIntent{Model: "gpt-image-2", ImageResponseFormat: "url", ImageStreamPrefix: "image_generation"}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed {
		t.Fatalf("expected committed image response: %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["created"].(float64) != 1710000000 || payload["output_format"] != "png" || payload["size"] != "1024x1024" {
		t.Fatalf("bad image response metadata: %v", payload)
	}
	data := payload["data"].([]any)[0].(map[string]any)
	if data["url"] != "data:image/png;base64,QUJD" || data["revised_prompt"] != "drawn" {
		t.Fatalf("bad image data: %v", data)
	}
	if payload["usage"].(map[string]any)["input_tokens"].(float64) != 3 {
		t.Fatalf("usage missing: %v", payload)
	}
}

func TestWriteImageJSONResponseTreatsEventStreamAcceptAsSSE(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{Upstream: config.UpstreamConfig{NonStreamMaxResponseBytes: 1 << 20}}}
	body := strings.Join([]string{
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"created_at":1710000001,"output":[{"type":"image_generation_call","result":"QUJD","output_format":"png"}],"tool_usage":{"image_gen":{"images":1}}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil),
	}
	recorder := httptest.NewRecorder()
	result, err := pipeline.writeImageJSONResponse(recorder, resp, Attempt{Intent: RequestIntent{
		Model:               "gpt-image-2",
		UpstreamAccept:      "text/event-stream",
		ImageResponseFormat: "b64_json",
		ImageStreamPrefix:   "image_generation",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.Status != http.StatusOK {
		t.Fatalf("expected committed image response: %+v", result)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	data := payload["data"].([]any)[0].(map[string]any)
	if data["b64_json"] != "QUJD" {
		t.Fatalf("event-stream body was not collected as image response: %v", payload)
	}
}

func defaultPromptCacheConfig() config.PromptCacheConfig {
	return config.PromptCacheConfig{
		AffinityEnabled:       true,
		AutoKeyEnabled:        true,
		MaxLanesPerKey:        3,
		PrimaryWait:           500 * time.Millisecond,
		LaneWait:              100 * time.Millisecond,
		PreviousOwnerWait:     800 * time.Millisecond,
		PreviousStrict:        true,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
		ResponseTTL:           24 * time.Hour,
	}
}

func zstdEncodeForTest(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

type failingWriter struct {
	failAfter int
	written   int
}

type countingResponseRecorder struct {
	*httptest.ResponseRecorder
	writeHeaderCount int
}

type flushErrorResponseWriter struct {
	header     http.Header
	body       bytes.Buffer
	status     int
	flushErr   error
	flushCalls int
}

type failNthFlushResponseWriter struct {
	header     http.Header
	body       bytes.Buffer
	status     int
	failAt     int
	flushErr   error
	flushCalls int
}

func (w *failNthFlushResponseWriter) Header() http.Header { return w.header }

func (w *failNthFlushResponseWriter) WriteHeader(status int) { w.status = status }

func (w *failNthFlushResponseWriter) Write(value []byte) (int, error) {
	return w.body.Write(value)
}

func (w *failNthFlushResponseWriter) FlushError() error {
	w.flushCalls++
	if w.flushCalls == w.failAt {
		return w.flushErr
	}
	return nil
}

func (w *flushErrorResponseWriter) Header() http.Header { return w.header }

func (w *flushErrorResponseWriter) WriteHeader(status int) { w.status = status }

func (w *flushErrorResponseWriter) Write(value []byte) (int, error) {
	return w.body.Write(value)
}

func (w *flushErrorResponseWriter) FlushError() error {
	w.flushCalls++
	return w.flushErr
}

type partialResponseWriter struct {
	header   http.Header
	status   int
	writeErr error
}

func (w *partialResponseWriter) Header() http.Header { return w.header }

func (w *partialResponseWriter) WriteHeader(status int) { w.status = status }

func (w *partialResponseWriter) Write(value []byte) (int, error) {
	n := len(value) / 2
	if n == 0 && len(value) > 0 {
		n = 1
	}
	return n, w.writeErr
}

type fakeProxyStore struct {
	mu               sync.Mutex
	tokens           []store.Token
	tokenErrorCh     chan *time.Time
	tokenErrorEvents chan tokenErrorEvent
	attempts         []store.GatewayRequestAttempt
}

type fakeOAuthClient struct {
	result oauth.RefreshResult
	err    error
}

func (c fakeOAuthClient) Refresh(context.Context, string) (oauth.RefreshResult, error) {
	return c.result, c.err
}

type tokenErrorEvent struct {
	TokenID       int64
	Message       string
	Deactivate    bool
	CooldownUntil *time.Time
}

func (s *fakeProxyStore) ListAvailableTokens(context.Context) ([]store.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	out := make([]store.Token, 0, len(s.tokens))
	for _, token := range s.tokens {
		if !token.IsActive || token.DisabledAt != nil {
			continue
		}
		if token.CooldownUntil != nil && token.CooldownUntil.After(now) {
			continue
		}
		out = append(out, token)
	}
	return out, nil
}

func (s *fakeProxyStore) TouchTokens(context.Context, []int64, time.Time) error {
	return nil
}

func (s *fakeProxyStore) MarkTokenSuccess(_ context.Context, tokenID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.tokens {
		if s.tokens[i].ID == tokenID {
			now := time.Now().UTC()
			s.tokens[i].IsActive = true
			s.tokens[i].DisabledAt = nil
			s.tokens[i].CooldownUntil = nil
			s.tokens[i].LastError = nil
			s.tokens[i].LastUsedAt = &now
			s.tokens[i].UpdatedAt = now
			break
		}
	}
	return nil
}

func (s *fakeProxyStore) MarkTokenError(_ context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error {
	return s.MarkTokenErrorWithContext(context.Background(), tokenID, message, deactivate, cooldownUntil, store.TokenStateEventContext{})
}

func (s *fakeProxyStore) MarkTokenErrorWithContext(_ context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time, _ store.TokenStateEventContext) error {
	s.mu.Lock()
	for i := range s.tokens {
		if s.tokens[i].ID == tokenID {
			msg := message
			now := time.Now().UTC()
			s.tokens[i].LastError = &msg
			s.tokens[i].CooldownUntil = cooldownUntil
			if deactivate {
				s.tokens[i].IsActive = false
				s.tokens[i].DisabledAt = &now
			}
			s.tokens[i].UpdatedAt = now
			break
		}
	}
	s.mu.Unlock()
	if s.tokenErrorCh != nil {
		s.tokenErrorCh <- cooldownUntil
	}
	if s.tokenErrorEvents != nil {
		s.tokenErrorEvents <- tokenErrorEvent{
			TokenID:       tokenID,
			Message:       message,
			Deactivate:    deactivate,
			CooldownUntil: cooldownUntil,
		}
	}
	return nil
}

func (s *fakeProxyStore) UpdateTokenSecret(_ context.Context, update store.TokenSecretUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !update.RequireCredentialMatch {
		return fmt.Errorf("credential refresh update must be fenced")
	}
	for i := range s.tokens {
		if s.tokens[i].ID != update.TokenID {
			continue
		}
		if update.RequireCredentialMatch && (s.tokens[i].AccessToken != update.ExpectedAccessToken || s.tokens[i].RefreshToken != update.ExpectedRefreshToken) {
			return store.ErrTokenCredentialsChanged
		}
		if update.AccessToken != "" {
			s.tokens[i].AccessToken = update.AccessToken
		}
		if update.RefreshToken != "" {
			s.tokens[i].RefreshToken = update.RefreshToken
		}
		if update.AccountID != "" {
			accountID := update.AccountID
			s.tokens[i].AccountID = &accountID
		}
		if update.Email != "" {
			email := update.Email
			s.tokens[i].Email = &email
		}
		if update.PlanType != "" {
			planType := update.PlanType
			s.tokens[i].PlanType = &planType
		}
		if !update.PreserveActivation {
			s.tokens[i].IsActive = true
			s.tokens[i].DisabledAt = nil
			s.tokens[i].LastError = nil
		}
		s.tokens[i].UpdatedAt = time.Now().UTC()
		return nil
	}
	return fmt.Errorf("token %d not found", update.TokenID)
}

func (s *fakeProxyStore) InsertGatewayRequestAttempt(_ context.Context, item store.GatewayRequestAttempt) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := int64(len(s.attempts) + 1)
	s.attempts = append(s.attempts, item)
	return id, nil
}

func (s *fakeProxyStore) UpsertRequestLogs(context.Context, []store.RequestLog) error {
	return nil
}

func (s *fakeProxyStore) EnqueueRequestLogOutbox(context.Context, store.RequestLog) error {
	return nil
}

func (r *countingResponseRecorder) WriteHeader(status int) {
	r.writeHeaderCount++
	r.ResponseRecorder.WriteHeader(status)
}

func (w failingWriter) Header() http.Header {
	return http.Header{}
}

func (w failingWriter) WriteHeader(int) {}

func (w failingWriter) Write(p []byte) (int, error) {
	if w.written+len(p) > w.failAfter {
		return w.failAfter - w.written, fmt.Errorf("client disconnected")
	}
	w.written += len(p)
	return len(p), nil
}

var _ http.ResponseWriter = failingWriter{}
var _ io.Writer = failingWriter{}
