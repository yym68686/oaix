package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
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

func TestCopyProxyHeadersStripsBodyEncodingHeaders(t *testing.T) {
	src := http.Header{}
	src.Set("Content-Encoding", "zstd")
	src.Set("Accept-Encoding", "zstd")
	src.Set("Authorization", "Bearer client")
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
	if got := dst.Get("X-Custom"); got != "ok" {
		t.Fatalf("X-Custom = %q", got)
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
	if payload["model"] != "gpt-5.5" || payload["input"] != "hello" {
		t.Fatalf("unexpected upstream payload: %v", payload)
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
		`data: {"type":"response.failed","response":{"status":"failed","error":{"code":"rate_limit_exceeded","message":"Concurrency limit exceeded for account, please retry later"}}}`,
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
	if err == nil {
		t.Fatal("expected retryable preflight error")
	}
	if result.Committed || !result.Retry || result.Status != http.StatusTooManyRequests {
		t.Fatalf("unexpected result: %+v err=%v", result, err)
	}
	if !result.StreamState.DownstreamStarted || !result.StreamState.KeepaliveSent {
		t.Fatalf("response.created should start downstream with keepalive: %+v", result.StreamState)
	}
	output := recorder.Body.String()
	if !strings.Contains(output, "event: keepalive") || !strings.Contains(output, `"type":"keepalive"`) {
		t.Fatalf("keepalive should be written immediately: %q", output)
	}
	if strings.Contains(output, "response.created") || strings.Contains(output, "response.failed") {
		t.Fatalf("preflight failure should not flush buffered upstream events: %q", output)
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
	result, err := pipeline.writeResponsesJSONFromSSE(recorder, resp, Attempt{Intent: RequestIntent{Model: "gpt-5.5"}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Committed || result.ResponseID != "resp_1" || result.Usage == nil {
		t.Fatalf("unexpected result: %+v", result)
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

type fakeProxyStore struct {
	tokens       []store.Token
	tokenErrorCh chan *time.Time
}

func (s *fakeProxyStore) ListAvailableTokens(context.Context) ([]store.Token, error) {
	return append([]store.Token(nil), s.tokens...), nil
}

func (s *fakeProxyStore) TouchTokens(context.Context, []int64, time.Time) error {
	return nil
}

func (s *fakeProxyStore) MarkTokenSuccess(context.Context, int64) error {
	return nil
}

func (s *fakeProxyStore) MarkTokenError(_ context.Context, _ int64, _ string, _ bool, cooldownUntil *time.Time) error {
	if s.tokenErrorCh != nil {
		s.tokenErrorCh <- cooldownUntil
	}
	return nil
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
