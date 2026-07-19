package proxy

import (
	"context"
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

	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

func TestGatewayIdempotencyReplaysCompletedResponse(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("X-Upstream-Test", "preserved")
		writeGatewayIdempotencySSE(w, "resp_idempotent", `[{"type":"message"}]`)
	}))
	defer upstream.Close()

	pipeline, coordinator := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)
	body := `{"model":"gpt-5.5","input":"hello","stream":false}`
	first := httptest.NewRecorder()
	pipeline.Proxy(first, gatewayIdempotencyTestRequest(body, "attempt-replay", "request-first"), gatewayIdempotencyTestIntent())
	second := httptest.NewRecorder()
	pipeline.Proxy(second, gatewayIdempotencyTestRequest(body, "attempt-replay", "request-second"), gatewayIdempotencyTestIntent())

	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d, %d; bodies = %q, %q", first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("replayed body changed: first=%q second=%q", first.Body.String(), second.Body.String())
	}
	if got := first.Header().Get(gatewayIdempotencyStatusHeader); got != "executed" {
		t.Fatalf("first idempotency status = %q", got)
	}
	if got := second.Header().Get(gatewayIdempotencyStatusHeader); got != "replayed" {
		t.Fatalf("second idempotency status = %q", got)
	}
	if got := second.Header().Get("X-Upstream-Test"); got != "preserved" {
		t.Fatalf("replayed response header = %q", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls.Load())
	}
	if coordinator.beginCalls.Load() < 2 || coordinator.completeCalls.Load() != 1 {
		t.Fatalf("coordinator calls begin=%d complete=%d", coordinator.beginCalls.Load(), coordinator.completeCalls.Load())
	}
}

func TestGatewayIdempotencyCoalescesConcurrentRequests(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		writeGatewayIdempotencySSE(w, "resp_concurrent", `[]`)
	}))
	defer upstream.Close()

	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)
	body := `{"model":"gpt-5.5","input":"one execution","stream":false}`
	first := httptest.NewRecorder()
	second := httptest.NewRecorder()
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		pipeline.Proxy(first, gatewayIdempotencyTestRequest(body, "attempt-concurrent", "request-concurrent-a"), gatewayIdempotencyTestIntent())
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not reach upstream")
	}
	go func() {
		defer close(secondDone)
		pipeline.Proxy(second, gatewayIdempotencyTestRequest(body, "attempt-concurrent", "request-concurrent-b"), gatewayIdempotencyTestIntent())
	}()
	time.Sleep(75 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("concurrent duplicate reached upstream %d times", calls.Load())
	}
	close(release)
	for name, done := range map[string]<-chan struct{}{"first": firstDone, "second": secondDone} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s request did not finish", name)
		}
	}
	if first.Code != http.StatusOK || second.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("coalesced responses differ: first=%d/%q second=%d/%q", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if second.Header().Get(gatewayIdempotencyStatusHeader) != "replayed" {
		t.Fatalf("duplicate status = %q", second.Header().Get(gatewayIdempotencyStatusHeader))
	}
}

func TestGatewayIdempotencyHeartbeatPreventsLeaseTakeover(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		writeGatewayIdempotencySSE(w, "resp_heartbeat", `[]`)
	}))
	defer upstream.Close()
	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)
	pipeline.cfg.Idempotency.LeaseDuration = 120 * time.Millisecond
	pipeline.cfg.Idempotency.HeartbeatInterval = 20 * time.Millisecond
	body := `{"model":"gpt-5.5","input":"heartbeat"}`
	first := httptest.NewRecorder()
	second := httptest.NewRecorder()
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		pipeline.Proxy(first, gatewayIdempotencyTestRequest(body, "attempt-heartbeat", "request-heartbeat-a"), gatewayIdempotencyTestIntent())
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not reach upstream")
	}
	time.Sleep(250 * time.Millisecond)
	go func() {
		defer close(secondDone)
		pipeline.Proxy(second, gatewayIdempotencyTestRequest(body, "attempt-heartbeat", "request-heartbeat-b"), gatewayIdempotencyTestIntent())
	}()
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("live heartbeat lease was taken over: calls=%d", calls.Load())
	}
	close(release)
	for _, done := range []<-chan struct{}{firstDone, secondDone} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("heartbeat request did not finish")
		}
	}
	if first.Code != http.StatusOK || second.Code != http.StatusOK || second.Header().Get(gatewayIdempotencyStatusHeader) != "replayed" {
		t.Fatalf("heartbeat responses first=%d second=%d status=%q", first.Code, second.Code, second.Header().Get(gatewayIdempotencyStatusHeader))
	}
}

func TestGatewayIdempotencyRejectsKeyReuseWithDifferentRequest(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeGatewayIdempotencySSE(w, "resp_conflict", `[]`)
	}))
	defer upstream.Close()
	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)

	first := httptest.NewRecorder()
	pipeline.Proxy(first, gatewayIdempotencyTestRequest(`{"model":"gpt-5.5","input":"first"}`, "attempt-conflict", "request-conflict-a"), gatewayIdempotencyTestIntent())
	second := httptest.NewRecorder()
	pipeline.Proxy(second, gatewayIdempotencyTestRequest(`{"model":"gpt-5.5","input":"different"}`, "attempt-conflict", "request-conflict-b"), gatewayIdempotencyTestIntent())

	if first.Code != http.StatusOK || second.Code != http.StatusConflict {
		t.Fatalf("statuses = %d, %d; conflict body=%q", first.Code, second.Code, second.Body.String())
	}
	if second.Header().Get(gatewayIdempotencyStatusHeader) != "conflict" {
		t.Fatalf("conflict status header = %q", second.Header().Get(gatewayIdempotencyStatusHeader))
	}
	if calls.Load() != 1 {
		t.Fatalf("conflicting request reached upstream: calls=%d", calls.Load())
	}
}

func TestGatewayIdempotencyContinuesAfterDownstreamCancellation(t *testing.T) {
	var calls atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		writeGatewayIdempotencySSE(w, "resp_after_disconnect", `[]`)
	}))
	defer upstream.Close()
	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)
	body := `{"model":"gpt-5.5","input":"survive disconnect"}`

	requestContext, cancelRequest := context.WithCancel(context.Background())
	firstRequest := gatewayIdempotencyTestRequest(body, "attempt-disconnect", "request-disconnect-a").WithContext(requestContext)
	firstWriter := &alwaysFailResponseWriter{header: make(http.Header)}
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		pipeline.Proxy(firstWriter, firstRequest, gatewayIdempotencyTestIntent())
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach upstream")
	}
	cancelRequest()
	close(release)
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("detached idempotent execution did not finish")
	}

	replay := httptest.NewRecorder()
	pipeline.Proxy(replay, gatewayIdempotencyTestRequest(body, "attempt-disconnect", "request-disconnect-b"), gatewayIdempotencyTestIntent())
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), "resp_after_disconnect") {
		t.Fatalf("replay after disconnect = %d %q", replay.Code, replay.Body.String())
	}
	if replay.Header().Get(gatewayIdempotencyStatusHeader) != "replayed" {
		t.Fatalf("replay status = %q", replay.Header().Get(gatewayIdempotencyStatusHeader))
	}
	if calls.Load() != 1 {
		t.Fatalf("disconnect caused duplicate upstream execution: calls=%d", calls.Load())
	}
}

func TestGatewayIdempotencyDoesNotCacheOversizedResponse(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeGatewayIdempotencySSE(w, "response-larger-than-limit", `[]`)
	}))
	defer upstream.Close()
	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 8)
	body := `{"model":"gpt-5.5","input":"large"}`

	for index := 0; index < 2; index++ {
		response := httptest.NewRecorder()
		pipeline.Proxy(response, gatewayIdempotencyTestRequest(body, "attempt-large", fmt.Sprintf("request-large-%d", index)), gatewayIdempotencyTestIntent())
		if response.Code != http.StatusOK || response.Header().Get(gatewayIdempotencyStatusHeader) != "executed" {
			t.Fatalf("response %d = %d status=%q body=%q", index, response.Code, response.Header().Get(gatewayIdempotencyStatusHeader), response.Body.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("oversized response was incorrectly replayed: calls=%d", calls.Load())
	}
}

func TestGatewayIdempotencyDoesNotCacheServerError(t *testing.T) {
	pipeline := &Pipeline{cfg: config.Config{
		Database: config.DatabaseConfig{AcquireTimeout: time.Second},
		Idempotency: config.IdempotencyConfig{
			Enabled:           true,
			RecordTTL:         time.Minute,
			LeaseDuration:     time.Second,
			HeartbeatInterval: 100 * time.Millisecond,
			WaitTimeout:       time.Second,
			ExecutionTimeout:  time.Second,
			MaxResponseBytes:  1024,
		},
	}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	coordinator := newMemoryGatewayIdempotencyStore()
	keyHash := hashText("attempt-server-error")
	requestHash := hashText("request-server-error")
	claim, err := coordinator.BeginGatewayIdempotency(context.Background(), store.GatewayIdempotencyBegin{
		OwnerUserID: 42, KeyHash: keyHash, RequestHash: requestHash, RequestID: "request-server-error",
		LeaseToken: "lease-server-error", LeaseDuration: time.Second, RecordTTL: time.Minute,
	})
	if err != nil || claim.Action != store.GatewayIdempotencyExecute {
		t.Fatalf("claim = %+v, %v", claim, err)
	}
	recorder := httptest.NewRecorder()
	capture := newIdempotencyCaptureWriter(recorder, 1024)
	capture.WriteHeader(http.StatusServiceUnavailable)
	_, _ = capture.Write([]byte(`{"error":"temporary"}`))
	ctx, cancel := context.WithCancel(context.Background())
	execution := &gatewayIdempotencyExecution{
		pipeline: pipeline, store: coordinator, ownerUserID: 42, keyHash: keyHash,
		requestHash: requestHash, requestID: "request-server-error", leaseToken: "lease-server-error",
		generation: claim.Record.Generation, writer: capture, request: httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx),
		cancelRequest: cancel, stopHeartbeat: func() {},
	}
	execution.finish()

	next, err := coordinator.BeginGatewayIdempotency(context.Background(), store.GatewayIdempotencyBegin{
		OwnerUserID: 42, KeyHash: keyHash, RequestHash: requestHash, RequestID: "request-server-error-retry",
		LeaseToken: "lease-server-error-retry", LeaseDuration: time.Second, RecordTTL: time.Minute,
	})
	if err != nil || next.Action != store.GatewayIdempotencyExecute || next.Record.Generation != 2 {
		t.Fatalf("server error was cached instead of reclaimed: result=%+v err=%v", next, err)
	}
}

func TestGatewayIdempotencyRequiresCoordinatorBeforeUpstream(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	now := time.Now().UTC()
	baseStore := &fakeProxyStore{tokens: []store.Token{{ID: 1, OwnerUserID: 42, AccessToken: "token", IsActive: true, CreatedAt: now, UpdatedAt: now}}}
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 1, baseStore)
	pipeline.cfg.Idempotency = gatewayIdempotencyTestConfig(1024)
	response := httptest.NewRecorder()
	pipeline.Proxy(response, gatewayIdempotencyTestRequest(`{"model":"gpt-5.5","input":"hello"}`, "attempt-no-store", "request-no-store"), gatewayIdempotencyTestIntent())
	if response.Code != http.StatusServiceUnavailable || response.Header().Get(gatewayIdempotencyStatusHeader) != "unavailable" {
		t.Fatalf("missing coordinator response = %d status=%q body=%q", response.Code, response.Header().Get(gatewayIdempotencyStatusHeader), response.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("request executed without idempotency coordinator: calls=%d", calls.Load())
	}
}

func TestGatewayIdempotencyHeaderIsExplicitOptIn(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeGatewayIdempotencySSE(w, "ordinary", `[]`)
	}))
	defer upstream.Close()
	pipeline, coordinator := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1024)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"ordinary"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	pipeline.Proxy(response, request, gatewayIdempotencyTestIntent())
	if response.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("ordinary request = %d calls=%d body=%q", response.Code, calls.Load(), response.Body.String())
	}
	if coordinator.beginCalls.Load() != 0 || response.Header().Get(gatewayIdempotencyStatusHeader) != "" {
		t.Fatalf("ordinary request unexpectedly entered idempotency: begin=%d header=%q", coordinator.beginCalls.Load(), response.Header().Get(gatewayIdempotencyStatusHeader))
	}
}

func TestGatewayIdempotencyRejectsInvalidAttemptIDBeforeUpstream(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	pipeline, coordinator := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1024)
	request := gatewayIdempotencyTestRequest(`{"model":"gpt-5.5"}`, strings.Repeat("x", maxGatewayRoutingAttemptIDBytes+1), "request-invalid-key")
	response := httptest.NewRecorder()
	pipeline.Proxy(response, request, gatewayIdempotencyTestIntent())
	if response.Code != http.StatusBadRequest || calls.Load() != 0 || coordinator.beginCalls.Load() != 0 {
		t.Fatalf("invalid key response=%d calls=%d begins=%d body=%q", response.Code, calls.Load(), coordinator.beginCalls.Load(), response.Body.String())
	}
}

func TestGatewayIdempotencyRequestHashBindsSemanticsNotCorrelation(t *testing.T) {
	apiKeyID := int64(7)
	intent := gatewayIdempotencyTestIntent()
	intent.APIKeyID = &apiKeyID
	firstHeaders := http.Header{
		"Authorization": []string{"Bearer stable-credential"},
		"Content-Type":  []string{"application/json"},
		"X-Request-Id":  []string{"correlation-a"},
	}
	secondHeaders := firstHeaders.Clone()
	secondHeaders.Set("X-Request-ID", "correlation-b")
	body := []byte(`{"model":"gpt-5.5","input":"same"}`)
	first, err := gatewayIdempotencyRequestHash(intent, body, firstHeaders, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := gatewayIdempotencyRequestHash(intent, body, secondHeaders, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("correlation-only header changed request hash: %s != %s", first, second)
	}
	secondHeaders.Set("Authorization", "Bearer different-credential")
	credentialChanged, err := gatewayIdempotencyRequestHash(intent, body, secondHeaders, nil)
	if err != nil {
		t.Fatal(err)
	}
	if credentialChanged == first {
		t.Fatal("credential change did not change request hash")
	}
	otherAPIKeyID := int64(8)
	intent.APIKeyID = &otherAPIKeyID
	apiKeyChanged, err := gatewayIdempotencyRequestHash(intent, body, firstHeaders, nil)
	if err != nil {
		t.Fatal(err)
	}
	if apiKeyChanged == first {
		t.Fatal("API key scope change did not change request hash")
	}
}

func TestGatewayIdempotencyReplaysStreamingBody(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer upstream.Close()
	pipeline, _ := newGatewayIdempotencyTestPipeline(t, upstream.URL, 1<<20)
	body := `{"model":"gpt-5.5","input":"stream","stream":true}`
	intent := gatewayIdempotencyTestIntent()
	intent.Stream = true
	first := httptest.NewRecorder()
	pipeline.Proxy(first, gatewayIdempotencyTestRequest(body, "attempt-stream", "request-stream-a"), intent)
	second := httptest.NewRecorder()
	pipeline.Proxy(second, gatewayIdempotencyTestRequest(body, "attempt-stream", "request-stream-b"), intent)
	if first.Code != http.StatusOK || second.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatalf("stream replay differs: first=%d/%q second=%d/%q", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if second.Header().Get(gatewayIdempotencyStatusHeader) != "replayed" || calls.Load() != 1 {
		t.Fatalf("stream replay status=%q calls=%d", second.Header().Get(gatewayIdempotencyStatusHeader), calls.Load())
	}
}

func newGatewayIdempotencyTestPipeline(t *testing.T, upstreamURL string, maxResponseBytes int64) (*Pipeline, *memoryGatewayIdempotencyStore) {
	t.Helper()
	now := time.Now().UTC()
	baseStore := &fakeProxyStore{tokens: []store.Token{{
		ID: 1, OwnerUserID: 42, AccessToken: "upstream-token", IsActive: true, CreatedAt: now, UpdatedAt: now,
	}}}
	coordinator := newMemoryGatewayIdempotencyStore()
	coordinator.fakeProxyStore = baseStore
	cfg := config.Config{
		Database:    config.DatabaseConfig{AcquireTimeout: time.Second},
		Idempotency: gatewayIdempotencyTestConfig(maxResponseBytes),
		Upstream: config.UpstreamConfig{
			ResponsesURL: upstreamURL, MaxRetries: 1, NonStreamMaxResponseBytes: 1 << 20, DisableCompression: true,
		},
		TokenPool:  config.TokenPoolConfig{DefaultCooldown: time.Second},
		RequestLog: config.RequestLogConfig{QueueMaxSize: 20, BatchSize: 1, Concurrency: 1},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := tokens.NewManager(coordinator, logger, time.Minute, time.Minute, 2)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	client := transport.New(cfg.Upstream)
	t.Cleanup(client.CloseIdleConnections)
	logWriter := logs.NewWriter(coordinator, logger, cfg.RequestLog)
	return New(cfg, logger, manager, client, logWriter, coordinator, affinity.NewMemoryStore()), coordinator
}

func gatewayIdempotencyTestConfig(maxResponseBytes int64) config.IdempotencyConfig {
	return config.IdempotencyConfig{
		Enabled: true, RecordTTL: time.Minute, LeaseDuration: time.Second,
		HeartbeatInterval: 100 * time.Millisecond, WaitTimeout: 3 * time.Second,
		ExecutionTimeout: 3 * time.Second, MaxResponseBytes: maxResponseBytes,
	}
}

func gatewayIdempotencyTestIntent() RequestIntent {
	return RequestIntent{Endpoint: "/v1/responses", OwnerUserID: 42, Model: "gpt-5.5"}
}

func gatewayIdempotencyTestRequest(body, attemptID, requestID string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(gatewayRoutingAttemptHeader, attemptID)
	request.Header.Set("X-Request-ID", requestID)
	return request
}

func writeGatewayIdempotencySSE(w http.ResponseWriter, responseID, output string) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(
		w,
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"completed\",\"output\":%s,\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n",
		responseID,
		output,
	)
}

type alwaysFailResponseWriter struct {
	header http.Header
	status int
}

func (w *alwaysFailResponseWriter) Header() http.Header    { return w.header }
func (w *alwaysFailResponseWriter) WriteHeader(status int) { w.status = status }
func (w *alwaysFailResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("downstream disconnected")
}

type memoryGatewayIdempotencyStore struct {
	*fakeProxyStore
	mu            sync.Mutex
	records       map[string]store.GatewayIdempotencyRecord
	beginCalls    atomic.Int64
	completeCalls atomic.Int64
}

func newMemoryGatewayIdempotencyStore() *memoryGatewayIdempotencyStore {
	return &memoryGatewayIdempotencyStore{fakeProxyStore: &fakeProxyStore{}, records: make(map[string]store.GatewayIdempotencyRecord)}
}

func (s *memoryGatewayIdempotencyStore) BeginGatewayIdempotency(_ context.Context, input store.GatewayIdempotencyBegin) (store.GatewayIdempotencyBeginResult, error) {
	s.beginCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	key := fmt.Sprintf("%d:%s", input.OwnerUserID, input.KeyHash)
	record, found := s.records[key]
	if !found {
		lease := input.LeaseToken
		leaseExpires := now.Add(input.LeaseDuration)
		record = store.GatewayIdempotencyRecord{
			OwnerUserID: input.OwnerUserID, KeyHash: input.KeyHash, RequestHash: input.RequestHash,
			RequestID: input.RequestID, Generation: 1, State: "in_progress", LeaseToken: &lease,
			LeaseExpiresAt: &leaseExpires, CreatedAt: now, StartedAt: now, UpdatedAt: now, ExpiresAt: now.Add(input.RecordTTL),
		}
		s.records[key] = record
		return store.GatewayIdempotencyBeginResult{Action: store.GatewayIdempotencyExecute, Record: cloneGatewayIdempotencyRecord(record)}, nil
	}
	if record.ExpiresAt.After(now) && record.RequestHash != input.RequestHash {
		return store.GatewayIdempotencyBeginResult{Action: store.GatewayIdempotencyConflict, Record: cloneGatewayIdempotencyRecord(record)}, nil
	}
	if record.ExpiresAt.After(now) && record.State == "completed" && record.Replayable {
		return store.GatewayIdempotencyBeginResult{Action: store.GatewayIdempotencyReplay, Record: cloneGatewayIdempotencyRecord(record)}, nil
	}
	if record.ExpiresAt.After(now) && record.State == "in_progress" && record.LeaseExpiresAt != nil && record.LeaseExpiresAt.After(now) {
		return store.GatewayIdempotencyBeginResult{Action: store.GatewayIdempotencyWait, Record: cloneGatewayIdempotencyRecord(record)}, nil
	}
	lease := input.LeaseToken
	leaseExpires := now.Add(input.LeaseDuration)
	record.RequestHash = input.RequestHash
	record.RequestID = input.RequestID
	record.Generation++
	record.State = "in_progress"
	record.LeaseToken = &lease
	record.LeaseExpiresAt = &leaseExpires
	record.StatusCode = nil
	record.ResponseHeaders = nil
	record.ResponseBody = nil
	record.Replayable = false
	record.FailureReason = nil
	record.StartedAt = now
	record.CompletedAt = nil
	record.UpdatedAt = now
	record.ExpiresAt = now.Add(input.RecordTTL)
	s.records[key] = record
	return store.GatewayIdempotencyBeginResult{Action: store.GatewayIdempotencyExecute, Record: cloneGatewayIdempotencyRecord(record)}, nil
}

func (s *memoryGatewayIdempotencyStore) RenewGatewayIdempotencyLease(_ context.Context, ownerUserID int64, keyHash, leaseToken string, leaseDuration, recordTTL time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%d:%s", ownerUserID, keyHash)
	record, found := s.records[key]
	if !found || record.State != "in_progress" || record.LeaseToken == nil || *record.LeaseToken != leaseToken {
		return false, nil
	}
	now := time.Now().UTC()
	leaseExpires := now.Add(leaseDuration)
	record.LeaseExpiresAt = &leaseExpires
	if extended := now.Add(recordTTL); extended.After(record.ExpiresAt) {
		record.ExpiresAt = extended
	}
	record.UpdatedAt = now
	s.records[key] = record
	return true, nil
}

func (s *memoryGatewayIdempotencyStore) CompleteGatewayIdempotency(_ context.Context, completion store.GatewayIdempotencyCompletion) (bool, error) {
	s.completeCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%d:%s", completion.OwnerUserID, completion.KeyHash)
	record, found := s.records[key]
	if !found || record.State != "in_progress" || record.LeaseToken == nil || *record.LeaseToken != completion.LeaseToken {
		return false, nil
	}
	now := time.Now().UTC()
	status := completion.StatusCode
	record.State = "completed"
	record.LeaseToken = nil
	record.LeaseExpiresAt = nil
	record.StatusCode = &status
	record.ResponseHeaders = append(record.ResponseHeaders[:0], completion.ResponseHeaders...)
	record.ResponseBody = append(record.ResponseBody[:0], completion.ResponseBody...)
	record.Replayable = true
	record.FailureReason = nil
	record.CompletedAt = &now
	record.UpdatedAt = now
	record.ExpiresAt = now.Add(completion.RecordTTL)
	s.records[key] = record
	return true, nil
}

func (s *memoryGatewayIdempotencyStore) FailGatewayIdempotency(_ context.Context, ownerUserID int64, keyHash, leaseToken, reason string, recordTTL time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%d:%s", ownerUserID, keyHash)
	record, found := s.records[key]
	if !found || record.State != "in_progress" || record.LeaseToken == nil || *record.LeaseToken != leaseToken {
		return false, nil
	}
	now := time.Now().UTC()
	record.State = "failed"
	record.LeaseToken = nil
	record.LeaseExpiresAt = nil
	record.StatusCode = nil
	record.ResponseHeaders = nil
	record.ResponseBody = nil
	record.Replayable = false
	record.FailureReason = &reason
	record.CompletedAt = &now
	record.UpdatedAt = now
	record.ExpiresAt = now.Add(recordTTL)
	s.records[key] = record
	return true, nil
}

func cloneGatewayIdempotencyRecord(record store.GatewayIdempotencyRecord) store.GatewayIdempotencyRecord {
	record.ResponseHeaders = append(record.ResponseHeaders[:0:0], record.ResponseHeaders...)
	record.ResponseBody = append(record.ResponseBody[:0:0], record.ResponseBody...)
	return record
}
