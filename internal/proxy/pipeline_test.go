package proxy

import (
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

type failingWriter struct {
	failAfter int
	written   int
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
