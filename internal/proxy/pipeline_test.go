package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	written, err := copyAndFlush(writer, strings.NewReader("hello world"))
	if err == nil {
		t.Fatal("expected write error")
	}
	if written == 0 {
		t.Fatal("expected partial write count")
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
