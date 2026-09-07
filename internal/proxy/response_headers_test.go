package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
)

func TestProxyRetryResponseHeaders(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		stream, keepalive, exhausted, image bool
	}{
		{name: "non_stream_eof"},
		{name: "stream_preflight", stream: true},
		{name: "stream_after_keepalive", stream: true, keepalive: true},
		{name: "exhausted_non_stream", exhausted: true},
		{name: "exhausted_stream", stream: true, exhausted: true},
		{name: "image_collection", image: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-OAI-Request-ID", fmt.Sprintf("attempt-%d", call))
				w.Header().Set("X-Codex-Primary-Used-Percent", fmt.Sprint(call))
				w.Header().Add("X-Multiple", "one")
				w.Header().Add("X-Multiple", "two")
				if call == 1 || tc.exhausted {
					w.Header().Set("X-Failed-Only", "must-not-leak")
					if tc.keepalive {
						_, _ = w.Write(sse.Encode("response.created", []byte(`{"type":"response.created","response":{"id":"failed"}}`)))
						w.(http.Flusher).Flush()
					}
					if tc.stream {
						_, _ = w.Write(sse.Encode("response.failed", []byte(`{"type":"response.failed","response":{"id":"failed","error":{"code":"server_error","message":"try again"}}}`)))
					} else {
						// A real HTTP body short read, matching the reported non-stream failure.
						w.Header().Set("Content-Length", "1000")
						_, _ = io.WriteString(w, "data: ")
					}
					return
				}
				payload := `{"type":"response.completed","response":{"id":"winner","status":"completed","output":[]}}`
				if tc.image {
					payload = `{"type":"response.completed","response":{"id":"winner","status":"completed","output":[{"type":"image_generation_call","result":"aW1hZ2U="}]}}`
				}
				_, _ = w.Write(sse.Encode("response.completed", []byte(payload)))
			}))
			defer upstream.Close()
			now := time.Now().UTC()
			fakes := &fakeProxyStore{tokens: []store.Token{
				{ID: 1, OwnerUserID: 1, AccessToken: "token-1", IsActive: true, CreatedAt: now, UpdatedAt: now},
				{ID: 2, OwnerUserID: 1, AccessToken: "token-2", IsActive: true, CreatedAt: now, UpdatedAt: now},
			}}
			p := newProxyPipelineTestHarness(t, upstream.URL, 2, fakes)
			intent := RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5", Stream: tc.stream}
			if tc.image {
				intent.ImageResponseFormat = "b64_json"
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"gpt-5.5","input":"hello","stream":%t}`, tc.stream)))
			w := httptest.NewRecorder()
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Add("X-Gateway-Multiple", "a")
			w.Header().Add("X-Gateway-Multiple", "b")
			w.Header().Set("X-OAIX-Idempotency-Status", "executed")
			p.Proxy(w, req, intent)
			if calls.Load() != 2 {
				t.Fatalf("attempts=%d body=%s", calls.Load(), w.Body.String())
			}
			response := w.Result()
			defer response.Body.Close()
			for name, h := range map[string]http.Header{"wire": response.Header, "writer": w.Header()} {
				if h.Get("Access-Control-Allow-Origin") != "*" || h.Get("X-OAIX-Idempotency-Status") != "executed" || !reflect.DeepEqual(h.Values("X-Gateway-Multiple"), []string{"a", "b"}) {
					t.Fatalf("%s lost gateway headers: %v", name, h)
				}
				if tc.exhausted {
					if h.Get("X-Failed-Only") != "" || h.Get("X-OAI-Request-ID") != "" || h.Get("X-OAIX-Token-ID") != "" {
						t.Errorf("%s leaked failed headers: %v", name, h)
					}
					continue
				}
				want := "attempt-2"
				if tc.keepalive {
					want = "attempt-1"
				} // Already sent HTTP headers cannot change after keepalive.
				if !reflect.DeepEqual(h.Values("X-OAI-Request-ID"), []string{want}) {
					t.Errorf("%s upstream IDs=%v want=%s", name, h.Values("X-OAI-Request-ID"), want)
				}
				if !reflect.DeepEqual(h.Values("X-Multiple"), []string{"one", "two"}) {
					t.Errorf("%s multivalue headers=%v", name, h.Values("X-Multiple"))
				}
				if !tc.keepalive && h.Get("X-Failed-Only") != "" {
					t.Errorf("%s leaked failed-only header", name)
				}
			}
			if !tc.exhausted && response.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", response.StatusCode, w.Body.String())
			}
			if tc.keepalive && strings.Count(w.Body.String(), "event: keepalive") != 1 {
				t.Fatal("keepalive changed", w.Body.String())
			}
			if !tc.stream && !tc.exhausted && response.Header.Get("Content-Type") != "application/json" {
				t.Fatal("response type changed")
			}
		})
	}
}
