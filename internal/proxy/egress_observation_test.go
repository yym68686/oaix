package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
)

func TestEgressObservationPreservesRetryBodyAndHeaders(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			w.Header().Set("X-Oai-Request-ID", "failed")
			w.Header().Set("Content-Length", "1000")
			_, _ = io.WriteString(w, "data: ")
			return
		}
		w.Header().Set("X-Oai-Request-ID", "winner")
		_, _ = w.Write(sse.Encode("response.completed", []byte(`{"type":"response.completed","sequence_number":5,"response":{"id":"winner","status":"completed","output":[]}}`)))
	}))
	defer up.Close()
	now := time.Now().UTC()
	fakes := &fakeProxyStore{tokens: []store.Token{{ID: 1, AccessToken: "first", IsActive: true, CreatedAt: now, UpdatedAt: now}, {ID: 2, AccessToken: "second", IsActive: true, CreatedAt: now, UpdatedAt: now}}}
	p := newProxyPipelineTestHarness(t, up.URL, 2, fakes)
	records := make(chan egress.TraceRecord, 2)
	recorder := egress.NewRecorder(nil, func(ctx context.Context, d egress.TraceRecord, _ []byte) error { records <- d; return nil }, nil)
	if err := recorder.SetPolicy(egress.Policy{Enabled: true, SuccessSamplePercent: 100}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go recorder.Run(ctx)
	p.SetEgressRecorder(recorder)
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-5.5","input":"hello"}`))
	w := httptest.NewRecorder()
	p.Proxy(w, req, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"})
	if calls.Load() != 2 || w.Code != 200 || len(w.Result().Header.Values("X-Oai-Request-ID")) != 1 || w.Header().Get("X-Oai-Request-ID") != "winner" {
		t.Fatalf("observation changed delivery: %v %s", w.Result().Header, w.Body.String())
	}
	var data []egress.TraceRecord
	for len(data) < 2 {
		select {
		case d := <-records:
			data = append(data, d)
		case <-time.After(3 * time.Second):
			t.Fatal("missing observation")
		}
	}
	if data[0].UpstreamStatus != 200 || data[0].LocalStatus != 502 || data[0].BodyReadError != "unexpected_eof" || !data[0].Retry || !data[0].BodyCloseCalled {
		t.Fatalf("first attempt facts wrong: %+v", data[0])
	}
	if data[1].CompletedEvents != 1 || data[1].LastSequence == nil || *data[1].LastSequence != 5 || data[1].LocalStatus != 200 || !data[1].Committed {
		t.Fatalf("terminal facts missing: %+v", data[1])
	}
	if data[0].TraceID != data[1].TraceID || data[0].AttemptID == data[1].AttemptID {
		t.Fatal("retry correlation missing")
	}
}
