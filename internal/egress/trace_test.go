package egress

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEgressTraceHTTP2CompressionAndTLSFailure(t *testing.T) {
	for _, mode := range []string{"http2", "gzip_truncated", "tls_failure"} {
		t.Run(mode, func(t *testing.T) {
			up := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "gzip_truncated" {
					var compressed bytes.Buffer
					g := gzip.NewWriter(&compressed)
					_, _ = g.Write([]byte("fixture-body"))
					_ = g.Close()
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write(compressed.Bytes()[:compressed.Len()-6])
					return
				}
				_, _ = io.WriteString(w, "fixture-body")
			}))
			up.EnableHTTP2 = true
			up.StartTLS()
			defer up.Close()
			base := http.DefaultTransport.(*http.Transport).Clone()
			base.ForceAttemptHTTP2 = true
			if mode != "tls_failure" {
				base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
			}
			client := &http.Client{Transport: NewTransport(base)}
			defer client.CloseIdleConnections()
			r := testRecorder(t)
			ctx, tr := r.Begin(context.Background(), nil, "protocol", 1, 1)
			req, _ := http.NewRequestWithContext(ctx, "GET", up.URL, nil)
			resp, err := client.Do(req)
			status := 502
			if err == nil {
				status = resp.StatusCode
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			r.Finish(tr, ctx, status, false, false, err != nil, err)
			d := <-r.queue
			switch mode {
			case "http2":
				if d.HTTPProtocol != "HTTP/2.0" || d.TLSProtocol != "h2" || !d.BodyEOF {
					t.Fatalf("HTTP2 facts: %+v", d)
				}
			case "gzip_truncated":
				if d.BodyReadError != "unexpected_eof" || !d.Uncompressed {
					t.Fatalf("decoded body facts: %+v", d)
				}
			case "tls_failure":
				if d.ErrorClass != "tls_certificate" || d.UpstreamStatus != 0 || d.ConnectionID == "" || d.TransportError != "tls_certificate" {
					t.Fatalf("TLS facts: %+v", d)
				}
			}
		})
	}
}

func TestEgressTraceDistinguishesResolverUDPFromTCP(t *testing.T) {
	r := testRecorder(t)
	ctx, tr := r.Begin(context.Background(), nil, "resolver", 1, 1)
	hooks := httptrace.ContextClientTrace(ctx)
	hooks.ConnectStart("udp", "1.1.1.1:53")
	hooks.ConnectDone("udp", "1.1.1.1:53", nil)
	hooks.ConnectStart("tcp4", "1.1.1.1:9999")
	hooks.ConnectDone("tcp4", "1.1.1.1:9999", nil)
	r.Finish(tr, ctx, 502, false, false, true, io.EOF)
	d := <-r.queue
	if d.SchemaVersion != 3 || d.Events[0].Phase != "udp" || d.Events[1].Phase != "udp" || d.Events[2].Phase != "tcp" || d.Events[3].Phase != "tcp" {
		t.Fatalf("wrong network metadata: %+v", d)
	}
}

func TestEgressRecorderDoesNotBlockOnExporterOrLoseBodyReadResult(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r := NewRecorder(nil, func(context.Context, TraceRecord, []byte) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	}, nil)
	_ = r.SetPolicy(Policy{Enabled: true, SuccessSamplePercent: 100})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	request, tr := r.Begin(context.Background(), nil, "blocked-exporter", 1, 1)
	r.Finish(tr, request, 502, false, false, true, io.ErrUnexpectedEOF)
	<-started
	completed := make(chan struct{})
	go func() {
		request, tr := r.Begin(context.Background(), nil, "next", 2, 1)
		r.Finish(tr, request, 200, true, false, false, nil)
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("request waited for diagnostic I/O")
	}
	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func testRecorder(t *testing.T) *Recorder {
	t.Helper()
	r := NewRecorder(nil, nil, nil)
	if err := r.SetPolicy(Policy{Enabled: true, SuccessSamplePercent: 100}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEgressTraceProxyFailureStagesAndRedaction(t *testing.T) {
	for _, mode := range []string{"connect_407", "body_truncated", "success"} {
		t.Run(mode, func(t *testing.T) {
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Proxy-Authorization") != "" {
					t.Error("credentials leaked to origin")
				}
				w.Header().Set("X-Oai-Request-ID", "upstream-safe-id")
				w.Header().Set("X-Codex-Turn-State", "secret-turn-state")
				if mode == "body_truncated" {
					w.Header().Set("Content-Length", "1000")
				}
				_, _ = io.WriteString(w, "body-data")
			}))
			defer upstream.Close()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "connect_407" {
					w.WriteHeader(407)
					return
				}
				dest, err := net.Dial("tcp", strings.TrimPrefix(upstream.URL, "https://"))
				if err != nil {
					t.Error(err)
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					dest.Close()
					t.Error(err)
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
				go func() { defer conn.Close(); defer dest.Close(); _, _ = io.Copy(dest, conn) }()
				go func() { defer conn.Close(); defer dest.Close(); _, _ = io.Copy(conn, dest) }()
			}))
			defer proxy.Close()
			base := http.DefaultTransport.(*http.Transport).Clone()
			base.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // Local fixture certificate only.
			base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(proxy.URL, "http://"))
			}
			var connectCallbacks atomic.Int32
			base.OnProxyConnectResponse = func(context.Context, *url.URL, *http.Request, *http.Response) error {
				connectCallbacks.Add(1)
				return nil
			}
			client := &http.Client{Transport: NewTransport(base)}
			defer client.CloseIdleConnections()
			recorder := testRecorder(t)
			ctx, trace := recorder.Begin(context.Background(), http.Header{"X-Fugue-Trace-Id": []string{strings.Repeat("a", 32)}}, "req-safe", 1, 42)
			proxyURL, _ := Parse("8.8.8.8:8080:secret-user:secret-password")
			req, _ := http.NewRequestWithContext(WithProxy(ctx, proxyURL), "GET", "https://upstream.example.com/", nil)
			resp, err := client.Do(req)
			status := 502
			if err == nil {
				status = resp.StatusCode
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			recorder.Finish(trace, ctx, status, false, false, err != nil, err)
			d := <-recorder.queue
			if connectCallbacks.Load() != 1 {
				t.Fatal("existing CONNECT callback changed")
			}
			wantConnect := 200
			if mode == "connect_407" {
				wantConnect = 407
			}
			found := false
			for _, ev := range d.Events {
				if ev.Phase == "proxy_connect" && ev.Status == wantConnect {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing CONNECT result: %+v", d.Events)
			}
			if mode == "body_truncated" && (d.UpstreamStatus != 200 || d.BodyReadError != "unexpected_eof" || d.BodyBytes != 9 || !d.BodyCloseCalled) {
				t.Fatalf("wrong body observation: %+v", d)
			}
			if mode == "success" && (!d.BodyEOF || d.ConnectionID == "" || d.UpstreamRequestID != "upstream-safe-id") {
				t.Fatalf("missing success observation: %+v", d)
			}
			raw, _ := json.Marshal(d)
			for _, secret := range []string{"secret-user", "secret-password", "secret-turn-state", "body-data"} {
				if strings.Contains(string(raw), secret) {
					t.Fatalf("trace leaked %s", secret)
				}
			}
		})
	}
}

func TestEgressTraceReuseAndObserverOff(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer up.Close()
	client := &http.Client{Transport: NewTransport(http.DefaultTransport.(*http.Transport))}
	defer client.CloseIdleConnections()
	r := testRecorder(t)
	var first string
	for i := 0; i < 2; i++ {
		ctx, tr := r.Begin(context.Background(), nil, "reuse", i+1, 1)
		req, _ := http.NewRequestWithContext(ctx, "GET", up.URL, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		r.Finish(tr, ctx, 200, true, false, false, nil)
		d := <-r.queue
		if d.BodyReadError != "" || !d.BodyEOF {
			t.Fatalf("normal EOF reported as failure: %+v", d)
		}
		for _, event := range d.Events {
			if event.Phase == "body_read" && event.Error != "" {
				t.Fatal("normal EOF makes an error span")
			}
		}
		if i == 0 {
			first = d.ConnectionID
			if first == "" {
				t.Fatal("no connection id")
			}
		} else if !d.Reused || d.ConnectionID != first {
			t.Fatalf("reuse changed connection identity: %+v", d)
		}
	}
	_ = r.SetPolicy(Policy{})
	ctx, tr := r.Begin(context.Background(), nil, "off", 1, 1)
	if tr != nil || TraceFrom(ctx) != nil {
		t.Fatal("disabled observation still captures")
	}
}

func TestEgressRecorderBoundsLateCallbacksAndErrors(t *testing.T) {
	r := testRecorder(t)
	ctx, tr := r.Begin(context.Background(), nil, "bounds", 1, 1)
	for i := 0; i < 200; i++ {
		tr.Event("tcp", "done", "8.8.8.8:443", errors.New("secret-password"), 0)
	}
	ObserveSSE(ctx, "secret-person@example.com", nil)
	r.Finish(tr, ctx, 502, false, false, true, fmt.Errorf("wrapped: %w", io.ErrUnexpectedEOF))
	d := <-r.queue
	if len(d.Events) != MaxTraceEvents || d.DroppedEvents != 200-MaxTraceEvents || !d.Truncated || d.ErrorClass != "unexpected_eof" || d.LastEventType != "other" {
		t.Fatalf("bounds failed: %+v", d)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); tr.Event("late", "done", "", nil, 0) }()
	}
	wg.Wait()
	if r.Stats().LateEvents != 20 {
		t.Fatal(r.Stats())
	}
	for i := 0; i < 300; i++ {
		c, tr := r.Begin(context.Background(), nil, "queue", i, 1)
		r.Finish(tr, c, 502, false, false, true, io.ErrUnexpectedEOF)
	}
	if r.Stats().QueueDepth != 256 || r.Stats().Dropped != 44 {
		t.Fatal(r.Stats())
	}
	data, _ := json.Marshal(d)
	if strings.Contains(string(data), "secret-") {
		t.Fatal("error or event text leaked")
	}
}

func TestEgressPolicyAndRateBudget(t *testing.T) {
	r := testRecorder(t)
	now := time.Now()
	_ = r.SetPolicy(Policy{Enabled: true, TokenIDs: []int64{3}, SuccessSamplePercent: 100, UpdatedAt: now})
	_ = r.SetPolicy(Policy{UpdatedAt: now.Add(-time.Second)})
	if !r.Policy().Enabled {
		t.Fatal("stale reload overwrote policy")
	}
	if _, tr := r.Begin(context.Background(), nil, "excluded", 1, 4); tr != nil {
		t.Fatal("token filter ignored")
	}
	if err := r.SetPolicy(Policy{SuccessSamplePercent: 101}); err == nil {
		t.Fatal("invalid policy accepted")
	}
	for i := 0; i < 610; i++ {
		ctx, tr := r.Begin(context.Background(), nil, "rate", i, 3)
		r.Finish(tr, ctx, 500, false, false, true, io.ErrUnexpectedEOF)
		select {
		case <-r.queue:
		default:
		}
	}
	if r.Stats().RateDropped != 10 {
		t.Fatal(r.Stats())
	}
}

func TestEgressExporterPayloadAndFailureIsolation(t *testing.T) {
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected export request")
		}
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(200)
	}))
	defer server.Close()
	e := NewOTLPExporter(server.URL, map[string]string{"app_id": "app-test"})
	defer e.Close()
	r := testRecorder(t)
	ctx, tr := r.Begin(context.Background(), http.Header{"Traceparent": []string{"00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01"}}, "export", 1, 1)
	tr.Event("tls", "start", "", nil, 0)
	tr.Event("tls", "done", "", nil, 0)
	tr.Event("udp", "start", "1.1.1.1:53", nil, 0)
	tr.Event("udp", "start", "1.1.1.1:53", nil, 0)
	tr.Event("udp", "done", "1.1.1.1:53", nil, 0)
	tr.Event("udp", "done", "1.1.1.1:53", nil, 0)
	r.Finish(tr, ctx, 200, true, false, false, nil)
	d := <-r.queue
	if err := e.Export(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	spans := received["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
	ambiguous := 0
	for _, raw := range spans {
		span := raw.(map[string]any)
		if span["name"] != "udp" {
			continue
		}
		ambiguous++
		if span["startTimeUnixNano"] != span["endTimeUnixNano"] {
			t.Fatal("invented a duration for overlapping DNS dials")
		}
		for _, rawAttr := range span["attributes"].([]any) {
			attr := rawAttr.(map[string]any)
			if attr["key"] == "paired_start" && attr["value"].(map[string]any)["stringValue"] != "false" {
				t.Fatal("ambiguous dial pairing claimed exact")
			}
		}
	}
	if ambiguous != 2 {
		t.Fatal("missing parallel dial observations")
	}
	if received["resourceSpans"] == nil || d.ParentSpanID != strings.Repeat("b", 16) {
		t.Fatal("OTLP identity missing")
	}
	r = NewRecorder(nil, func(context.Context, TraceRecord, []byte) error { return errors.New("secret-store-error") }, func(context.Context, TraceRecord) error { return errors.New("secret-export-error") })
	r.deliver(d)
	if r.Stats().StoreErrors != 1 || r.Stats().ExportErrors != 1 {
		t.Fatal(r.Stats())
	}
}

func BenchmarkEgressObservation(b *testing.B) {
	r := NewRecorder(nil, nil, nil)
	_ = r.SetPolicy(Policy{Enabled: true})
	for i := 0; i < b.N; i++ {
		ctx, tr := r.Begin(context.Background(), nil, "benchmark", 1, 1)
		for j := 0; j < 20; j++ {
			tr.Event("tcp", "done", "1.1.1.1:443", nil, 0)
		}
		r.Finish(tr, ctx, 200, true, false, false, nil)
	}
}
