package admindiag

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/observability"
)

func enabled(t *testing.T) *Recorder {
	t.Helper()
	r := New("fixture")
	if err := r.SetPolicy(Policy{Enabled: true, MaxRecordsPerMinute: 60, SuccessSamplePercent: 100}); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestDisabledBudgetAndToggle(t *testing.T) {
	r := New("test")
	if _, d := r.Begin(context.Background(), "route", "", "", ""); d != nil {
		t.Fatal("default enabled")
	}
	r = enabled(t)
	for i := 0; i < MaxActive; i++ {
		r.Begin(context.Background(), "route", "", "", "")
	}
	if _, d := r.Begin(context.Background(), "route", "", "", ""); d != nil {
		t.Fatal("active cap")
	}
	if r.Stats().Dropped != 1 {
		t.Fatal(r.Stats())
	}
	r.SetPolicy(Policy{UpdatedAt: time.Now()})
	if len(r.Targets()) != 0 {
		t.Fatal("disabled sampling")
	}
	r.SetPolicy(Policy{Enabled: true, MaxRecordsPerMinute: 60, UpdatedAt: time.Now().Add(-time.Hour)})
	if r.Policy().Enabled {
		t.Fatal("stale reload overrides disable")
	}
}
func TestCancellationAndIndependentSpans(t *testing.T) {
	r := enabled(t)
	ctx, d := r.Begin(observability.ContextWithRequestID(context.Background(), "req-1"), "route", "page-1", "", "")
	ctx, stop := context.WithCancel(ctx)
	sc, done := Stage(ctx, "pending")
	h := BeginEvent(sc, "query", "fingerprint", 42, "conn", "DO NOT EXPORT SQL OR ARGUMENTS")
	r.Track(sc, h)
	h.start = time.Now().Add(-time.Second)
	targets := r.Targets()
	if len(targets) != 1 {
		t.Fatal("missing active query")
	}
	targets[0].Add(Sample{State: "active", WaitType: "Lock", Wait: "relation"}, true, false)
	stop()
	h.End(sc, ctx.Err(), 0)
	r.Untrack(h)
	done(ctx.Err())
	r.Finish(d, 503)
	q := <-r.queue
	raw, _ := json.Marshal(q.d)
	if strings.Contains(string(raw), "DO NOT EXPORT") || !strings.Contains(string(raw), "canceled") {
		t.Fatal(string(raw))
	}
	if len(q.d.Samples) != 1 || q.d.PageLoadID != "page-1" || q.d.RequestID != "req-1" {
		t.Fatal(q.d)
	}
	targets[0].Add(Sample{}, true, false)
	if len(q.d.Samples) != 1 {
		t.Fatal("late callback changed record")
	}
}
func TestEventsSamplesAndExportFailureBounded(t *testing.T) {
	r := enabled(t)
	ctx, d := r.Begin(context.Background(), "route", "", "", "")
	for i := 0; i < MaxEvents+10; i++ {
		BeginEvent(ctx, "stage", "", 0, "", "")
	}
	r.Finish(d, 200)
	c, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(c, func(context.Context, Record, []byte) error { return errors.New("store down") }, func(context.Context, Record, []byte) error { return errors.New("export down") })
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for r.Stats().ExportErrors == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if r.Stats().StoreErrors != 1 || r.Stats().ExportErrors != 1 {
		t.Fatal(r.Stats())
	}
}
func BenchmarkObservationDisabled(b *testing.B) {
	r := New("fixture")
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.Begin(ctx, "route", "", "", "")
	}
}
func BenchmarkObservationEnabled(b *testing.B) {
	r := New("fixture")
	r.SetPolicy(Policy{Enabled: true, MaxRecordsPerMinute: 60, SuccessSamplePercent: 100})
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.admitted = 0
		c, t := r.Begin(ctx, "route", "", "", "")
		c, end := Stage(c, "pending")
		h := BeginEvent(c, "query", "fingerprint", 123, "connection", "select 1")
		h.End(c, nil, 0)
		end(nil)
		r.Finish(t, 200)
		<-r.queue
	}
}
