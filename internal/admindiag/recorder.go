// Package admindiag records bounded administrator diagnostics. It never owns
// serving configuration, changes a deadline, or waits for export on a request.
package admindiag

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/observability"
)

var BuildRevision = "unknown"
var buildIdentityOnce sync.Once
var buildIdentityValue string

func buildIdentity() string {
	buildIdentityOnce.Do(func() {
		buildIdentityValue = BuildRevision
		if BuildRevision != "unknown" {
			return
		}
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, s := range info.Settings {
				if s.Key == "vcs.revision" {
					buildIdentityValue = s.Value
					return
				}
			}
		}
		// Docker source archives have no .git. An immutable binary digest identifies
		// the actual executor without inventing a source commit or changing build.
		name, err := os.Executable()
		if err != nil {
			return
		}
		f, err := os.Open(name)
		if err != nil {
			return
		}
		defer f.Close()
		h := sha256.New()
		if _, err = io.Copy(h, f); err == nil {
			buildIdentityValue = fmt.Sprintf("binary_sha256:%x", h.Sum(nil))
		}
	})
	return buildIdentityValue
}

func NewConnectionID() string { return observability.NewRequestID() }

const PolicyKey = "admin_query_observability"
const MaxBytes = 32768
const MaxEvents = 64
const MaxSamples = 120
const MaxActive = 16
const MaxTargets = 8

type Policy struct {
	Enabled              bool      `json:"enabled"`
	SuccessSamplePercent int       `json:"success_sample_percent"`
	MaxRecordsPerMinute  int       `json:"max_records_per_minute"`
	UpdatedAt            time.Time `json:"updated_at,omitempty"`
}

func (p Policy) Validate() error {
	if p.SuccessSamplePercent < 0 || p.SuccessSamplePercent > 100 || p.MaxRecordsPerMinute < 0 || p.MaxRecordsPerMinute > 60 {
		return errors.New("sample must be 0..100 and record budget 0..60")
	}
	if p.Enabled && p.MaxRecordsPerMinute == 0 {
		return errors.New("enabled policy requires a record budget")
	}
	return nil
}

type Event struct {
	ID                  string `json:"id"`
	Kind                string `json:"kind"`
	Stage               string `json:"stage"`
	StartUS             int64  `json:"start_us"`
	EndUS               *int64 `json:"end_us,omitempty"`
	DeadlineRemainingUS *int64 `json:"deadline_remaining_us,omitempty"`
	Error               string `json:"error,omitempty"`
	ContextError        string `json:"context_error,omitempty"`
	Fingerprint         string `json:"fingerprint,omitempty"`
	PID                 uint32 `json:"pid,omitempty"`
	Connection          string `json:"connection,omitempty"`
	Rows                int64  `json:"rows,omitempty"`
	// FirstRow is deliberately absent: pgx QueryTracer reports query start/end,
	// not first-row delivery. Do not label EndUS as first byte or server CPU.
}
type Sample struct {
	QueryID      string    `json:"query_id"`
	AtUS         int64     `json:"at_us"`
	BackendStart time.Time `json:"backend_start"`
	QueryStart   time.Time `json:"query_start"`
	State        string    `json:"state"`
	WaitType     string    `json:"wait_type,omitempty"`
	Wait         string    `json:"wait,omitempty"`
	PGQueryID    string    `json:"pg_query_id,omitempty"`
	Blockers     []int32   `json:"blockers,omitempty"`
}
type Record struct {
	SchemaVersion   int       `json:"schema_version"`
	ID              string    `json:"id"`
	RequestID       string    `json:"request_id"`
	PageLoadID      string    `json:"page_load_id,omitempty"`
	TraceID         string    `json:"trace_id"`
	ParentSpanID    string    `json:"parent_span_id,omitempty"`
	Instance        string    `json:"instance"`
	Revision        string    `json:"revision"`
	Route           string    `json:"route"`
	StartedAt       time.Time `json:"started_at"`
	DurationUS      int64     `json:"duration_us"`
	Status          int       `json:"status"`
	Reason          string    `json:"reason"`
	Events          []Event   `json:"events"`
	Samples         []Sample  `json:"samples"`
	DroppedEvents   int       `json:"dropped_events"`
	DroppedSamples  int       `json:"dropped_samples"`
	SampleErrors    int       `json:"sample_errors"`
	SampleMisses    int       `json:"sample_misses"`
	TargetLimitHits int       `json:"target_limit_hits"`
	Truncated       bool      `json:"truncated"`
}
type contextKey struct{}
type stageKey struct{}
type Trace struct {
	mu            sync.Mutex
	start         time.Time
	data          Record
	done          bool
	samplePercent int
}
type Handle struct {
	t     *Trace
	index int
	start time.Time
	sql   string
}

func from(ctx context.Context) *Trace { t, _ := ctx.Value(contextKey{}).(*Trace); return t }
func Stage(ctx context.Context, name string) (context.Context, func(error)) {
	if from(ctx) == nil {
		return ctx, func(error) {}
	}
	ctx = context.WithValue(ctx, stageKey{}, name)
	h := BeginEvent(ctx, "stage", "", 0, "", "")
	return ctx, func(err error) { h.End(ctx, err, 0) }
}
func BeginEvent(ctx context.Context, kind, fingerprint string, pid uint32, connection, sql string) *Handle {
	t := from(ctx)
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return nil
	}
	if len(t.data.Events) >= MaxEvents {
		t.data.DroppedEvents++
		t.data.Truncated = true
		return nil
	}
	name, _ := ctx.Value(stageKey{}).(string)
	if name == "" {
		name = "request"
	}
	e := Event{ID: fmt.Sprintf("%d", len(t.data.Events)+1), Kind: kind, Stage: name, StartUS: time.Since(t.start).Microseconds(), Fingerprint: fingerprint, PID: pid, Connection: connection}
	if deadline, ok := ctx.Deadline(); ok {
		n := time.Until(deadline).Microseconds()
		e.DeadlineRemainingUS = &n
	}
	t.data.Events = append(t.data.Events, e)
	return &Handle{t: t, index: len(t.data.Events) - 1, start: time.Now(), sql: sql}
}
func ErrorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var pg interface{ SQLState() string }
	if errors.As(err, &pg) {
		return "sqlstate_" + pg.SQLState()
	}
	return "other"
}
func (h *Handle) End(ctx context.Context, err error, rows int64) {
	if h == nil {
		return
	}
	h.t.mu.Lock()
	defer h.t.mu.Unlock()
	if h.t.done {
		return
	}
	e := &h.t.data.Events[h.index]
	if e.EndUS != nil {
		return
	}
	n := time.Since(h.t.start).Microseconds()
	e.EndUS = &n
	e.Error = ErrorClass(err)
	e.ContextError = ErrorClass(ctx.Err())
	e.Rows = rows
}

type Target struct {
	PID    uint32
	SQL    string
	handle *Handle
}

func (t Target) Add(s Sample, matched bool, failed bool) {
	h := t.handle
	h.t.mu.Lock()
	defer h.t.mu.Unlock()
	if h.t.done || h.t.data.Events[h.index].EndUS != nil {
		return
	}
	if failed {
		h.t.data.SampleErrors++
		return
	}
	if !matched {
		h.t.data.SampleMisses++
		return
	}
	if len(h.t.data.Samples) >= MaxSamples {
		h.t.data.DroppedSamples++
		return
	}
	s.QueryID = h.t.data.Events[h.index].ID
	s.AtUS = time.Since(h.t.start).Microseconds()
	h.t.data.Samples = append(h.t.data.Samples, s)
}

type Stats struct {
	Started        uint64 `json:"started"`
	Active         int    `json:"active"`
	Enqueued       uint64 `json:"enqueued"`
	Dropped        uint64 `json:"dropped"`
	SampledOut     uint64 `json:"sampled_out"`
	Stored         uint64 `json:"stored"`
	StoreErrors    uint64 `json:"store_errors"`
	Exported       uint64 `json:"exported"`
	ExportErrors   uint64 `json:"export_errors"`
	PolicyErrors   uint64 `json:"policy_errors"`
	CleanupErrors  uint64 `json:"cleanup_errors"`
	QueueDepth     int    `json:"queue_depth"`
	OldestQueuedMS int64  `json:"oldest_queued_ms"`
	LastStoredAt   string `json:"last_stored_at"`
}
type Delivery func(context.Context, Record, []byte) error
type queued struct {
	d  Record
	at time.Time
}
type Recorder struct {
	mu                 sync.Mutex
	policy             Policy
	active             map[*Trace]map[*Handle]bool
	queue              chan queued
	stopped            bool
	minute             int64
	admitted           int
	instance, revision string
	started            atomic.Uint64
	enqueued           atomic.Uint64
	dropped            atomic.Uint64
	sampledOut         atomic.Uint64
	stored             atomic.Uint64
	storeErrors        atomic.Uint64
	exported           atomic.Uint64
	exportErrors       atomic.Uint64
	policyErrors       atomic.Uint64
	cleanupErrors      atomic.Uint64
	oldest             atomic.Int64
	lastStored         atomic.Int64
}

func New(revision string) *Recorder {
	if revision == "unknown" {
		revision = buildIdentity()
	}
	return &Recorder{active: make(map[*Trace]map[*Handle]bool), queue: make(chan queued, 256), instance: observability.NewRequestID(), revision: revision}
}
func (r *Recorder) Policy() Policy {
	if r == nil {
		return Policy{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.policy
}
func (r *Recorder) SetPolicy(p Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.policy.UpdatedAt.After(p.UpdatedAt) {
		r.policy = p
	}
	return nil
}
func (r *Recorder) PolicyFailed()  { r.policyErrors.Add(1) }
func (r *Recorder) CleanupFailed() { r.cleanupErrors.Add(1) }
func (r *Recorder) Begin(ctx context.Context, route, pageID, traceID, parent string) (context.Context, *Trace) {
	if r == nil {
		return ctx, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || !r.policy.Enabled {
		return ctx, nil
	}
	minute := time.Now().Unix() / 60
	if r.minute != minute {
		r.minute = minute
		r.admitted = 0
	}
	// Bound before allocation/sampling, not only at export. A flood cannot grow
	// concurrent records or make all requests perform diagnostic work.
	if len(r.active) >= MaxActive || r.admitted >= r.policy.MaxRecordsPerMinute {
		r.dropped.Add(1)
		return ctx, nil
	}
	r.admitted++
	id := observability.NewRequestID()
	if traceID == "" {
		traceID = id
	}
	start := time.Now()
	t := &Trace{start: start, samplePercent: r.policy.SuccessSamplePercent, data: Record{SchemaVersion: 1, ID: id, RequestID: observability.RequestIDFromContext(ctx), PageLoadID: observability.NormalizeRequestID(pageID), TraceID: traceID, ParentSpanID: parent, Instance: r.instance, Revision: r.revision, Route: route, StartedAt: start.UTC(), Events: []Event{}, Samples: []Sample{}}}
	r.active[t] = make(map[*Handle]bool)
	r.started.Add(1)
	return context.WithValue(ctx, contextKey{}, t), t
}
func (r *Recorder) Track(ctx context.Context, h *Handle) {
	if r == nil || h == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if qs, ok := r.active[from(ctx)]; ok {
		qs[h] = true
	}
}
func (r *Recorder) Untrack(h *Handle) {
	if r == nil || h == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active[h.t], h)
}
func (r *Recorder) Targets() []Target {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.policy.Enabled || r.stopped {
		return nil
	}
	out := []Target{}
	for t, hs := range r.active {
		t.mu.Lock()
		for h := range hs {
			e := t.data.Events[h.index]
			if len(t.data.Samples) >= MaxSamples {
				t.data.Truncated = true
				continue
			}
			if e.EndUS != nil || time.Since(h.start) < 500*time.Millisecond {
				continue
			}
			if len(out) >= MaxTargets {
				t.data.TargetLimitHits++
				continue
			}
			out = append(out, Target{PID: e.PID, SQL: h.sql, handle: h})
		}
		t.mu.Unlock()
	}
	return out
}
func (r *Recorder) Finish(t *Trace, status int) {
	if r == nil || t == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return
	}
	t.done = true
	delete(r.active, t)
	d := t.data
	d.Status = status
	d.DurationUS = time.Since(t.start).Microseconds()
	if status >= 400 {
		d.Reason = "failure"
	} else if d.DurationUS >= 1_000_000 {
		d.Reason = "slow"
	} else {
		h := fnv.New32a()
		_, _ = h.Write([]byte(d.ID))
		if int(h.Sum32()%100) >= t.samplePercent {
			r.sampledOut.Add(1)
			return
		}
		d.Reason = "success_sample"
	}
	if r.stopped {
		r.dropped.Add(1)
		return
	}
	q := queued{d: d, at: time.Now()}
	select {
	case r.queue <- q:
		r.enqueued.Add(1)
		r.oldest.CompareAndSwap(0, q.at.UnixMilli())
	default:
		r.dropped.Add(1)
	}
}
func (r *Recorder) Run(ctx context.Context, save, export Delivery) {
	defer func() { r.mu.Lock(); r.stopped = true; r.dropped.Add(uint64(len(r.queue))); r.mu.Unlock() }()
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-r.queue:
			r.oldest.Store(q.at.UnixMilli())
			d := q.d
			raw, err := json.Marshal(d)
			if err == nil && len(raw) > MaxBytes {
				d.DroppedSamples += len(d.Samples)
				d.Samples = nil
				d.Truncated = true
				raw, err = json.Marshal(d)
			}
			if err != nil || len(raw) > MaxBytes {
				r.dropped.Add(1)
				continue
			}
			for i, fn := range []Delivery{save, export} {
				if fn == nil {
					continue
				}
				c, cancel := context.WithTimeout(ctx, time.Second)
				err = fn(c, d, raw)
				cancel()
				if i == 0 {
					if err != nil {
						r.storeErrors.Add(1)
					} else {
						r.stored.Add(1)
						r.lastStored.Store(time.Now().UnixMilli())
					}
				} else {
					if err != nil {
						r.exportErrors.Add(1)
					} else {
						r.exported.Add(1)
					}
				}
			}
			if len(r.queue) == 0 {
				r.oldest.Store(0)
			}
		}
	}
}
func (r *Recorder) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	r.mu.Lock()
	active := len(r.active)
	r.mu.Unlock()
	age := int64(0)
	if n := r.oldest.Load(); n > 0 {
		age = time.Now().UnixMilli() - n
	}
	last := ""
	if n := r.lastStored.Load(); n > 0 {
		last = time.UnixMilli(n).UTC().Format(time.RFC3339Nano)
	}
	return Stats{Started: r.started.Load(), Active: active, Enqueued: r.enqueued.Load(), Dropped: r.dropped.Load(), SampledOut: r.sampledOut.Load(), Stored: r.stored.Load(), StoreErrors: r.storeErrors.Load(), Exported: r.exported.Load(), ExportErrors: r.exportErrors.Load(), PolicyErrors: r.policyErrors.Load(), CleanupErrors: r.cleanupErrors.Load(), QueueDepth: len(r.queue), OldestQueuedMS: age, LastStoredAt: last}
}
