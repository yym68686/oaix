package egress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const PolicyKey = "egress_observability"

type Policy struct {
	Enabled              bool      `json:"enabled"`
	TokenIDs             []int64   `json:"token_ids"`
	SuccessSamplePercent int       `json:"success_sample_percent"`
	UpdatedAt            time.Time `json:"updated_at,omitempty"`
}

func (p Policy) Validate() error {
	if p.SuccessSamplePercent < 0 || p.SuccessSamplePercent > 100 || len(p.TokenIDs) > 100 {
		return errors.New("sample percent must be 0..100 and token_ids must contain at most 100 IDs")
	}
	seen := map[int64]bool{}
	for _, id := range p.TokenIDs {
		if id <= 0 || seen[id] {
			return errors.New("token_ids must be positive and unique")
		}
		seen[id] = true
	}
	return nil
}

type RecorderStats struct {
	Started       uint64 `json:"started"`
	SampledOut    uint64 `json:"sampled_out"`
	Enqueued      uint64 `json:"enqueued"`
	Dropped       uint64 `json:"dropped"`
	Stored        uint64 `json:"stored"`
	StoreErrors   uint64 `json:"store_errors"`
	Exported      uint64 `json:"exported"`
	ExportErrors  uint64 `json:"export_errors"`
	RateDropped   uint64 `json:"rate_dropped"`
	CleanupErrors uint64 `json:"cleanup_errors"`
	LateEvents    uint64 `json:"late_events"`
	PolicyErrors  uint64 `json:"policy_errors"`
	QueueDepth    int    `json:"queue_depth"`
	QueueCapacity int    `json:"queue_capacity"`
}

type Recorder struct {
	policy        atomic.Pointer[Policy]
	policyMu      sync.Mutex
	queueMu       sync.Mutex
	rateMinute    int64
	rateCount     int
	queue         chan TraceRecord
	logger        *slog.Logger
	store         func(context.Context, TraceRecord, []byte) error
	export        func(context.Context, TraceRecord) error
	started       atomic.Uint64
	sampledOut    atomic.Uint64
	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	stored        atomic.Uint64
	storeErrors   atomic.Uint64
	exported      atomic.Uint64
	exportErrors  atomic.Uint64
	lateEvents    atomic.Uint64
	policyErrors  atomic.Uint64
	rateDropped   atomic.Uint64
	cleanupErrors atomic.Uint64
	stopped       atomic.Bool
}

// No worker, database read, or goroutine is created on a request path.
func NewRecorder(logger *slog.Logger, save func(context.Context, TraceRecord, []byte) error, export func(context.Context, TraceRecord) error) *Recorder {
	r := &Recorder{queue: make(chan TraceRecord, 256), logger: logger, store: save, export: export}
	r.policy.Store(&Policy{})
	return r
}

func (r *Recorder) SetPolicy(p Policy) error {
	if err := p.Validate(); err != nil {
		r.policyErrors.Add(1)
		return err
	}
	r.policyMu.Lock()
	defer r.policyMu.Unlock()
	if old := r.policy.Load(); old != nil && old.UpdatedAt.After(p.UpdatedAt) {
		return nil
	}
	p.TokenIDs = append([]int64(nil), p.TokenIDs...)
	r.policy.Store(&p)
	return nil
}

func (r *Recorder) Policy() Policy {
	if r == nil {
		return Policy{}
	}
	p := *r.policy.Load()
	p.TokenIDs = append([]int64(nil), p.TokenIDs...)
	return p
}

func (r *Recorder) PolicyLoadFailed() {
	if r != nil {
		r.policyErrors.Add(1)
	}
}

func (r *Recorder) Begin(ctx context.Context, headers http.Header, requestID string, index int, tokenID int64) (context.Context, *Trace) {
	if r == nil || r.stopped.Load() {
		return ctx, nil
	}
	p := r.policy.Load()
	if p == nil || !p.Enabled {
		return ctx, nil
	}
	if len(p.TokenIDs) > 0 {
		found := false
		for _, id := range p.TokenIDs {
			if id == tokenID {
				found = true
				break
			}
		}
		if !found {
			return ctx, nil
		}
	}
	traceID, parent := TraceIdentity(headers)
	if traceID == "" {
		digest := sha256.Sum256([]byte(bootID + ":" + requestID))
		traceID = hex.EncodeToString(digest[:16])
	}
	start := time.Now()
	t := &Trace{start: start, late: &r.lateEvents, successSamplePercent: p.SuccessSamplePercent, data: TraceRecord{
		SchemaVersion: 2, AttemptID: randomHex(16), RequestID: safeID(requestID), AttemptIndex: index,
		TraceID: traceID, SpanID: randomHex(8), ParentSpanID: parent, TokenID: tokenID,
		StartedAt: start.UTC(), ContentLength: -1, Route: RouteSnapshot{Kind: "unresolved"},
		Events: make([]Event, 0, 24),
	}}
	r.started.Add(1)
	return t.withHooks(ctx), t
}

func (r *Recorder) Finish(t *Trace, ctx context.Context, status int, committed, downstreamStarted, retry bool, err error) {
	if r == nil || t == nil {
		return
	}
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	d := t.data
	d.DurationUS = time.Since(t.start).Microseconds()
	d.LocalStatus, d.Committed, d.DownstreamStarted, d.Retry = status, committed, downstreamStarted, retry
	d.ErrorClass, d.ContextError = ErrorClass(err), ErrorClass(ctx.Err())
	d.Events = append([]Event(nil), d.Events...)
	t.mu.Unlock()
	if err != nil || status >= 400 {
		d.SampleReason = "failure"
	} else {
		h := fnv.New32a()
		_, _ = h.Write([]byte(d.AttemptID))
		if int(h.Sum32()%100) >= t.successSamplePercent {
			r.sampledOut.Add(1)
			return
		}
		d.SampleReason = "success_sample"
	}
	r.queueMu.Lock()
	defer r.queueMu.Unlock()
	if r.stopped.Load() {
		r.dropped.Add(1)
		return
	}
	minute := time.Now().Unix() / 60
	if minute != r.rateMinute {
		r.rateMinute = minute
		r.rateCount = 0
	}
	if r.rateCount >= 600 {
		r.rateDropped.Add(1)
		r.dropped.Add(1)
		return
	}
	r.rateCount++
	select {
	case r.queue <- d:
		r.enqueued.Add(1)
	default:
		r.dropped.Add(1)
	}
}

func (r *Recorder) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.queueMu.Lock()
			r.stopped.Store(true)
			r.dropped.Add(uint64(len(r.queue)))
			r.queueMu.Unlock()
			return
		case d := <-r.queue:
			r.deliver(d)
		}
	}
}

func (r *Recorder) deliver(d TraceRecord) {
	data, err := json.Marshal(d)
	if err != nil {
		r.dropped.Add(1)
		return
	}
	if len(data) > MaxTraceBytes {
		d.DroppedEvents += len(d.Events)
		d.Events = nil
		d.Truncated = true
		data, err = json.Marshal(d)
		if err != nil || len(data) > MaxTraceBytes {
			r.dropped.Add(1)
			return
		}
	}
	if r.store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = r.store(ctx, d, data)
		cancel()
		if err != nil {
			r.storeErrors.Add(1)
		} else {
			r.stored.Add(1)
		}
	}
	if r.export != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = r.export(ctx, d)
		cancel()
		if err != nil {
			r.exportErrors.Add(1)
		} else {
			r.exported.Add(1)
		}
	}
	if r.logger != nil {
		// This is an attempt diagnostic, not a second top-level request fact.
		r.logger.Info("egress_attempt", "event_type", "egress_attempt", "trace_id", d.TraceID, "request_id", d.RequestID, "attempt_id", d.AttemptID, "attempt_index", d.AttemptIndex, "upstream_status", d.UpstreamStatus, "local_status", d.LocalStatus, "error_class", d.ErrorClass, "body_read_error", d.BodyReadError, "proxy_channel_id", d.Route.ChannelID, "connection_id", d.ConnectionID, "duration_ms", d.DurationUS/1000)
	}
}

func (r *Recorder) Stats() RecorderStats {
	if r == nil {
		return RecorderStats{}
	}
	return RecorderStats{RateDropped: r.rateDropped.Load(), CleanupErrors: r.cleanupErrors.Load(), Started: r.started.Load(), SampledOut: r.sampledOut.Load(), Enqueued: r.enqueued.Load(), Dropped: r.dropped.Load(), Stored: r.stored.Load(), StoreErrors: r.storeErrors.Load(), Exported: r.exported.Load(), ExportErrors: r.exportErrors.Load(), LateEvents: r.lateEvents.Load(), PolicyErrors: r.policyErrors.Load(), QueueDepth: len(r.queue), QueueCapacity: cap(r.queue)}
}

func (r *Recorder) CleanupFailed() {
	if r != nil {
		r.cleanupErrors.Add(1)
	}
}
