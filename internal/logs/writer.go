package logs

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

type Sink interface {
	UpsertRequestLogs(ctx context.Context, logs []store.RequestLog) error
	EnqueueRequestLogOutbox(ctx context.Context, item store.RequestLog) error
}

type Writer struct {
	sink     Sink
	logger   *slog.Logger
	cfg      config.RequestLogConfig
	queue    chan store.RequestLog
	stopCh   chan struct{}
	wg       sync.WaitGroup
	dropped  atomic.Int64
	enqueued atomic.Int64
	written  atomic.Int64
	outbox   atomic.Int64
}

type Stats struct {
	Queued       int   `json:"queued"`
	QueueMax     int   `json:"queue_max"`
	Enqueued     int64 `json:"enqueued"`
	Written      int64 `json:"written"`
	Dropped      int64 `json:"dropped"`
	OutboxWrites int64 `json:"outbox_writes"`
}

func NewWriter(sink Sink, logger *slog.Logger, cfg config.RequestLogConfig) *Writer {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.QueueMaxSize <= 0 {
		cfg.QueueMaxSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	return &Writer{
		sink:   sink,
		logger: logger,
		cfg:    cfg,
		queue:  make(chan store.RequestLog, cfg.QueueMaxSize),
		stopCh: make(chan struct{}),
	}
}

func (w *Writer) Start(ctx context.Context) {
	for i := 0; i < w.cfg.Concurrency; i++ {
		w.wg.Add(1)
		go w.worker(ctx)
	}
}

func (w *Writer) Stop(ctx context.Context) error {
	close(w.stopCh)
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (w *Writer) Submit(ctx context.Context, item store.RequestLog, final bool) {
	if item.StartedAt.IsZero() {
		item.StartedAt = time.Now().UTC()
	}
	// Request handlers keep enriching these maps after the initial log is
	// submitted. Snapshot them before handing the item to an async worker so
	// queued writes never race with the live request.
	item.TimingSpans = cloneAnyMap(item.TimingSpans)
	item.PromptCacheTrace = cloneAnyMap(item.PromptCacheTrace)
	select {
	case w.queue <- item:
		w.enqueued.Add(1)
		return
	default:
	}
	if final && w.cfg.FinalSyncOnFull {
		if err := w.upsertWithRetry(ctx, []store.RequestLog{item}); err == nil {
			w.written.Add(1)
			return
		} else if outboxErr := w.sink.EnqueueRequestLogOutbox(ctx, item); outboxErr == nil {
			w.outbox.Add(1)
			return
		} else if w.logger != nil {
			w.logger.Error("final request log sync and outbox fallback failed", "error", err, "outbox_error", outboxErr)
		}
	}
	w.dropped.Add(1)
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (w *Writer) Stats() Stats {
	return Stats{
		Queued:       len(w.queue),
		QueueMax:     cap(w.queue),
		Enqueued:     w.enqueued.Load(),
		Written:      w.written.Load(),
		Dropped:      w.dropped.Load(),
		OutboxWrites: w.outbox.Load(),
	}
}

func (w *Writer) worker(ctx context.Context) {
	defer w.wg.Done()
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()
	batch := make([]store.RequestLog, 0, w.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := w.upsertWithRetry(writeCtx, batch)
		cancel()
		if err != nil {
			if w.logger != nil {
				w.logger.Warn("request log batch write failed; falling back to outbox", "error", err, "batch_size", len(batch))
			}
			for _, item := range batch {
				outboxCtx, outboxCancel := context.WithTimeout(context.Background(), 3*time.Second)
				if outboxErr := w.sink.EnqueueRequestLogOutbox(outboxCtx, item); outboxErr == nil {
					w.outbox.Add(1)
				} else {
					w.dropped.Add(1)
					if w.logger != nil {
						w.logger.Error("request log outbox fallback failed", "error", outboxErr)
					}
				}
				outboxCancel()
			}
		} else {
			w.written.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-w.stopCh:
			flush()
			return
		case item := <-w.queue:
			batch = append(batch, item)
			if len(batch) >= w.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (w *Writer) upsertWithRetry(ctx context.Context, batch []store.RequestLog) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := w.sink.UpsertRequestLogs(ctx, batch); err != nil {
			lastErr = err
			timer := time.NewTimer(time.Duration(attempt+1) * 50 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		return nil
	}
	return lastErr
}
