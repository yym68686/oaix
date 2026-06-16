package logs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

type fakeSink struct {
	upserts   atomic.Int64
	outbox    atomic.Int64
	upsertErr error
	outboxErr error
}

func (f *fakeSink) UpsertRequestLogs(context.Context, []store.RequestLog) error {
	f.upserts.Add(1)
	return f.upsertErr
}

func (f *fakeSink) EnqueueRequestLogOutbox(context.Context, store.RequestLog) error {
	f.outbox.Add(1)
	return f.outboxErr
}

func TestFinalLogSyncsWhenQueueFull(t *testing.T) {
	sink := &fakeSink{}
	writer := NewWriter(sink, nil, config.RequestLogConfig{
		Concurrency:     1,
		BatchSize:       10,
		QueueMaxSize:    1,
		FlushInterval:   time.Hour,
		FinalSyncOnFull: true,
	})
	ctx := context.Background()
	writer.Submit(ctx, store.RequestLog{RequestID: "start", Endpoint: "/v1/responses", StartedAt: time.Now()}, false)
	writer.Submit(ctx, store.RequestLog{RequestID: "final", Endpoint: "/v1/responses", StartedAt: time.Now()}, true)
	if sink.upserts.Load() == 0 {
		t.Fatal("expected final log sync write when queue is full")
	}
	if writer.Stats().Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", writer.Stats().Dropped)
	}
}

func TestFinalLogFallsBackToOutboxWhenSyncFails(t *testing.T) {
	sink := &fakeSink{upsertErr: errors.New("db unavailable")}
	writer := NewWriter(sink, nil, config.RequestLogConfig{
		Concurrency:     1,
		BatchSize:       10,
		QueueMaxSize:    1,
		FlushInterval:   time.Hour,
		FinalSyncOnFull: true,
	})
	ctx := context.Background()
	writer.Submit(ctx, store.RequestLog{RequestID: "start", Endpoint: "/v1/responses", StartedAt: time.Now()}, false)
	writer.Submit(ctx, store.RequestLog{RequestID: "final", Endpoint: "/v1/responses", StartedAt: time.Now()}, true)
	stats := writer.Stats()
	if sink.outbox.Load() == 0 || stats.OutboxWrites == 0 {
		t.Fatalf("expected outbox fallback, sink=%d stats=%+v", sink.outbox.Load(), stats)
	}
	if stats.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", stats.Dropped)
	}
}

func TestFinalLogDropCountWhenAllDurableWritesFail(t *testing.T) {
	sink := &fakeSink{upsertErr: errors.New("db unavailable"), outboxErr: errors.New("outbox unavailable")}
	writer := NewWriter(sink, nil, config.RequestLogConfig{
		Concurrency:     1,
		BatchSize:       10,
		QueueMaxSize:    1,
		FlushInterval:   time.Hour,
		FinalSyncOnFull: true,
	})
	ctx := context.Background()
	writer.Submit(ctx, store.RequestLog{RequestID: "start", Endpoint: "/v1/responses", StartedAt: time.Now()}, false)
	writer.Submit(ctx, store.RequestLog{RequestID: "final", Endpoint: "/v1/responses", StartedAt: time.Now()}, true)
	if got := writer.Stats().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}
