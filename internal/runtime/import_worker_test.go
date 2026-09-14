package runtime

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

func TestImportLoopWakesAndDrainsWithoutMaintenanceTick(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wake := make(chan struct{}, 1)
	called := make(chan struct{}, 10)
	var pending atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runImportLoop(ctx, wake, time.Hour, func() {}, func() (int, error) {
			count := pending.Load()
			if count > 0 {
				pending.Add(-1)
			}
			called <- struct{}{}
			return int(count), nil
		})
	}()
	waitCall := func() {
		t.Helper()
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("import worker waited for maintenance instead of processing work")
		}
	}
	waitCall() // Empty startup scan.
	pending.Store(3)
	wake <- struct{}{} // One coalesced event must drain all three batches.
	for i := 0; i < 4; i++ {
		waitCall()
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle worker did not stop")
	}
}

func TestImportWorkersClaimEachRefreshOnce(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var calls atomic.Int32
	prefix := fmt.Sprintf("mock-refresh-%d-", time.Now().UnixNano())
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if strings.HasPrefix(r.Form.Get("refresh_token"), prefix) {
			calls.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"invalid_grant"}`)
	}))
	defer upstream.Close()
	cfg := config.Config{
		Database:   config.DatabaseConfig{URL: dsn, MaxConns: 4, ConnectTimeout: time.Second},
		Import:     config.ImportConfig{MaxConcurrency: 2, StagingBatchSize: 2},
		Upstream:   config.UpstreamConfig{OAuthTokenURL: upstream.URL},
		RequestLog: config.RequestLogConfig{AggregationWindow: time.Hour},
	}
	db, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owner, err := db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var payloads []map[string]any
	for i := 0; i < 20; i++ {
		payloads = append(payloads, map[string]any{"refresh_token": fmt.Sprintf("%s%d", prefix, i)})
	}
	job, err := db.CreateQueuedImportJobForOwner(ctx, owner, payloads, "front")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Pool().Exec(context.Background(), "delete from token_import_jobs where id=$1", job.ID)
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		go func() { RunImportWorker(workerCtx, cfg, nil, db, nil); done <- struct{}{} }()
	}
	defer func() { stop(); <-done; <-done }()
	for {
		progress, err := db.GetImportJob(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if progress.Status == "completed" {
			if progress.FailedCount != 20 || calls.Load() != 20 {
				t.Fatalf("duplicate or missing refresh: failures=%d calls=%d", progress.FailedCount, calls.Load())
			}
			return
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestImportLoopPeriodicSweepRecoversWithoutNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var sweeps atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		runImportLoop(ctx, nil, time.Millisecond, func() {
			if sweeps.Add(1) == 2 {
				cancel()
			}
		}, func() (int, error) { return 0, nil })
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fallback sweep did not run")
	}
}

func TestImportWorkerPostgresNotificationLatency(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cfg := config.Config{
		Database:   config.DatabaseConfig{URL: dsn, MaxConns: 4, ConnectTimeout: time.Second},
		Import:     config.ImportConfig{MaxConcurrency: 2, StagingBatchSize: 1},
		RequestLog: config.RequestLogConfig{AggregationWindow: time.Hour},
	}
	db, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owner, err := db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { defer close(done); RunImportWorker(workerCtx, cfg, logger, db, nil) }()
	defer func() { stop(); <-done }()
	// Use another Store to prove that the wake-up crosses process/connection
	// boundaries and is not an in-memory shortcut.
	writer, err := store.Connect(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var enqueueMS, queueMS, completeMS []float64
	for i := 0; i < 25; i++ {
		start := time.Now()
		job, err := writer.CreateQueuedImportJobForOwner(ctx, owner, []map[string]any{{"access_token": fmt.Sprintf("import-latency-fixture-%d-%d", start.UnixNano(), i), "is_active": false, "share_enabled": false}}, "front")
		if err != nil {
			t.Fatal(err)
		}
		enqueueMS = append(enqueueMS, float64(time.Since(start).Microseconds())/1000)
		deadline := time.Now().Add(2 * time.Second)
		for {
			items, err := writer.ListImportJobItems(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) == 1 && items[0].Status == "published" {
				item := items[0]
				token, err := writer.GetToken(ctx, *item.TokenID)
				if err != nil {
					t.Fatal(err)
				}
				if token.IsActive || token.ShareEnabled {
					t.Fatal("diagnostic import became eligible for traffic")
				}
				queueMS = append(queueMS, float64(item.ValidationStarted.Sub(item.CreatedAt).Microseconds())/1000)
				completeMS = append(completeMS, float64(item.PublishedAt.Sub(item.CreatedAt).Microseconds())/1000)
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("committed job did not finish without maintenance tick")
			}
			time.Sleep(time.Millisecond)
		}
	}
	for name, samples := range map[string][]float64{"enqueue": enqueueMS, "queue": queueMS, "durable_item_completion": completeMS} {
		sort.Float64s(samples)
		t.Logf("%s ms: n=%d p50=%.3f p95=%.3f max=%.3f (local PostgreSQL, synthetic access tokens, no OAuth)", name, len(samples), samples[len(samples)/2], samples[len(samples)*95/100], samples[len(samples)-1])
	}
}
