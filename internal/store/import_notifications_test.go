package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestImportNotificationCommitRollbackAndReconnect(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{URL: dsn, MaxConns: 1, ConnectTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wake := make(chan struct{}, 1)
	listenCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- db.ListenForImportJobs(listenCtx, wake) }()
	awaitWake := func() {
		t.Helper()
		select {
		case <-wake:
		case <-ctx.Done():
			t.Fatal("listener did not wake")
		}
	}
	awaitWake() // Initial committed-queue scan.
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "select pg_notify('oaix_import_jobs','')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
		t.Fatal("rolled back enqueue produced a notification")
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := db.pool.Exec(ctx, "select pg_notify('oaix_import_jobs','')"); err != nil {
		t.Fatal(err)
	}
	awaitWake()
	// Terminate only the dedicated listener in this isolated fixture database.
	if _, err := db.pool.Exec(ctx, `select pg_terminate_backend(pid) from pg_stat_activity where datname=current_database() and application_name='oaix-import-listener'`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("connection failure was not reported")
		}
	case <-ctx.Done():
		t.Fatal("listener did not notice disconnect")
	}
	go func() { done <- db.ListenForImportJobs(listenCtx, wake) }()
	awaitWake() // Reconnect must scan committed jobs even without a new event.
	stop()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("listener did not release its connection")
	}
}
