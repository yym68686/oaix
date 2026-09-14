package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestImportProgressDoesNotTouchUnrelatedJobs(t *testing.T) {
	for _, maxConns := range []int32{2, 1} {
		t.Run(fmt.Sprintf("connections_%d", maxConns), func(t *testing.T) { testImportProgressScope(t, maxConns) })
	}
}

func testImportProgressScope(t *testing.T, maxConns int32) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// A single connection also proves UpdateImportItems returns the batch
	// connection before requesting another one for progress aggregation.
	db, err := Connect(ctx, config.DatabaseConfig{URL: dsn, MaxConns: maxConns, ConnectTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	old, err := db.CreateQueuedImportJob(ctx, []map[string]any{{"access_token": "historical-fixture"}}, "front")
	if err != nil {
		t.Fatal(err)
	}
	defer db.pool.Exec(context.Background(), "delete from token_import_jobs where id=$1", old.ID)
	if _, err := db.pool.Exec(ctx, "update token_import_jobs set status='running',heartbeat_at=now()-interval '10 minutes' where id=$1", old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, "update token_import_items set status='validating' where job_id=$1", old.ID); err != nil {
		t.Fatal(err)
	}
	before, err := db.GetImportJob(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := db.CreateQueuedImportJob(ctx, []map[string]any{{"access_token": "current-fixture"}}, "front")
	if err != nil {
		t.Fatal(err)
	}
	defer db.pool.Exec(context.Background(), "delete from token_import_jobs where id=$1", current.ID)
	items, err := db.ListImportJobItems(ctx, current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateImportItems(ctx, []ImportItemUpdate{{ID: items[0].ID, Status: "failed", ErrorMessage: "fixture"}}); err != nil {
		t.Fatal(err)
	}
	after, err := db.GetImportJob(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !before.HeartbeatAt.Equal(*after.HeartbeatAt) {
		t.Fatal("updating one item refreshed an unrelated stale job heartbeat")
	}
	progress, err := db.GetImportJob(ctx, current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Status != "completed" || progress.ProcessedCount != 1 || progress.FailedCount != 1 {
		t.Fatalf("current job progress: %+v", progress)
	}
	if _, err := db.ResumeStaleImportJobs(ctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.GetImportJob(ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != "queued" {
		t.Fatal("unrelated stale job was not recoverable")
	}
}
