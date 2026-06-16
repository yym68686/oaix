package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestImportJobClaimAndResumeFixture(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL:            dsn,
		MaxConns:       4,
		MinConns:       0,
		ConnectTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := db.CreateQueuedImportJob(ctx, []map[string]any{
		{"access_token": "fixture-access-1"},
		{"access_token": "fixture-access-2"},
	}, "front")
	if err != nil {
		t.Fatal(err)
	}
	items, err := db.ClaimImportItems(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != "validating" {
		t.Fatalf("unexpected claimed items: %+v", items)
	}
	if err := db.UpdateImportItems(ctx, []ImportItemUpdate{{ID: items[0].ID, Status: "failed", ErrorMessage: "fixture"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(ctx, `
		update token_import_jobs
		set status = 'running', heartbeat_at = now() - interval '10 minutes'
		where id = $1
	`, job.ID); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.ResumeStaleImportJobs(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if resumed == 0 {
		t.Fatal("expected stale job to resume")
	}
}
