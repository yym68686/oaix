package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresQuotaSnapshotPlanTransitionClearsModelCapabilityLossAtomically(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL:            configURL(dsn),
		MaxConns:       4,
		MinConns:       1,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(email, access_token, refresh_token, plan_type, is_active)
		values ($1, $2, $3, 'pro', true)
		returning id
	`, "quota-plan-"+suffix+"@example.test", "access-"+suffix, "refresh-"+suffix).Scan(&tokenID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, "delete from codex_tokens where id = $1", tokenID)
	})

	if err := db.MarkTokenModelCapabilityLoss(ctx, tokenID, 0, "gpt-5.6-sol", "unsupported", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	plan := "free"
	result, err := db.SaveQuotaSnapshotAndPlan(
		ctx,
		tokenID,
		map[string]any{"plan_type": plan},
		&plan,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.PlanChanged || result.PlanType != plan {
		t.Fatalf("save result = %+v", result)
	}

	var storedPlan string
	var snapshotCount int
	var lossCount int
	if err := db.pool.QueryRow(ctx, "select plan_type from codex_tokens where id = $1", tokenID).Scan(&storedPlan); err != nil {
		t.Fatal(err)
	}
	if err := db.pool.QueryRow(ctx, "select count(*) from token_quota_snapshots where token_id = $1", tokenID).Scan(&snapshotCount); err != nil {
		t.Fatal(err)
	}
	if err := db.pool.QueryRow(ctx, "select count(*) from codex_token_scoped_cooldowns where token_id = $1", tokenID).Scan(&lossCount); err != nil {
		t.Fatal(err)
	}
	if storedPlan != plan || snapshotCount != 1 || lossCount != 0 {
		t.Fatalf("atomic state: plan=%q snapshots=%d model_losses=%d", storedPlan, snapshotCount, lossCount)
	}
}
