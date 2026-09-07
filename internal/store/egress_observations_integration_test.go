package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/egress"
)

func TestEgressStartupMigrationPreservesServingAndAvoidsHotTableLocks(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated Postgres required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{URL: configURL(dsn), MaxConns: 4, MinConns: 1, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := db.UpdateEgressPolicy(ctx, egress.Policy{Enabled: true, TokenIDs: []int64{42}, SuccessSamplePercent: 10})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Pool().Exec(ctx, `drop table gateway_egress_observations; update schema_migrations set version=30 where name='oaix_go'`)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	_, err = lock.Exec(ctx, `lock table gateway_request_logs, codex_tokens, proxy_channels, token_proxy_bindings in access exclusive mode`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MigrateForStartup(ctx); err != nil {
		t.Fatalf("additive diagnostic migration touched serving tables: %v", err)
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetEgressPolicy(ctx)
	if err != nil || !got.Enabled || !got.UpdatedAt.Equal(p.UpdatedAt) {
		t.Fatalf("configuration changed: %+v %v", got, err)
	}
	if err := db.MigrateForStartup(ctx); err != nil {
		t.Fatal("migration not idempotent", err)
	}
}
