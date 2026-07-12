package store

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresRepriceGPT56RequestCosts(t *testing.T) {
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
		t.Fatalf("Connect returned error: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("Migrate returned error: %v", err)
	}

	var ownerUserID int64
	if err := db.pool.QueryRow(ctx, `select id from platform_users where email = 'platform@oaix.local'`).Scan(&ownerUserID); err != nil {
		t.Fatalf("load platform owner: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	gpt56RequestID := "gpt56-reprice-" + suffix
	lateGPT56RequestID := "gpt56-reprice-late-" + suffix
	legacyRequestID := "gpt56-reprice-legacy-" + suffix
	gpt56Model := "gpt-5.6-sol-2026-07-01-" + suffix
	legacyModel := "gpt-5.4"
	startedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour).Add(3 * time.Minute)
	bucketStart := startedAt.Truncate(time.Hour)
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(email, refresh_token, owner_user_id)
		values ($1, $2, $3)
		returning id
	`, "gpt56-reprice-"+suffix+"@example.com", "refresh-"+suffix, ownerUserID).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_hourly_stats where owner_user_id = $1 and model_name = $2`, ownerUserID, gpt56Model)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_logs where request_id in ($1, $2, $3)`, gpt56RequestID, lateGPT56RequestID, legacyRequestID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_token_costs where token_id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from codex_tokens where id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_settings where key = $1`, gpt56CostRepriceSettingKey)
	}()

	if _, err := db.pool.Exec(ctx, `delete from gateway_settings where key = $1`, gpt56CostRepriceSettingKey); err != nil {
		t.Fatalf("clear reprice marker: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_logs (
			request_id, owner_user_id, token_owner_user_id, endpoint, model, model_name,
			token_id, started_at, finished_at, input_tokens, cache_write_input_tokens,
			cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			prompt_cache_trace, analytics_recorded_at
		)
		values (
			$1, $2, $2, '/v1/responses', $3, $3, $4, $5::timestamptz, $5::timestamptz + interval '1 second',
			100, 20, 30, 10, 110, 0.00138,
			'{"billing":{"pricing_model":"gpt-5.6-sol","input_per_million_usd":10,"cache_write_per_million_usd":12.5,"cached_input_per_million_usd":1,"output_per_million_usd":60,"estimated_cost_usd":0.00138}}'::jsonb,
			now()
		), (
			$6, $2, $2, '/v1/responses', $7, $7, $4, $5::timestamptz, $5::timestamptz + interval '1 second',
			100, 0, 0, 10, 110, 1.25, null, now()
		)
	`, gpt56RequestID, ownerUserID, gpt56Model, tokenID, startedAt, legacyRequestID, legacyModel); err != nil {
		t.Fatalf("insert request logs: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_token_costs(token_id, owner_user_id, request_count, estimated_cost_usd)
		values ($1, $2, 99, 99)
	`, tokenID, ownerUserID); err != nil {
		t.Fatalf("insert stale token cost: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_hourly_stats(owner_user_id, bucket_start, model_name, request_count, estimated_cost_usd)
		values ($1, $2, $3, 1, 0.00138)
	`, ownerUserID, bucketStart, gpt56Model); err != nil {
		t.Fatalf("insert stale hourly cost: %v", err)
	}

	result, err := db.RepriceGPT56RequestCosts(ctx)
	if err != nil {
		t.Fatalf("RepriceGPT56RequestCosts returned error: %v", err)
	}
	if result.UpdatedLogs != 1 || result.RebuiltTokenCosts < 1 || result.RebuiltHourlyCosts != 1 || result.Completed {
		t.Fatalf("unexpected initial reprice result: %+v", result)
	}

	var logCost float64
	if err := db.pool.QueryRow(ctx, `select estimated_cost_usd from gateway_request_logs where request_id = $1`, gpt56RequestID).Scan(&logCost); err != nil {
		t.Fatalf("load repriced request: %v", err)
	}
	assertApprox(t, logCost, 0.00069)

	var requestCount int64
	var tokenCost float64
	if err := db.pool.QueryRow(ctx, `
		select request_count, estimated_cost_usd
		from gateway_request_token_costs
		where token_id = $1
	`, tokenID).Scan(&requestCount, &tokenCost); err != nil {
		t.Fatalf("load rebuilt token cost: %v", err)
	}
	if requestCount != 2 {
		t.Fatalf("request count = %d, want 2", requestCount)
	}
	assertApprox(t, tokenCost, 1.25069)

	var hourlyCost float64
	if err := db.pool.QueryRow(ctx, `
		select estimated_cost_usd
		from gateway_request_hourly_stats
		where owner_user_id = $1 and bucket_start = $2 and model_name = $3
	`, ownerUserID, bucketStart, gpt56Model).Scan(&hourlyCost); err != nil {
		t.Fatalf("load rebuilt hourly cost: %v", err)
	}
	assertApprox(t, hourlyCost, 0.00069)

	lateStartedAt := time.Now().UTC()
	lateBucketStart := lateStartedAt.Truncate(time.Hour)
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_logs (
			request_id, owner_user_id, token_owner_user_id, endpoint, model, model_name,
			token_id, started_at, finished_at, input_tokens, cache_write_input_tokens,
			cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			analytics_recorded_at
		)
		values (
			$1, $2, $2, '/v1/responses', $3, $3, $4, $5::timestamptz, $5::timestamptz + interval '1 second',
			100, 20, 30, 10, 110, 0.00138, now()
		)
	`, lateGPT56RequestID, ownerUserID, gpt56Model, tokenID, lateStartedAt); err != nil {
		t.Fatalf("insert late request log: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_hourly_stats(owner_user_id, bucket_start, model_name, request_count, estimated_cost_usd)
		values ($1, $2, $3, 1, 0.00138)
		on conflict (owner_user_id, bucket_start, model_name) do update set
			request_count = gateway_request_hourly_stats.request_count + 1,
			estimated_cost_usd = gateway_request_hourly_stats.estimated_cost_usd + excluded.estimated_cost_usd
	`, ownerUserID, lateBucketStart, gpt56Model); err != nil {
		t.Fatalf("insert late hourly cost: %v", err)
	}

	if _, err := db.pool.Exec(ctx, `
		update gateway_settings
		set value = jsonb_set(value, '{first_seen_at}', to_jsonb((now() - interval '16 minutes')::text))
		where key = $1
	`, gpt56CostRepriceSettingKey); err != nil {
		t.Fatalf("age reprice marker: %v", err)
	}
	result, err = db.RepriceGPT56RequestCosts(ctx)
	if err != nil {
		t.Fatalf("final RepriceGPT56RequestCosts returned error: %v", err)
	}
	if !result.Completed || result.UpdatedLogs != 1 || result.RebuiltTokenCosts < 1 || result.RebuiltHourlyCosts < 1 {
		t.Fatalf("unexpected final reprice result: %+v", result)
	}
	if err := db.pool.QueryRow(ctx, `select estimated_cost_usd from gateway_request_logs where request_id = $1`, lateGPT56RequestID).Scan(&logCost); err != nil {
		t.Fatalf("load late repriced request: %v", err)
	}
	assertApprox(t, logCost, 0.00069)
	if err := db.pool.QueryRow(ctx, `select request_count, estimated_cost_usd from gateway_request_token_costs where token_id = $1`, tokenID).Scan(&requestCount, &tokenCost); err != nil {
		t.Fatalf("load final rebuilt token cost: %v", err)
	}
	if requestCount != 3 {
		t.Fatalf("final request count = %d, want 3", requestCount)
	}
	assertApprox(t, tokenCost, 1.25138)

	result, err = db.RepriceGPT56RequestCosts(ctx)
	if err != nil {
		t.Fatalf("completed RepriceGPT56RequestCosts returned error: %v", err)
	}
	if !result.Completed || result.UpdatedLogs != 0 || result.RebuiltTokenCosts != 0 || result.RebuiltHourlyCosts != 0 {
		t.Fatalf("completed reprice was not idempotent: %+v", result)
	}
}

func assertApprox(t *testing.T, actual, expected float64) {
	t.Helper()
	if math.Abs(actual-expected) > 0.000000001 {
		t.Fatalf("value = %.10f, want %.10f", actual, expected)
	}
}
