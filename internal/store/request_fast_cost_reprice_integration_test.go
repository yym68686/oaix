package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresRepriceFastRequestCosts(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set OAIX_TEST_DATABASE_URL to run Postgres integration fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	fillerRequestPrefix := "fast-reprice-filler-" + suffix + "-"
	requestIDs := []string{
		"fast-reprice-sol-" + suffix,
		"fast-reprice-mini-" + suffix,
		"fast-reprice-pending-" + suffix,
		"fast-reprice-normal-" + suffix,
		"fast-reprice-invalid-tier-" + suffix,
	}
	solModel := "GPT-5.6-SOL-" + suffix
	miniModel := "gpt-5.4-mini-" + suffix
	pendingModel := "gpt-5.5-" + suffix
	normalModel := "gpt-5.4-normal-" + suffix
	invalidTierModel := "gpt-5.6-luna-" + suffix
	models := []string{solModel, miniModel, pendingModel, normalModel, invalidTierModel}
	startedAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour).Add(3 * time.Minute)
	bucketStart := startedAt.Truncate(time.Hour)
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(email, refresh_token, owner_user_id)
		values ($1, $2, $3)
		returning id
	`, "fast-reprice-"+suffix+"@example.com", "refresh-"+suffix, ownerUserID).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_hourly_stats where owner_user_id = $1 and model_name = any($2::text[])`, ownerUserID, models)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_logs where request_id = any($1::text[])`, requestIDs)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_logs where request_id like $1`, fillerRequestPrefix+"%")
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_token_costs where token_id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from codex_tokens where id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_settings where key = any($1::text[])`, []string{fastCostRepriceSettingKey, gpt56CostRepriceSettingKey})
	}()

	if _, err := db.pool.Exec(ctx, `delete from gateway_settings where key = any($1::text[])`, []string{fastCostRepriceSettingKey, gpt56CostRepriceSettingKey}); err != nil {
		t.Fatalf("clear cost reprice markers: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_logs (
			request_id, owner_user_id, endpoint, model, model_name, started_at, finished_at,
			success, input_tokens, cached_input_tokens, output_tokens, total_tokens,
			estimated_cost_usd, timing_spans
		)
		select
			$1 || series::text, $2, '/v1/responses', 'gpt-5.6-luna', 'gpt-5.6-luna',
			$3::timestamptz - interval '1 minute' + series * interval '1 millisecond',
			$3::timestamptz - interval '59 seconds' + series * interval '1 millisecond',
			true, 1, 0, 1, 2, 0.000007, '{"service_tier":"default"}'::jsonb
		from generate_series(1, $4::integer) as series
	`, fillerRequestPrefix, ownerUserID, startedAt, fastCostRepriceBatchSize); err != nil {
		t.Fatalf("insert non-Fast cursor filler logs: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_logs (
			request_id, owner_user_id, token_owner_user_id, endpoint, model, model_name,
			token_id, started_at, finished_at, success, input_tokens, cache_write_input_tokens,
			cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			timing_spans, prompt_cache_trace, analytics_recorded_at
		)
		values
		(
			$1, $6, $6, '/v1/responses', $9, $9, $7, $8::timestamptz, $8::timestamptz + interval '1 second', true,
			100, 20, 30, 10, 110, 0.00069,
			'{"fast_mode_requested":true,"service_tier":"priority"}'::jsonb,
			'{"usage":{"billing":{"pricing_model":"gpt-5.6-sol","estimated_cost_usd":0.00069}}}'::jsonb,
			now()
		),
		(
			$2, $6, $6, '/v1/responses', $10, $10, $7, $8::timestamptz, $8::timestamptz + interval '1 second', true,
			100, 20, 30, 10, 110, 0.00009975,
			'{"fast_mode_requested":true,"service_tier":"priority"}'::jsonb,
			'{"billing":{"pricing_model":"gpt-5.4-mini","estimated_cost_usd":0.00009975}}'::jsonb,
			now()
		),
		(
			$3, $6, $6, '/v1/chat/completions', $11, $11, $7, $8::timestamptz, $8::timestamptz + interval '1 second', true,
			100, 20, 30, 10, 110, 0.000665,
			'{"fast_mode_requested":true,"service_tier":"priority"}'::jsonb,
			null,
			null
		),
		(
			$4, $6, $6, '/v1/responses', $12, $12, $7, $8::timestamptz, $8::timestamptz + interval '1 second', true,
			100, 0, 30, 10, 110, 0.25,
			'{"service_tier":"default"}'::jsonb,
			null,
			now()
		),
		(
			$5, $6, $6, '/v1/responses', $13, $13, $7, $8::timestamptz, $8::timestamptz + interval '1 second', true,
			100, 20, 30, 10, 110, 0.000138,
			'{"fast_mode_requested":true,"service_tier":"fast"}'::jsonb,
			null,
			null
		)
	`, requestIDs[0], requestIDs[1], requestIDs[2], requestIDs[3], requestIDs[4], ownerUserID, tokenID, startedAt,
		solModel, miniModel, pendingModel, normalModel, invalidTierModel); err != nil {
		t.Fatalf("insert Fast request logs: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `
		insert into gateway_request_hourly_stats (
			owner_user_id, bucket_start, model_name, request_count, success_count,
			input_tokens, cache_write_input_tokens, cached_input_tokens, output_tokens,
			total_tokens, estimated_cost_usd
		)
		values ($1, $2, $3, 1, 1, 100, 20, 30, 10, 110, 0.00069)
	`, ownerUserID, bucketStart, solModel); err != nil {
		t.Fatalf("insert stale hourly cost: %v", err)
	}

	// A missing or incomplete GPT-5.6 repair must prevent Fast from creating
	// state or touching any request. This models the cross-maintenance ordering
	// during a rolling deployment where the older repair spans many batches.
	for _, dependency := range []struct {
		name  string
		setup func() error
	}{
		{name: "missing", setup: func() error { return nil }},
		{name: "incomplete", setup: func() error {
			_, err := db.pool.Exec(ctx, `
				insert into gateway_settings(key, value, updated_at)
				values ($1, '{"completed":false,"phase":"final","cursor":1}'::jsonb, now())
				on conflict (key) do update set value = excluded.value, updated_at = excluded.updated_at
			`, gpt56CostRepriceSettingKey)
			return err
		}},
	} {
		t.Run("waits for GPT-5.6 repair "+dependency.name, func(t *testing.T) {
			if err := dependency.setup(); err != nil {
				t.Fatalf("set up %s GPT-5.6 state: %v", dependency.name, err)
			}
			waiting, err := db.RepriceFastRequestCosts(ctx)
			if err != nil {
				t.Fatalf("waiting RepriceFastRequestCosts returned error: %v", err)
			}
			if waiting.Phase != "waiting_gpt56" || waiting.Completed || waiting.ScannedLogs != 0 || waiting.UpdatedLogs != 0 || waiting.UpdatedTokenCosts != 0 || waiting.UpdatedHourlyCosts != 0 {
				t.Fatalf("unexpected dependency wait result: %+v", waiting)
			}
			var fastStateCount int
			if err := db.pool.QueryRow(ctx, `select count(*) from gateway_settings where key = $1`, fastCostRepriceSettingKey).Scan(&fastStateCount); err != nil {
				t.Fatalf("check Fast state: %v", err)
			}
			if fastStateCount != 0 {
				t.Fatalf("Fast state was created while GPT-5.6 was %s", dependency.name)
			}
			var cost float64
			if err := db.pool.QueryRow(ctx, `select estimated_cost_usd from gateway_request_logs where request_id = $1`, requestIDs[0]).Scan(&cost); err != nil {
				t.Fatalf("load untouched request: %v", err)
			}
			assertApprox(t, cost, 0.00069)
		})
	}
	if _, err := db.pool.Exec(ctx, `
		update gateway_settings
		set value = jsonb_build_object('completed', true, 'phase', 'completed'), updated_at = now()
		where key = $1
	`, gpt56CostRepriceSettingKey); err != nil {
		t.Fatalf("complete GPT-5.6 dependency: %v", err)
	}

	result, err := db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("RepriceFastRequestCosts returned error: %v", err)
	}
	if result.Phase != "initial" || result.ScannedLogs != int64(fastCostRepriceBatchSize) || result.UpdatedLogs != 0 || result.UpdatedTokenCosts != 0 || result.UpdatedHourlyCosts != 0 || result.Completed {
		t.Fatalf("non-Fast raw page did not advance safely: %+v", result)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("second RepriceFastRequestCosts returned error: %v", err)
	}
	if result.Phase != "initial" || result.ScannedLogs != 5 || result.UpdatedLogs != 3 || result.UpdatedTokenCosts != 1 || result.UpdatedHourlyCosts != 2 || result.Completed {
		t.Fatalf("unexpected initial Fast reprice result: %+v", result)
	}

	expectedCosts := map[string]float64{
		requestIDs[0]: 0.001725,
		requestIDs[1]: 0.0001995,
		requestIDs[2]: 0.0016625,
		requestIDs[3]: 0.25,
		requestIDs[4]: 0.000138,
	}
	for requestID, expected := range expectedCosts {
		var cost float64
		if err := db.pool.QueryRow(ctx, `select estimated_cost_usd from gateway_request_logs where request_id = $1`, requestID).Scan(&cost); err != nil {
			t.Fatalf("load request %s: %v", requestID, err)
		}
		assertApprox(t, cost, expected)
	}

	for _, requestID := range requestIDs[:3] {
		var baseCost, multiplier, finalCost float64
		if err := db.pool.QueryRow(ctx, `
			select
				(prompt_cache_trace#>>'{usage,billing,base_cost_usd}')::float8,
				(prompt_cache_trace#>>'{usage,billing,multiplier}')::float8,
				(prompt_cache_trace#>>'{usage,billing,final_cost_usd}')::float8
			from gateway_request_logs where request_id = $1
		`, requestID).Scan(&baseCost, &multiplier, &finalCost); err != nil {
			t.Fatalf("load Fast billing trace for %s: %v", requestID, err)
		}
		if multiplier != map[string]float64{requestIDs[0]: 2.5, requestIDs[1]: 2, requestIDs[2]: 2.5}[requestID] {
			t.Fatalf("request %s multiplier = %v", requestID, multiplier)
		}
		assertApprox(t, finalCost, expectedCosts[requestID])
		if baseCost <= 0 || finalCost <= baseCost {
			t.Fatalf("request %s ambiguous billing trace: base=%v final=%v", requestID, baseCost, finalCost)
		}
	}
	var legacyMultiplier float64
	if err := db.pool.QueryRow(ctx, `select (prompt_cache_trace#>>'{billing,multiplier}')::float8 from gateway_request_logs where request_id = $1`, requestIDs[1]).Scan(&legacyMultiplier); err != nil {
		t.Fatalf("legacy root billing trace was not repaired: %v", err)
	}
	if legacyMultiplier != 2 {
		t.Fatalf("legacy root billing multiplier = %v, want 2", legacyMultiplier)
	}

	var requestCount int64
	var tokenCost float64
	if err := db.pool.QueryRow(ctx, `select request_count, estimated_cost_usd from gateway_request_token_costs where token_id = $1`, tokenID).Scan(&requestCount, &tokenCost); err != nil {
		t.Fatalf("missing token aggregate was not rebuilt: %v", err)
	}
	if requestCount != 3 {
		t.Fatalf("rebuilt token request count = %d, want 3", requestCount)
	}
	assertApprox(t, tokenCost, 0.2519245)

	for model, expected := range map[string]float64{solModel: 0.001725, miniModel: 0.0001995} {
		var hourlyCost float64
		if err := db.pool.QueryRow(ctx, `
			select estimated_cost_usd from gateway_request_hourly_stats
			where owner_user_id = $1 and bucket_start = $2 and model_name = $3
		`, ownerUserID, bucketStart, model).Scan(&hourlyCost); err != nil {
			t.Fatalf("load hourly cost for %s: %v", model, err)
		}
		assertApprox(t, hourlyCost, expected)
	}

	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("initial wait returned error: %v", err)
	}
	if result.Phase != "initial" || result.ScannedLogs != 0 || result.UpdatedLogs != 0 || result.Completed {
		t.Fatalf("unexpected initial wait result: %+v", result)
	}
	if _, err := db.pool.Exec(ctx, `
		update gateway_settings
		set value = jsonb_set(value, '{first_seen_at}', to_jsonb((now() - interval '16 minutes')::text))
		where key = $1
	`, fastCostRepriceSettingKey); err != nil {
		t.Fatalf("age Fast reprice marker: %v", err)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("initial phase completion returned error: %v", err)
	}
	if result.Phase != "final" || result.ScannedLogs != 0 || result.Completed {
		t.Fatalf("unexpected initial phase completion: %+v", result)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("final scan returned error: %v", err)
	}
	if result.Phase != "final" || result.ScannedLogs != int64(fastCostRepriceBatchSize) || result.UpdatedLogs != 0 || result.UpdatedTokenCosts != 0 || result.UpdatedHourlyCosts != 0 || result.Completed {
		t.Fatalf("final non-Fast raw page was not idempotent: %+v", result)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("final candidate scan returned error: %v", err)
	}
	if result.Phase != "final" || result.ScannedLogs != 5 || result.UpdatedLogs != 0 || result.UpdatedTokenCosts != 0 || result.UpdatedHourlyCosts != 0 || result.Completed {
		t.Fatalf("final scan was not idempotent: %+v", result)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("final phase completion returned error: %v", err)
	}
	if result.Phase != "completed" || !result.Completed || result.ScannedLogs != 0 {
		t.Fatalf("unexpected final completion: %+v", result)
	}
	result, err = db.RepriceFastRequestCosts(ctx)
	if err != nil {
		t.Fatalf("completed Fast reprice returned error: %v", err)
	}
	if result.Phase != "completed" || !result.Completed || result.ScannedLogs != 0 || result.UpdatedLogs != 0 || result.UpdatedTokenCosts != 0 || result.UpdatedHourlyCosts != 0 {
		t.Fatalf("completed Fast reprice was not idempotent: %+v", result)
	}

	var settingCompleted bool
	if err := db.pool.QueryRow(ctx, `select (value->>'completed')::boolean from gateway_settings where key = $1`, fastCostRepriceSettingKey).Scan(&settingCompleted); err != nil {
		t.Fatalf("load Fast reprice setting: %v", err)
	}
	if !settingCompleted {
		t.Fatal("Fast reprice setting was not completed")
	}
}
