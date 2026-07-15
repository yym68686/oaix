package store

import (
	"strings"
	"testing"
	"time"
)

func TestFastCostRepriceSQLIsPreciseAndBounded(t *testing.T) {
	sql := compactSQL(repriceFastRequestLogsBatchSQL)
	for _, required := range []string{
		"timing_spans->>'fast_mode_requested' = 'true'",
		"timing_spans->>'service_tier' = 'priority'",
		"lower(btrim(coalesce(model_name, model, '')))",
		"limit $3::integer for update",
		"'gpt-5.6-sol'",
		"'gpt-5.6-terra'",
		"'gpt-5.6-luna'",
		"'gpt-5.5'",
		"'gpt-5.4-mini'",
		"'gpt-5.4-nano'",
		"'base_cost_usd'",
		"'multiplier'",
		"'final_cost_usd'",
		"jsonb_typeof(prompt_cache_trace::jsonb)",
		"update gateway_request_token_costs",
		"update gateway_request_hourly_stats",
		"on conflict (token_id) do nothing",
		"on conflict (owner_user_id, bucket_start, model_name) do nothing",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("Fast reprice SQL missing %q: %s", required, sql)
		}
	}
	if fastCostRepriceBatchSize > 500 {
		t.Fatalf("Fast reprice batch size = %d, want <= 500", fastCostRepriceBatchSize)
	}
	wantStart := time.Date(2026, time.July, 13, 22, 30, 12, 0, time.UTC)
	if !fastCostTrackingStartedAt.Equal(wantStart) {
		t.Fatalf("Fast tracking start = %s, want %s", fastCostTrackingStartedAt, wantStart)
	}
}

func TestFastCostRepriceSQLDoesNotTouchUserBilling(t *testing.T) {
	sql := compactSQL(repriceFastRequestLogsBatchSQL)
	for _, forbidden := range []string{"ledger", "settlement", "balance", "credit", "wallet", "0-0"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("OAIX observed-cost repair must not touch user billing marker %q: %s", forbidden, sql)
		}
	}
}
