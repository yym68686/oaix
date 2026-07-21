package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestPostgresStartupMigrationBootstrapsSchema(t *testing.T) {
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
	if err := db.MigrateForStartup(ctx); err != nil {
		t.Fatalf("MigrateForStartup returned error: %v", err)
	}
	health, err := db.CheckSchema(ctx)
	if err != nil {
		t.Fatalf("CheckSchema returned error: %v", err)
	}
	if !health.OK || health.SchemaVersion != SchemaVersion {
		t.Fatalf("unexpected schema health: %+v", health)
	}
}

func TestPostgresIntegrationFixture(t *testing.T) {
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
	if _, err := db.TokenCounts(ctx); err != nil {
		t.Fatalf("TokenCounts returned error: %v", err)
	}
}

func TestPostgresStartupMigrationSkipsDDLWhenSchemaIsCurrent(t *testing.T) {
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

	lockTx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock transaction: %v", err)
	}
	defer lockTx.Rollback(context.Background())
	if _, err := lockTx.Exec(ctx, `lock table gateway_request_logs in access exclusive mode`); err != nil {
		t.Fatalf("lock gateway_request_logs: %v", err)
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), time.Second)
	defer startupCancel()
	if err := db.MigrateForStartup(startupCtx); err != nil {
		t.Fatalf("MigrateForStartup replayed DDL for current schema: %v", err)
	}
}

func TestPostgresStartupMigrationFromVersion19DoesNotReplayHistoricalRepairs(t *testing.T) {
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
	if _, err := db.pool.Exec(ctx, `drop table if exists gateway_idempotency_records`); err != nil {
		t.Fatalf("drop version 20 table: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `drop table if exists token_agent_identities`); err != nil {
		t.Fatalf("drop version 22 table: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `update schema_migrations set version = 19 where name = 'oaix_go'`); err != nil {
		t.Fatalf("set schema version 19: %v", err)
	}

	lockTx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin historical table lock: %v", err)
	}
	defer lockTx.Rollback(context.Background())
	if _, err := lockTx.Exec(ctx, `lock table token_quota_snapshots in access exclusive mode`); err != nil {
		t.Fatalf("lock historical repair table: %v", err)
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer startupCancel()
	if err := db.MigrateForStartup(startupCtx); err != nil {
		t.Fatalf("incremental startup migration touched a historical repair table: %v", err)
	}
	var version int
	var idempotencyTable *string
	var agentIdentityTable *string
	if err := db.pool.QueryRow(ctx, `
		select (select version from schema_migrations where name = 'oaix_go'),
		       to_regclass('public.gateway_idempotency_records')::text,
		       to_regclass('public.token_agent_identities')::text
	`).Scan(&version, &idempotencyTable, &agentIdentityTable); err != nil {
		t.Fatalf("inspect incremental migration result: %v", err)
	}
	if version != SchemaVersion || idempotencyTable == nil || *idempotencyTable != "gateway_idempotency_records" ||
		agentIdentityTable == nil || *agentIdentityTable != "token_agent_identities" {
		t.Fatalf("unexpected incremental migration result: version=%d idempotency_table=%v agent_identity_table=%v", version, idempotencyTable, agentIdentityTable)
	}
}

func TestPostgresStartupMigrationFromVersion20AddsSub2APIUsageDailySnapshots(t *testing.T) {
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
	if _, err := db.pool.Exec(ctx, `drop table if exists sub2api_usage_daily_snapshots`); err != nil {
		t.Fatalf("drop daily snapshot table: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `alter table sub2api_usage_snapshots drop column if exists through_date`); err != nil {
		t.Fatalf("drop baseline through date: %v", err)
	}
	if _, err := db.pool.Exec(ctx, `update schema_migrations set version = 20 where name = 'oaix_go'`); err != nil {
		t.Fatalf("set schema version 20: %v", err)
	}
	if err := db.MigrateForStartup(ctx); err != nil {
		t.Fatalf("MigrateForStartup returned error: %v", err)
	}
	var tableName *string
	if err := db.pool.QueryRow(ctx, `select to_regclass('sub2api_usage_daily_snapshots')::text`).Scan(&tableName); err != nil {
		t.Fatalf("check daily snapshot table: %v", err)
	}
	if tableName == nil || *tableName != "sub2api_usage_daily_snapshots" {
		t.Fatalf("daily snapshot table = %v", tableName)
	}
	var throughDateColumn bool
	if err := db.pool.QueryRow(ctx, `
		select exists(
			select 1
			from information_schema.columns
			where table_schema = current_schema()
			  and table_name = 'sub2api_usage_snapshots'
			  and column_name = 'through_date'
		)
	`).Scan(&throughDateColumn); err != nil {
		t.Fatalf("check through_date column: %v", err)
	}
	if !throughDateColumn {
		t.Fatal("through_date column was not added")
	}
}

func TestPostgresGPT56CacheWriteObservability(t *testing.T) {
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

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	firstRequestID := "gpt56-cache-observability-" + suffix + "-1"
	secondRequestID := "gpt56-cache-observability-" + suffix + "-2"
	missingRequestID := "gpt56-cache-observability-" + suffix + "-missing"
	unobservedRequestID := "gpt56-cache-observability-" + suffix + "-unobserved"
	cacheKey := "gpt56-cache-key-" + suffix
	model := "gpt-5.6-sol"
	source := "response.usage.input_tokens_details.cache_write_tokens"
	success := true
	statusCode := 200
	inputTokens := 100
	cacheWriteTokens := 0
	firstCachedTokens := 0
	secondCachedTokens := 80
	outputTokens := 1
	totalTokens := 101
	now := time.Now().UTC()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(
			cleanupCtx,
			"delete from gateway_request_logs where request_id in ($1, $2, $3, $4)",
			firstRequestID,
			secondRequestID,
			missingRequestID,
			unobservedRequestID,
		)
	})

	before, err := db.CacheAnalytics(ctx, 1)
	if err != nil {
		t.Fatalf("CacheAnalytics before fixture returned error: %v", err)
	}

	logs := []RequestLog{
		{
			RequestID:              firstRequestID,
			Endpoint:               "/v1/responses",
			Model:                  &model,
			ModelName:              &model,
			StatusCode:             &statusCode,
			Success:                &success,
			AttemptCount:           1,
			StartedAt:              now.Add(-2 * time.Minute),
			InputTokens:            &inputTokens,
			CacheWriteInputTokens:  &cacheWriteTokens,
			CacheWriteTokensSource: &source,
			CachedInputTokens:      &firstCachedTokens,
			OutputTokens:           &outputTokens,
			TotalTokens:            &totalTokens,
			PromptCacheKeyHash:     &cacheKey,
		},
		{
			RequestID:              secondRequestID,
			Endpoint:               "/v1/responses",
			Model:                  &model,
			ModelName:              &model,
			StatusCode:             &statusCode,
			Success:                &success,
			AttemptCount:           1,
			StartedAt:              now.Add(-time.Minute),
			InputTokens:            &inputTokens,
			CacheWriteInputTokens:  &cacheWriteTokens,
			CacheWriteTokensSource: &source,
			CachedInputTokens:      &secondCachedTokens,
			OutputTokens:           &outputTokens,
			TotalTokens:            &totalTokens,
			PromptCacheKeyHash:     &cacheKey,
		},
		{
			RequestID:         missingRequestID,
			Endpoint:          "/v1/responses",
			Model:             &model,
			ModelName:         &model,
			StatusCode:        &statusCode,
			Success:           &success,
			AttemptCount:      1,
			StartedAt:         now.Add(-30 * time.Second),
			InputTokens:       &inputTokens,
			CachedInputTokens: &firstCachedTokens,
			OutputTokens:      &outputTokens,
			TotalTokens:       &totalTokens,
			PromptCacheTrace: map[string]any{
				"usage": map[string]any{
					"raw": map[string]any{
						"cache_write_reported": false,
					},
					"anomalies": []string{"gpt56_cache_write_tokens_missing"},
				},
			},
		},
		{
			RequestID:         unobservedRequestID,
			Endpoint:          "/v1/responses",
			Model:             &model,
			ModelName:         &model,
			StatusCode:        &statusCode,
			Success:           &success,
			AttemptCount:      1,
			StartedAt:         now.Add(-15 * time.Second),
			InputTokens:       &inputTokens,
			CachedInputTokens: &firstCachedTokens,
			OutputTokens:      &outputTokens,
			TotalTokens:       &totalTokens,
		},
	}
	if err := db.UpsertRequestLogs(ctx, logs); err != nil {
		t.Fatalf("UpsertRequestLogs returned error: %v", err)
	}

	items, total, err := db.ListRequestLogsFiltered(ctx, RequestLogListOptions{
		Limit:        1,
		IncludeTotal: true,
		RequestID:    secondRequestID,
	})
	if err != nil {
		t.Fatalf("ListRequestLogsFiltered returned error: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("request log lookup returned total=%d items=%d", total, len(items))
	}
	item := items[0]
	if item.CacheWriteInputTokens == nil || *item.CacheWriteInputTokens != 0 {
		t.Fatalf("cache write tokens = %v, want explicit zero", item.CacheWriteInputTokens)
	}
	if item.CacheWriteTokensSource == nil || *item.CacheWriteTokensSource != source {
		t.Fatalf("cache write source = %v, want %q", item.CacheWriteTokensSource, source)
	}
	if item.CachedInputTokens == nil || *item.CachedInputTokens != secondCachedTokens {
		t.Fatalf("cached input tokens = %v, want %d", item.CachedInputTokens, secondCachedTokens)
	}

	analytics, err := db.CacheAnalytics(ctx, 1)
	if err != nil {
		t.Fatalf("CacheAnalytics returned error: %v", err)
	}
	if analytics.CacheWriteReportedRequests < 2 {
		t.Fatalf("cache write reported requests = %d, want at least 2", analytics.CacheWriteReportedRequests)
	}
	if analytics.GPT56ZeroWriteThenReadSequences < 1 {
		t.Fatalf("zero-write-then-read sequences = %d, want at least 1", analytics.GPT56ZeroWriteThenReadSequences)
	}
	if analytics.CacheWriteTokenSources[source] < 2 {
		t.Fatalf("cache write source count = %d, want at least 2", analytics.CacheWriteTokenSources[source])
	}
	if analytics.GPT56CacheWriteMissingRequests != before.GPT56CacheWriteMissingRequests+1 {
		t.Fatalf(
			"confirmed missing requests = %d, want %d",
			analytics.GPT56CacheWriteMissingRequests,
			before.GPT56CacheWriteMissingRequests+1,
		)
	}
	if analytics.GPT56CacheWriteUnobservedRequests != before.GPT56CacheWriteUnobservedRequests+1 {
		t.Fatalf(
			"unobserved requests = %d, want %d",
			analytics.GPT56CacheWriteUnobservedRequests,
			before.GPT56CacheWriteUnobservedRequests+1,
		)
	}
}

func TestPostgresRequestLogInitialAfterFinalPreservesStreamDelivery(t *testing.T) {
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

	requestID := fmt.Sprintf("stream-delivery-order-%d", time.Now().UnixNano())
	connectionID := "oaixc-order-test"
	state := "terminal_flushed"
	startedAt := time.Now().UTC()
	trace := NewStreamDeliveryTrace(connectionID)
	trace.Upstream.ResponseCompletedCount = 1
	trace.Upstream.FirstResponseCompleted = &StreamDeliveryCompletedUpstream{
		EventOrdinal: 1,
		ReceivedAt:   startedAt,
		DataBytes:    2,
		DataSHA256:   "00",
	}
	trace.Downstream.FirstResponseCompleted = &StreamDeliveryCompletedDownstream{
		EventOrdinal:     1,
		WireBytes:        2,
		WireSHA256:       "11",
		WriteAttemptedAt: startedAt,
		WrittenBytes:     2,
		WriteResult:      "succeeded",
		WriteCompletedAt: startedAt,
		FlushResult:      "succeeded",
	}
	trace.End = StreamDeliveryEndTrace{At: startedAt, Reason: "upstream_eof_after_terminal"}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, "delete from gateway_request_logs where request_id = $1", requestID)
	})

	if err := db.UpsertRequestLogs(ctx, []RequestLog{{
		RequestID:              requestID,
		Endpoint:               "/v1/responses",
		IsStream:               true,
		StartedAt:              startedAt,
		AttemptCount:           1,
		StreamDeliveryState:    &state,
		DownstreamConnectionID: &connectionID,
		StreamDeliveryTrace:    trace,
	}}); err != nil {
		t.Fatalf("final UpsertRequestLogs returned error: %v", err)
	}
	if err := db.UpsertRequestLogs(ctx, []RequestLog{{
		RequestID:              requestID,
		Endpoint:               "/v1/responses",
		IsStream:               true,
		StartedAt:              startedAt.Add(-time.Millisecond),
		AttemptCount:           0,
		DownstreamConnectionID: &connectionID,
	}}); err != nil {
		t.Fatalf("late initial UpsertRequestLogs returned error: %v", err)
	}

	item, err := db.GetRequestLog(ctx, requestID)
	if err != nil {
		t.Fatalf("GetRequestLog returned error: %v", err)
	}
	if item.StreamDeliveryState == nil || *item.StreamDeliveryState != state {
		t.Fatalf("stream delivery state = %v, want %q", item.StreamDeliveryState, state)
	}
	if item.StreamDeliveryTrace == nil || item.StreamDeliveryTrace.State() != state {
		t.Fatalf("stream delivery trace was overwritten by late initial log: %+v", item.StreamDeliveryTrace)
	}
	if item.DownstreamConnectionID == nil || *item.DownstreamConnectionID != connectionID {
		t.Fatalf("connection id = %v, want %q", item.DownstreamConnectionID, connectionID)
	}
}

func TestPostgresLateInitialOutboxPreservesFinalBillingTrace(t *testing.T) {
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

	requestID := fmt.Sprintf("late-initial-billing-trace-%d", time.Now().UnixNano())
	startedAt := time.Now().UTC().Add(-time.Second)
	finishedAt := startedAt.Add(1500 * time.Millisecond)
	durationMS := 1500
	cost := 0.001725
	initial := RequestLog{
		RequestID:   requestID,
		Endpoint:    "/v1/responses",
		StartedAt:   startedAt,
		TimingSpans: map[string]any{"request_body_bytes": 123, "fast_mode_requested": true, "service_tier": "priority"},
		PromptCacheTrace: map[string]any{
			"usage": map[string]any{},
		},
	}
	final := initial
	final.FinishedAt = &finishedAt
	final.DurationMs = &durationMS
	final.EstimatedCostUSD = &cost
	final.TimingSpans = map[string]any{"request_body_bytes": 123, "total_ms": 1500, "fast_mode_requested": true, "service_tier": "priority"}
	final.PromptCacheTrace = map[string]any{
		"usage": map[string]any{
			"billing": map[string]any{
				"base_cost_usd":  0.00069,
				"multiplier":     2.5,
				"final_cost_usd": cost,
			},
		},
	}

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, `delete from request_log_outbox where request_id = $1`, requestID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_logs where request_id = $1`, requestID)
	}()

	// Reproduce the production ordering: the initial write falls back to the
	// durable outbox, the final write succeeds, then the older initial payload
	// is replayed after the completed row already exists.
	if err := db.EnqueueRequestLogOutbox(ctx, initial); err != nil {
		t.Fatalf("enqueue initial request log: %v", err)
	}
	if err := db.UpsertRequestLogs(ctx, []RequestLog{final}); err != nil {
		t.Fatalf("write final request log: %v", err)
	}
	drained, err := db.DrainRequestLogOutbox(ctx, 1000)
	if err != nil {
		t.Fatalf("drain initial request log: %v", err)
	}
	if drained == 0 {
		t.Fatal("initial request log was not replayed")
	}
	var pendingOutbox int
	if err := db.pool.QueryRow(ctx, `select count(*) from request_log_outbox where request_id = $1`, requestID).Scan(&pendingOutbox); err != nil {
		t.Fatalf("check replayed initial request log: %v", err)
	}
	if pendingOutbox != 0 {
		t.Fatalf("initial request log is still pending after drain: %d", pendingOutbox)
	}

	var finished bool
	var totalMS, multiplier string
	var persistedCost float64
	if err := db.pool.QueryRow(ctx, `
		select
			finished_at is not null,
			timing_spans->>'total_ms',
			prompt_cache_trace#>>'{usage,billing,multiplier}',
			estimated_cost_usd
		from gateway_request_logs
		where request_id = $1
	`, requestID).Scan(&finished, &totalMS, &multiplier, &persistedCost); err != nil {
		t.Fatalf("load replayed request log: %v", err)
	}
	if !finished || totalMS != "1500" || multiplier != "2.5" {
		t.Fatalf("late initial log overwrote final observability: finished=%v total_ms=%q multiplier=%q", finished, totalMS, multiplier)
	}
	assertApprox(t, persistedCost, cost)
}

func configURL(value string) string {
	return value
}
