package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestRequestAnalyticsPendingOverlayAndMultiBatchDrain(t *testing.T) {
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
	if _, err := db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatalf("ReconcileRecordedTokenCosts returned error: %v", err)
	}

	suffix := time.Now().UnixNano()
	var ownerID int64
	if err := db.pool.QueryRow(ctx, `
		insert into platform_users(email, display_name, role, status)
		values($1, 'analytics fixture', 'admin', 'active')
		returning id
	`, fmt.Sprintf("analytics-%d@example.com", suffix)).Scan(&ownerID); err != nil {
		t.Fatalf("insert owner: %v", err)
	}
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(refresh_token, is_active, owner_user_id)
		values($1, true, $2)
		returning id
	`, fmt.Sprintf("analytics-refresh-%d", suffix), ownerID).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}

	const requestCount = requestAnalyticsQueueBatchSize + 1
	const costPerRequest = 0.01
	logs := make([]RequestLog, 0, requestCount)
	finishedAt := time.Now().UTC()
	success := true
	for index := 0; index < requestCount; index++ {
		requestID := fmt.Sprintf("analytics-%d-%d", suffix, index)
		logs = append(logs, RequestLog{
			RequestID:        requestID,
			OwnerUserID:      &ownerID,
			TokenOwnerUserID: &ownerID,
			Endpoint:         "/v1/responses",
			StartedAt:        finishedAt.Add(-time.Second),
			FinishedAt:       &finishedAt,
			Success:          &success,
			TokenID:          &tokenID,
			EstimatedCostUSD: float64Ptr(costPerRequest),
		})
	}
	if err := db.UpsertRequestLogs(ctx, logs); err != nil {
		t.Fatalf("UpsertRequestLogs returned error: %v", err)
	}

	before, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: tokenID}})
	if err != nil {
		t.Fatalf("TokenObservedCostsCurrentSnapshot before drain returned error: %v", err)
	}
	want := float64(requestCount) * costPerRequest
	assertApprox(t, valueOrZero(before[tokenID]), want)

	aggregated, err := db.AggregateRequestHourlyStats(ctx)
	if err != nil {
		t.Fatalf("AggregateRequestHourlyStats returned error: %v", err)
	}
	if aggregated != requestCount {
		t.Fatalf("aggregated rows = %d, want %d", aggregated, requestCount)
	}
	after, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: tokenID}})
	if err != nil {
		t.Fatalf("TokenObservedCostsCurrentSnapshot after drain returned error: %v", err)
	}
	assertApprox(t, valueOrZero(after[tokenID]), want)
}

func TestCurrentObservedCostSnapshotRepairsLateFinalizedRequestIDReuse(t *testing.T) {
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
	if _, err := db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatalf("ReconcileRecordedTokenCosts returned error: %v", err)
	}

	suffix := time.Now().UnixNano()
	var ownerID int64
	if err := db.pool.QueryRow(ctx, `
		insert into platform_users(email, display_name, role, status)
		values($1, 'late finalized fixture', 'admin', 'active')
		returning id
	`, fmt.Sprintf("late-finalized-%d@example.com", suffix)).Scan(&ownerID); err != nil {
		t.Fatalf("insert owner: %v", err)
	}
	var tokenID int64
	if err := db.pool.QueryRow(ctx, `
		insert into codex_tokens(refresh_token, is_active, owner_user_id)
		values($1, true, $2)
		returning id
	`, fmt.Sprintf("late-finalized-refresh-%d", suffix), ownerID).Scan(&tokenID); err != nil {
		t.Fatalf("insert token: %v", err)
	}
	requestID := fmt.Sprintf("late-finalized-request-%d", suffix)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_analytics_queue where request_log_id in (select id from gateway_request_logs where request_id = $1)`, requestID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_logs where request_id = $1`, requestID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_token_costs where token_id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from gateway_request_hourly_stats where owner_user_id = $1`, ownerID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from codex_tokens where id = $1`, tokenID)
		_, _ = db.pool.Exec(cleanupCtx, `delete from platform_users where id = $1`, ownerID)
	})

	startedAt := time.Now().UTC().Add(-2 * time.Minute)
	earlyFinishedAt := startedAt.Add(time.Second)
	earlyStatus := 503
	earlySuccess := false
	if err := db.UpsertRequestLogs(ctx, []RequestLog{{
		RequestID:   requestID,
		OwnerUserID: &ownerID,
		Endpoint:    "/v1/responses",
		StartedAt:   startedAt,
		FinishedAt:  &earlyFinishedAt,
		StatusCode:  &earlyStatus,
		Success:     &earlySuccess,
	}}); err != nil {
		t.Fatalf("write early finalized request log: %v", err)
	}
	if _, err := db.AggregateRequestHourlyStats(ctx); err != nil {
		t.Fatalf("aggregate early request log: %v", err)
	}

	lateFinishedAt := time.Now().UTC().Add(time.Minute)
	lateStatus := 200
	lateSuccess := true
	cost := 23.7137867
	if err := db.UpsertRequestLogs(ctx, []RequestLog{{
		RequestID:        requestID,
		OwnerUserID:      &ownerID,
		TokenOwnerUserID: &ownerID,
		Endpoint:         "/v1/responses",
		StartedAt:        startedAt,
		FinishedAt:       &lateFinishedAt,
		StatusCode:       &lateStatus,
		Success:          &lateSuccess,
		TokenID:          &tokenID,
		EstimatedCostUSD: &cost,
	}}); err != nil {
		t.Fatalf("write late finalized request log: %v", err)
	}

	var analyticsBeforeFinish bool
	var aggregateCost float64
	if err := db.pool.QueryRow(ctx, `
		select analytics_recorded_at < finished_at,
		       coalesce((select estimated_cost_usd from gateway_request_token_costs where token_id = $2), 0)
		from gateway_request_logs
		where request_id = $1
	`, requestID, tokenID).Scan(&analyticsBeforeFinish, &aggregateCost); err != nil {
		t.Fatalf("inspect late finalized fixture: %v", err)
	}
	if !analyticsBeforeFinish || aggregateCost != 0 {
		t.Fatalf("fixture did not reproduce stale aggregate: analytics_before_finish=%v aggregate_cost=%f", analyticsBeforeFinish, aggregateCost)
	}

	if _, err := db.pool.Exec(ctx, "drop index "+requestCostRepairIndexName); err != nil {
		t.Fatal(err)
	}
	// With no partial index, repair must not touch the heap even when it is locked.
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "lock table gateway_request_logs in access exclusive mode"); err != nil {
		t.Fatal(err)
	}
	shortCtx, shortCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	unchanged := map[int64]*float64{tokenID: float64Ptr(7)}
	err = db.overrideLateFinalizedRequestCosts(shortCtx, map[int64]int64{tokenID: tokenID}, unchanged)
	shortCancel()
	if err != nil || valueOrZero(unchanged[tokenID]) != 7 {
		t.Fatalf("repair without index touched logs or changed the snapshot: %v, %v", unchanged, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureRequestCostRepairIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureRequestCostRepairIndex(ctx); err != nil {
		t.Fatalf("ensure existing index: %v", err)
	}

	costs, err := db.TokenObservedCostsCurrentSnapshot(ctx, []Token{{ID: tokenID}})
	if err != nil {
		t.Fatalf("TokenObservedCostsCurrentSnapshot returned error: %v", err)
	}
	assertApprox(t, valueOrZero(costs[tokenID]), roundCostUSD(cost))
}

func float64Ptr(value float64) *float64 {
	return &value
}

func valueOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
