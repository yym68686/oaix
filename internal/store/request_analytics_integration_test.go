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

func float64Ptr(value float64) *float64 {
	return &value
}

func valueOrZero(value *float64) float64 {
	if value == nil {
		return 0
	}
	return *value
}
