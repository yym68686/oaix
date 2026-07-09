package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

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
		_, _ = db.pool.Exec(cleanupCtx, "delete from gateway_request_logs where request_id in ($1, $2)", firstRequestID, secondRequestID)
	})

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
}

func configURL(value string) string {
	return value
}
