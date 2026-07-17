package store

import (
	"os"
	"strings"
	"testing"
)

func TestAggregateRequestHourlyStatsTokenCostsGroupByTokenOnly(t *testing.T) {
	sql := compactSQL(aggregateRequestTokenCostsSQL)
	if !strings.Contains(sql, "max(coalesce(token_owner_user_id, owner_user_id)) as owner_user_id") {
		t.Fatal("token cost aggregation should prefer token owner metadata")
	}
	if !strings.Contains(sql, "group by token_id") {
		t.Fatal("token cost aggregation must group by token_id")
	}
	if strings.Contains(sql, "group by token_id, owner_user_id") {
		t.Fatal("token cost aggregation must not emit duplicate rows for one token")
	}
}

func TestRequestAnalyticsQueueOnlyAggregatesClaimedIDs(t *testing.T) {
	sql := compactSQL(aggregateRequestTokenCostsSQL)
	for _, fragment := range []string{
		"id = any($1::integer[])",
		"analytics_recorded_at is null",
		"finished_at is not null",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("token aggregation missing bounded queue fragment %q: %s", fragment, sql)
		}
	}
	if strings.Contains(sql, "order by id") || strings.Contains(sql, "limit 5000") {
		t.Fatalf("token aggregation still discovers work from the request-log heap: %s", sql)
	}
}

func TestRequestAnalyticsQueueEnqueuesOnlyFinishedPendingLogs(t *testing.T) {
	source, err := os.ReadFile("request_logs.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "func (s *Store) UpsertRequestLogs")
	if start < 0 {
		t.Fatal("UpsertRequestLogs source not found")
	}
	endOffset := strings.Index(text[start:], "func (s *Store) EnqueueRequestLogOutbox")
	if endOffset < 0 {
		t.Fatal("EnqueueRequestLogOutbox source not found")
	}
	sql := compactSQL(text[start : start+endOffset])
	for _, fragment := range []string{
		"with upserted as",
		"insert into gateway_request_analytics_queue",
		"finished_at is not null",
		"analytics_recorded_at is null",
		"on conflict (request_log_id) do nothing",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("request analytics enqueue missing fragment %q: %s", fragment, sql)
		}
	}
	if requestAnalyticsLegacyBackfillWindow != 6_000 {
		t.Fatalf("legacy backfill window = %d, want 6000 proven ID range", requestAnalyticsLegacyBackfillWindow)
	}
	if requestAnalyticsQueueBatchSize != 500 {
		t.Fatalf("analytics queue batch size = %d, want 500", requestAnalyticsQueueBatchSize)
	}
}

func TestFillMissingObservedCostsDefaultsZero(t *testing.T) {
	existing := 12.34
	result := map[int64]*float64{
		2: &existing,
	}
	fillMissingObservedCosts([]Token{
		{ID: 1},
		{ID: 2},
		{ID: 0},
		{ID: 1},
	}, result)

	if got := result[1]; got == nil || *got != 0 {
		t.Fatalf("missing token cost = %v, want 0", got)
	}
	if got := result[2]; got == nil || *got != existing {
		t.Fatalf("existing token cost = %v, want %v", got, existing)
	}
	if _, ok := result[0]; ok {
		t.Fatal("invalid token ID should not be added")
	}
}

func TestGPT56CacheAnalyticsModelFilterIsExplicit(t *testing.T) {
	filter := compactSQL(gpt56ModelSQL)
	for _, model := range []string{"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"} {
		if !strings.Contains(filter, model) {
			t.Fatalf("GPT-5.6 analytics filter missing %q: %s", model, filter)
		}
	}
	for _, fragment := range []string{"model_name in", "model_name >=", "model_name <", "model_name is null", "model in"} {
		if !strings.Contains(filter, fragment) {
			t.Fatalf("GPT-5.6 analytics filter missing index-friendly fragment %q: %s", fragment, filter)
		}
	}
	for _, forbidden := range []string{"= 'gpt-5.6'", "like ", "lower(", "coalesce("} {
		if strings.Contains(filter, forbidden) {
			t.Fatalf("GPT-5.6 analytics filter uses overbroad match %q: %s", forbidden, filter)
		}
	}
}

func TestRequestUsageSummaryUsesHourlyAggregate(t *testing.T) {
	source, err := os.ReadFile("request_admin.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "func (s *Store) RequestUsageSummaryScoped")
	if start < 0 {
		t.Fatal("RequestUsageSummaryScoped source not found")
	}
	endOffset := strings.Index(text[start:], "func (s *Store) RequestUsageByOwner")
	if endOffset < 0 {
		t.Fatal("RequestUsageByOwner source not found")
	}
	body := compactSQL(text[start : start+endOffset])
	if !strings.Contains(body, "from gateway_request_hourly_stats") {
		t.Fatalf("usage summary does not use hourly aggregates: %s", body)
	}
	if strings.Contains(body, "from gateway_request_logs") {
		t.Fatalf("usage summary still scans request logs: %s", body)
	}
}

func TestAggregateObservedCostSnapshotAvoidsRequestLogFallback(t *testing.T) {
	source, err := os.ReadFile("request_logs.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "func (s *Store) TokenObservedCostsAggregateSnapshot")
	if start < 0 {
		t.Fatal("TokenObservedCostsAggregateSnapshot source not found")
	}
	endOffset := strings.Index(text[start:], "func (s *Store) tokenObservedCosts")
	if endOffset < 0 {
		t.Fatal("tokenObservedCosts source not found")
	}
	body := text[start : start+endOffset]
	if !strings.Contains(body, "addRequestCostsByTokenAggregate") {
		t.Fatalf("aggregate snapshot does not read token aggregates: %s", body)
	}
	for _, forbidden := range []string{"addRequestCostsByTokenLogs", "addRequestCostsByUniqueAccountFallback"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("aggregate snapshot uses unbounded fallback %q: %s", forbidden, body)
		}
	}
}

func compactSQL(sql string) string {
	return strings.ToLower(strings.Join(strings.Fields(sql), " "))
}
