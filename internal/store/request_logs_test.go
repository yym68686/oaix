package store

import (
	"strings"
	"testing"
)

func TestAggregateRequestHourlyStatsTokenCostsGroupByTokenOnly(t *testing.T) {
	sql := compactSQL(aggregateRequestTokenCostsSQL)
	if !strings.Contains(sql, "coalesce(token_owner_user_id, owner_user_id) as owner_user_id") {
		t.Fatal("token cost aggregation should prefer token owner metadata")
	}
	if !strings.Contains(sql, "max(owner_user_id) as owner_user_id") {
		t.Fatal("token cost aggregation should collapse owner metadata per token")
	}
	if !strings.Contains(sql, "group by token_id") {
		t.Fatal("token cost aggregation must group by token_id")
	}
	if strings.Contains(sql, "group by token_id, owner_user_id") {
		t.Fatal("token cost aggregation must not emit duplicate rows for one token")
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

func compactSQL(sql string) string {
	return strings.ToLower(strings.Join(strings.Fields(sql), " "))
}
