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

func compactSQL(sql string) string {
	return strings.ToLower(strings.Join(strings.Fields(sql), " "))
}
