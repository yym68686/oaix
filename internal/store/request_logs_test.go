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

func compactSQL(sql string) string {
	return strings.ToLower(strings.Join(strings.Fields(sql), " "))
}
