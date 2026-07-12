package store

import (
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeTokenStatusFilters(t *testing.T) {
	t.Run("defaults to available and cooling", func(t *testing.T) {
		got := normalizeTokenStatusFilters(nil)
		want := []string{Sub2APITokenStatusAvailable, Sub2APITokenStatusCooling}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	t.Run("normalizes aliases and removes duplicates", func(t *testing.T) {
		got := normalizeTokenStatusFilters([]string{" disabled ", "cooldown", "available", "active", "bogus", "inactive"})
		want := []string{Sub2APITokenStatusDisabled, Sub2APITokenStatusCooling, Sub2APITokenStatusAvailable}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}

func TestTokenStatusFilterSQL(t *testing.T) {
	sql := tokenStatusFilterSQL([]string{Sub2APITokenStatusCooling, Sub2APITokenStatusDisabled})
	for _, want := range []string{
		"t.is_active = true",
		"t.disabled_at is null",
		"t.cooldown_until > now()",
		"t.is_active = false or t.disabled_at is not null",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("expected SQL %q to contain %q", sql, want)
		}
	}
	if strings.Contains(sql, "t.cooldown_until <= now()") {
		t.Fatalf("expected SQL %q to exclude available-token condition", sql)
	}

	defaultSQL := tokenStatusFilterSQL(nil)
	for _, want := range []string{
		"t.cooldown_until is null or t.cooldown_until <= now()",
		"t.cooldown_until > now()",
	} {
		if !strings.Contains(defaultSQL, want) {
			t.Fatalf("expected default SQL %q to contain %q", defaultSQL, want)
		}
	}
	if strings.Contains(defaultSQL, "t.is_active = false or t.disabled_at is not null") {
		t.Fatalf("expected default SQL %q to exclude disabled-token condition", defaultSQL)
	}
}

func TestSub2APITokenCandidateQueryCastsRawPayloadToJSONB(t *testing.T) {
	expr := sub2APIRawPayloadSelectSQL()
	if !strings.Contains(expr, "t.raw_payload::jsonb") {
		t.Fatal("sub2api candidate query must cast raw_payload to jsonb before coalesce")
	}
	if strings.Contains(expr, "coalesce(t.raw_payload, '{}'::jsonb)") {
		t.Fatal("sub2api candidate query must not coalesce json raw_payload directly with jsonb")
	}
}

func TestValidateDistinctSub2APIRemoteAccounts(t *testing.T) {
	if err := ValidateDistinctSub2APIRemoteAccounts([]Sub2APIUsageSyncMapping{
		{TokenID: 1, RemoteAccountID: 10},
		{TokenID: 1, RemoteAccountID: 10},
		{TokenID: 2, RemoteAccountID: 11},
	}); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if err := ValidateDistinctSub2APIRemoteAccounts([]Sub2APIUsageSyncMapping{
		{TokenID: 1, RemoteAccountID: 10},
		{TokenID: 2, RemoteAccountID: 10},
	}); err == nil {
		t.Fatal("expected duplicate remote account validation error")
	}
}
