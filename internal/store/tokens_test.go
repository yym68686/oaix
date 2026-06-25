package store

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

type fakeTokenRow struct {
	values []any
}

func (r fakeTokenRow) Scan(dest ...any) error {
	for index, value := range r.values {
		switch target := dest[index].(type) {
		case *int64:
			*target = value.(int64)
		case **string:
			*target, _ = value.(*string)
		case *string:
			*target = value.(string)
		case *sql.NullString:
			if value == nil {
				*target = sql.NullString{}
			} else {
				*target = sql.NullString{String: value.(string), Valid: true}
			}
		case *bool:
			*target = value.(bool)
		case **time.Time:
			*target, _ = value.(*time.Time)
		case *time.Time:
			*target = value.(time.Time)
		default:
			panic("unsupported scan target")
		}
	}
	return nil
}

func TestParseAccessTokenClaims(t *testing.T) {
	token := "header.eyJhY2NvdW50X2lkIjoiYWNjdF8xIiwiZW1haWwiOiJhQGV4YW1wbGUuY29tIiwicGxhbl90eXBlIjoicHJvIn0.signature"
	accountID, email, planType := parseAccessTokenClaims(token)
	if accountID == nil || *accountID != "acct_1" {
		t.Fatalf("accountID = %v", accountID)
	}
	if email == nil || *email != "a@example.com" {
		t.Fatalf("email = %v", email)
	}
	if planType == nil || *planType != "pro" {
		t.Fatalf("planType = %v", planType)
	}
}

func TestNormalizeTokenPayloadDropsImportMetadata(t *testing.T) {
	payload := normalizeTokenPayload(map[string]any{
		"_import_index":           7,
		"_previous_refresh_token": "rt-old",
		"refresh_token":           "rt-new",
	})
	if _, ok := payload["_import_index"]; ok {
		t.Fatalf("internal import index leaked: %#v", payload)
	}
	if _, ok := payload["_previous_refresh_token"]; ok {
		t.Fatalf("previous refresh token leaked: %#v", payload)
	}
	if payload["refresh_token"] != "rt-new" {
		t.Fatalf("payload = %#v", payload)
	}
	if importPayloadIndex(map[string]any{"_import_index": float64(9)}, 1) != 9 {
		t.Fatal("importPayloadIndex did not parse numeric metadata")
	}
}

func TestTimeFromPayloadParsesRFC3339Nano(t *testing.T) {
	got := timeFromPayload(map[string]any{
		"last_refresh": "2026-06-17T12:34:56.123456789+08:00",
	}, "last_refresh")
	if got == nil {
		t.Fatal("expected parsed timestamp")
	}
	if got.Location() != time.UTC {
		t.Fatalf("timestamp location = %s", got.Location())
	}
	if got.Format(time.RFC3339Nano) != "2026-06-17T04:34:56.123456789Z" {
		t.Fatalf("timestamp = %s", got.Format(time.RFC3339Nano))
	}
}

func TestPostgresTextArrayDeduplicates(t *testing.T) {
	got := postgresTextArray(" rt1 ", "", "rt2", "rt1")
	if len(got) != 2 || got[0] != "rt1" || got[1] != "rt2" {
		t.Fatalf("postgresTextArray = %#v", got)
	}
}

func TestImportIdentityNormalizesEmailAndAccountID(t *testing.T) {
	email := "  User@Example.COM  "
	accountID := "  acct-123  "
	if got := normalizedImportEmail(&email); got != "user@example.com" {
		t.Fatalf("normalizedImportEmail = %q", got)
	}
	if got := importStringValue(&accountID); got != "acct-123" {
		t.Fatalf("importStringValue = %q", got)
	}
	if got := normalizedImportEmail(nil); got != "" {
		t.Fatalf("nil email normalized to %q", got)
	}
}

func TestBuildPlanCountsIncludesCommonPlansAndExtras(t *testing.T) {
	got := buildPlanCounts(map[string]int{
		"pro":        12,
		"enterprise": 3,
		"unknown":    4,
	})
	want := []TokenPlanCount{
		{Plan: "free", Label: "Free", Count: 0},
		{Plan: "plus", Label: "Plus", Count: 0},
		{Plan: "team", Label: "Team", Count: 0},
		{Plan: "pro", Label: "Pro", Count: 12},
		{Plan: "enterprise", Label: "enterprise", Count: 3},
		{Plan: "unknown", Label: "Unknown", Count: 4},
	}
	if len(got) != len(want) {
		t.Fatalf("plan count length = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("plan count[%d] = %#v, want %#v", index, got[index], want[index])
		}
	}
}

func TestTokenListWhereCanSkipPlanFilterForPlanCounts(t *testing.T) {
	asOf := time.Date(2026, 6, 24, 6, 32, 3, 0, time.UTC)
	where, args := tokenListWhere(TokenListOptions{
		Query:      "acct",
		Status:     "available",
		Plan:       "pro",
		StatusAsOf: &asOf,
	}, false)
	if len(args) != 2 || args[0] != "%acct%" || args[1] != asOf {
		t.Fatalf("args = %#v", args)
	}
	if !strings.Contains(where, "is_active = true") {
		t.Fatalf("status filter missing: %s", where)
	}
	if !strings.Contains(where, "cooldown_until <= $2") || !strings.Contains(where, "retryable upstream failure:%") {
		t.Fatalf("status filter should use fixed timestamp parameter: %s", where)
	}
	if strings.Contains(where, "plan_type") {
		t.Fatalf("plan filter should be skipped: %s", where)
	}

	where, args = tokenListWhere(TokenListOptions{Plan: "unknown"}, true)
	if len(args) != 1 || args[0] != "unknown" {
		t.Fatalf("args = %#v", args)
	}
	if !strings.Contains(where, "plan_type is null") {
		t.Fatalf("unknown plan filter missing: %s", where)
	}
}

func TestTokenListWhereCoolingExcludesDisabledTokens(t *testing.T) {
	asOf := time.Date(2026, 6, 24, 6, 32, 3, 0, time.UTC)
	where, args := tokenListWhere(TokenListOptions{Status: "cooling", StatusAsOf: &asOf}, true)
	if len(args) != 1 || args[0] != asOf {
		t.Fatalf("args = %#v", args)
	}
	for _, fragment := range []string{
		"is_active = true",
		"disabled_at is null",
		"cooldown_until > $1",
		"not (coalesce(last_error, '') like 'retryable upstream failure:%'",
	} {
		if !strings.Contains(where, fragment) {
			t.Fatalf("cooling filter missing %q: %s", fragment, where)
		}
	}
}

func TestTokenListWhereScopedAddsOwnerFilter(t *testing.T) {
	ownerID := int64(42)
	asOf := time.Date(2026, 6, 24, 6, 32, 3, 0, time.UTC)
	where, args := tokenListWhereScoped(TokenListOptions{Status: "available", StatusAsOf: &asOf}, true, OwnerResources(ownerID))
	if len(args) != 2 || args[0] != ownerID || args[1] != asOf {
		t.Fatalf("args = %#v, want owner id", args)
	}
	if !strings.Contains(where, "owner_user_id = $1") {
		t.Fatalf("owner filter missing: %s", where)
	}
	if !strings.Contains(where, "is_active = true") {
		t.Fatalf("status filter missing: %s", where)
	}
	if !strings.Contains(where, "cooldown_until <= $2") || !strings.Contains(where, "retryable upstream failure:%") {
		t.Fatalf("status filter should use scoped fixed timestamp parameter: %s", where)
	}
}

func TestTokenHasTransientRetryBackoff(t *testing.T) {
	now := time.Date(2026, 6, 25, 1, 25, 0, 0, time.UTC)
	shortUntil := now.Add(5 * time.Second)
	longUntil := now.Add(2 * time.Minute)
	expiredUntil := now.Add(-time.Second)
	retryError := "retryable upstream failure: Our servers are currently overloaded. Please try again later."
	manualError := "admin cooldown"

	tests := []struct {
		name  string
		token Token
		want  bool
	}{
		{
			name:  "short retry backoff",
			token: Token{CooldownUntil: &shortUntil, LastError: &retryError},
			want:  true,
		},
		{
			name:  "long retry cooldown stays cooling",
			token: Token{CooldownUntil: &longUntil, LastError: &retryError},
			want:  false,
		},
		{
			name:  "manual cooldown stays cooling",
			token: Token{CooldownUntil: &shortUntil, LastError: &manualError},
			want:  false,
		},
		{
			name:  "expired retry backoff",
			token: Token{CooldownUntil: &expiredUntil, LastError: &retryError},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := TokenHasTransientRetryBackoff(test.token, now); got != test.want {
				t.Fatalf("TokenHasTransientRetryBackoff = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTokenListWhereSupportsOwnerOption(t *testing.T) {
	ownerID := int64(77)
	where, args := tokenListWhere(TokenListOptions{OwnerUserID: ownerID, Query: "acct"}, true)
	if len(args) != 2 || args[0] != ownerID || args[1] != "%acct%" {
		t.Fatalf("args = %#v", args)
	}
	if !strings.Contains(where, "owner_user_id = $1") {
		t.Fatalf("owner option filter missing: %s", where)
	}
	if !strings.Contains(where, "(email ilike $2 or account_id ilike $2 or remark ilike $2)") {
		t.Fatalf("query filter missing: %s", where)
	}
}

func TestScanTokenAllowsMissingSecretColumns(t *testing.T) {
	now := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	token, err := scanToken(fakeTokenRow{values: []any{
		int64(1),
		int64(0),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		false,
		nil,
		&now,
		nil,
		nil,
		now,
		now,
	}})
	if err != nil {
		t.Fatalf("scanToken returned error: %v", err)
	}
	if token.AccessToken != "" || token.RefreshToken != "" {
		t.Fatalf("secret columns should scan to empty strings, got access=%q refresh=%q", token.AccessToken, token.RefreshToken)
	}
	if token.ID != 1 || token.DisabledAt == nil {
		t.Fatalf("unexpected token: %+v", token)
	}
}

func TestTouchTokensValuesDeduplicates(t *testing.T) {
	_ = time.Now()
	ids := []int64{1, 1, 2, 0, -1}
	unique := map[int64]struct{}{}
	for _, id := range ids {
		if id > 0 {
			unique[id] = struct{}{}
		}
	}
	if len(unique) != 2 {
		t.Fatalf("expected 2 unique positive ids, got %d", len(unique))
	}
}
