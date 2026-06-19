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
	where, args := tokenListWhere(TokenListOptions{
		Query:  "acct",
		Status: "available",
		Plan:   "pro",
	}, false)
	if len(args) != 1 || args[0] != "%acct%" {
		t.Fatalf("args = %#v", args)
	}
	if !strings.Contains(where, "is_active = true") {
		t.Fatalf("status filter missing: %s", where)
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

func TestTokenListWhereScopedAddsOwnerFilter(t *testing.T) {
	ownerID := int64(42)
	where, args := tokenListWhereScoped(TokenListOptions{Status: "available"}, true, OwnerResources(ownerID))
	if len(args) != 1 || args[0] != ownerID {
		t.Fatalf("args = %#v, want owner id", args)
	}
	if !strings.Contains(where, "owner_user_id = $1") {
		t.Fatalf("owner filter missing: %s", where)
	}
	if !strings.Contains(where, "is_active = true") {
		t.Fatalf("status filter missing: %s", where)
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
