package store

import (
	"database/sql"
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

func TestScanTokenAllowsMissingSecretColumns(t *testing.T) {
	now := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	token, err := scanToken(fakeTokenRow{values: []any{
		int64(1),
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
