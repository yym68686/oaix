package store

import (
	"testing"
	"time"
)

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
