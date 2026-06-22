package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
)

func TestUpsertTokenPayloadsForOwnerMergesByEmailWhenRefreshRotates(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
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

	suffix := time.Now().UnixNano()
	user, _, err := db.CreateRegisteredUser(ctx, fmt.Sprintf("merge-%d@example.test", suffix), "password123", "merge fixture")
	if err != nil {
		t.Fatalf("CreateRegisteredUser returned error: %v", err)
	}
	accountID := fmt.Sprintf("acct-merge-%d", suffix)
	tokenEmail := fmt.Sprintf("same-account-%d@example.test", suffix)

	first, err := db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{{
		"refresh_token": "rt-first",
		"account_id":    accountID,
		"email":         tokenEmail,
	}}, "test")
	if err != nil {
		t.Fatalf("first upsert returned error: %v", err)
	}
	if first.Created != 1 || len(first.Tokens) != 1 {
		t.Fatalf("first result = %+v", first)
	}

	second, err := db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{{
		"refresh_token": "rt-second",
		"account_id":    accountID,
		"email":         tokenEmail,
	}}, "test")
	if err != nil {
		t.Fatalf("second upsert returned error: %v", err)
	}
	if second.Updated != 1 || second.Created != 0 || len(second.Tokens) != 1 {
		t.Fatalf("second result = %+v", second)
	}
	if second.Tokens[0].ID != first.Tokens[0].ID {
		t.Fatalf("token id = %d, want %d", second.Tokens[0].ID, first.Tokens[0].ID)
	}
	if second.Tokens[0].RefreshToken != "rt-second" {
		t.Fatalf("refresh token was not updated: %q", second.Tokens[0].RefreshToken)
	}

	var count int
	if err := db.pool.QueryRow(ctx, `
		select count(*)::int
		from codex_tokens
		where owner_user_id = $1 and lower(email) = lower($2) and merged_into_token_id is null
	`, user.ID, tokenEmail).Scan(&count); err != nil {
		t.Fatalf("count query returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("matching token count = %d, want 1", count)
	}
}
