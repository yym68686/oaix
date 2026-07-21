package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
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

func TestUpsertAgentIdentitiesKeepsSharedWorkspaceRuntimesSeparate(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := Connect(ctx, config.DatabaseConfig{
		URL: configURL(dsn), MaxConns: 4, MinConns: 1, ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().UnixNano()
	user, _, err := db.CreateRegisteredUser(ctx, fmt.Sprintf("agent-owner-%d@example.test", suffix), "password123", "agent fixture")
	if err != nil {
		t.Fatal(err)
	}
	first := agentIdentityImportPayload(t, "runtime-a", "task-a", "user-a", "a@example.invalid")
	second := agentIdentityImportPayload(t, "runtime-b", "task-b", "user-b", "b@example.invalid")
	result, err := db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{first, second}, "test-agent-identity")
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 2 || result.Updated != 0 || result.Failed != 0 {
		t.Fatalf("first import result = %+v", result)
	}
	available, err := db.ListAvailableTokensScoped(ctx, OwnerResources(user.ID))
	if err != nil {
		t.Fatal(err)
	}
	if len(available) != 2 || available[0].AgentIdentity == nil || available[1].AgentIdentity == nil {
		t.Fatalf("available agent identities = %#v", available)
	}
	first["task_id"] = "task-a-next"
	result, err = db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{first}, "test-agent-identity")
	if err != nil {
		t.Fatal(err)
	}
	if result.Updated != 1 || result.Created != 0 {
		t.Fatalf("reimport result = %+v", result)
	}
	credentials, err := db.GetAgentIdentityCredentials(ctx, result.Tokens[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.RuntimeID != "runtime-a" || credentials.TaskID != "task-a-next" || credentials.AccountID != "shared-workspace" {
		t.Fatalf("stored credentials = %#v", credentials)
	}
}

func agentIdentityImportPayload(t *testing.T, runtimeID, taskID, userID, email string) map[string]any {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"auth_mode":         "agentIdentity",
		"agent_runtime_id":  runtimeID,
		"agent_private_key": base64.StdEncoding.EncodeToString(der),
		"task_id":           taskID,
		"account_id":        "shared-workspace",
		"chatgpt_user_id":   userID,
		"email":             email,
		"plan_type":         "pro",
	}
}
