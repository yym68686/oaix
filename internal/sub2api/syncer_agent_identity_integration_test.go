package sub2api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

func TestSyncTargetImportsAgentIdentityForSupportedTarget(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OAIX_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var created []CreateAccountRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/admin/accounts":
			if r.Method != http.MethodGet || r.URL.Query().Get("group") != "10" {
				t.Fatalf("account list request = %s %s", r.Method, r.URL.String())
			}
			writeSub2APISuccess(t, w, AccountPage{Items: []Account{}, Total: 0, Page: 1, PageSize: 1000})
		case "/api/v1/settings/public":
			if r.Method != http.MethodGet {
				t.Fatalf("settings method = %s", r.Method)
			}
			writeSub2APISuccess(t, w, map[string]any{"version": "0.1.162"})
		case "/api/v1/admin/accounts/batch":
			if r.Method != http.MethodPost || r.Header.Get("X-API-Key") != "admin-fixture" {
				t.Fatalf("batch request method/auth mismatch")
			}
			var payload struct {
				Accounts []CreateAccountRequest `json:"accounts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode batch request: %v", err)
			}
			created = append(created, payload.Accounts...)
			writeSub2APISuccess(t, w, BatchCreateResult{
				Success: 1,
				Results: []BatchAccountResult{{Name: payload.Accounts[0].Name, ID: 501, Success: true}},
			})
		default:
			t.Fatalf("unexpected Sub2API request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	db, err := store.Connect(ctx, config.DatabaseConfig{
		URL: dsn, MaxConns: 4, MinConns: 1, ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	suffix := time.Now().UnixNano()
	user, _, err := db.CreateRegisteredUser(ctx, fmt.Sprintf("agent-sync-%d@example.test", suffix), "password123", "agent sync fixture")
	if err != nil {
		t.Fatal(err)
	}
	privateKey := integrationAgentIdentityPrivateKey(t)
	importResult, err := db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{{
		"auth_mode":                  agentidentity.AuthMode,
		"agent_runtime_id":           fmt.Sprintf("runtime-%d", suffix),
		"agent_private_key":          privateKey,
		"task_id":                    fmt.Sprintf("task-%d", suffix),
		"account_id":                 fmt.Sprintf("account-%d", suffix),
		"chatgpt_user_id":            fmt.Sprintf("user-%d", suffix),
		"chatgpt_account_is_fedramp": false,
		"email":                      fmt.Sprintf("agent-%d@example.test", suffix),
		"plan_type":                  "k12",
	}}, "agent-sync-integration")
	if err != nil {
		t.Fatal(err)
	}
	if importResult.Created != 1 || len(importResult.Tokens) != 1 {
		t.Fatalf("agent identity import counts = created:%d tokens:%d", importResult.Created, len(importResult.Tokens))
	}

	enabled, autoSyncNew := true, true
	adminKey := "admin-fixture"
	target, err := db.CreateSub2APISyncTarget(ctx, store.Sub2APISyncTargetInput{
		Name:                 fmt.Sprintf("agent-target-%d", suffix),
		BaseURL:              server.URL,
		AdminKey:             &adminKey,
		Enabled:              &enabled,
		OwnerUserID:          user.ID,
		PlanFilters:          []string{"k12"},
		TokenStatusFilters:   []string{store.Sub2APITokenStatusAvailable, store.Sub2APITokenStatusCooling},
		TargetGroupIDs:       []int64{10},
		MinAvailable:         0,
		TopUpBatchSize:       1,
		AccountConcurrency:   10,
		CheckIntervalSeconds: 300,
		AutoSyncNew:          &autoSyncNew,
	})
	if err != nil {
		t.Fatal(err)
	}

	run, err := NewSyncer(db, NewClient(server.Client()), nil, "client-fixture").SyncTarget(ctx, target.ID, "schedule")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != store.Sub2APISyncStatusCompleted || run.SelectedCount != 1 || run.SyncedCount != 1 || run.FailedCount != 0 {
		t.Fatalf("sync result = status:%s selected:%d synced:%d failed:%d", run.Status, run.SelectedCount, run.SyncedCount, run.FailedCount)
	}
	if len(created) != 1 {
		t.Fatalf("created account count = %d, want 1", len(created))
	}
	credentials := created[0].Credentials
	if credentials["auth_mode"] != agentidentity.AuthMode || credentials["chatgpt_account_id"] != fmt.Sprintf("account-%d", suffix) || credentials["chatgpt_user_id"] != fmt.Sprintf("user-%d", suffix) {
		t.Fatalf("created account is missing Agent Identity metadata")
	}
	if credentials["agent_runtime_id"] == "" || credentials["agent_private_key"] != privateKey {
		t.Fatalf("created account is missing Agent Identity credentials")
	}
	if _, ok := credentials["access_token"]; ok {
		t.Fatal("created Agent Identity account unexpectedly contains access_token")
	}
	mappings, err := db.ListSub2APISyncedTokenMappings(ctx, target, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(mappings) != 1 || mappings[0].TokenID != int64(importResult.Tokens[0].ID) || mappings[0].RemoteAccountID != 501 {
		t.Fatalf("synced mapping count/id mismatch")
	}
}

func integrationAgentIdentityPrivateKey(t *testing.T) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(der)
}
