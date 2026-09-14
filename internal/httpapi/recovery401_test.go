package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/recovery"
	"github.com/yym68686/oaix/internal/store"
)

func TestRecoveryAccountNameEligibility(t *testing.T) {
	email, account, plan := "owner@example.test", "12345678-1234-4234-8234-123456abcdef", recovery.EligiblePlan
	c := store.Recovery401Candidate{Token: store.Token{Email: &email, AccountID: &account, PlanType: &plan}}
	got, err := recoveryAccountName(c)
	if err != nil || got != email+"----myWorkspace-abcdef" {
		t.Fatalf("got=%s err=%v", got, err)
	}
	c.Name = email + "----Custom Space"
	got, err = recoveryAccountName(c)
	if err != nil || got != c.Name {
		t.Fatalf("explicit name lost: %s %v", got, err)
	}
	for _, other := range []string{"business", "team", "plus", "free", "self_serve_business"} {
		c.Token.PlanType = &other
		if _, err := recoveryAccountName(c); err == nil {
			t.Fatalf("accepted %s", other)
		}
	}
}

func TestRecovery401WorkerIntegration(t *testing.T) {
	dsn := os.Getenv("OAIX_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated test Postgres")
	}
	ctx := context.Background()
	db, err := store.Connect(ctx, config.DatabaseConfig{URL: dsn, MaxConns: 6, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owner, err := db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"success", "site_failure", "probe_failure", "concurrent_edit", "manual_disabled", "other_plan"} {
		t.Run(mode, func(t *testing.T) {
			account := "12345678-1234-4234-8234-123456abcdef"
			email := fmt.Sprintf("recovery-%s-%d@example.test", mode, time.Now().UnixNano())
			imported, err := db.UpsertTokenPayloadsForOwner(ctx, owner, []map[string]any{{"email": email, "account_id": account, "plan_type": recovery.EligiblePlan, "access_token": "old-access", "refresh_token": email}}, "recovery-test")
			if err != nil {
				t.Fatal(err)
			}
			token := imported.Tokens[0]
			defer db.Pool().Exec(ctx, `delete from codex_tokens where id=$1`, token.ID)
			status := 401
			if err := db.MarkTokenErrorWithContext(ctx, token.ID, "upstream 401", true, nil, store.TokenStateEventContext{StatusCode: &status}); err != nil {
				t.Fatal(err)
			}
			claims, _ := json.Marshal(map[string]any{"exp": time.Now().Add(time.Hour).Unix(), "https://api.openai.com/auth": map[string]any{"chatgpt_account_id": account, "chatgpt_plan_type": recovery.EligiblePlan}, "https://api.openai.com/profile": map[string]any{"email": email}})
			fresh := "e30." + base64.RawURLEncoding.EncodeToString(claims) + ".signature"
			posts := 0
			website := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				posts++
				if mode == "site_failure" {
					_, _ = w.Write([]byte(`{"success":true,"status":"success","results":[{"success":false,"message":"workspace selection failed"}]}`))
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "status": "success", "results": []any{map[string]any{"success": true, "payload": map[string]any{"accounts": []any{map[string]any{"credentials": map[string]any{"access_token": fresh, "refresh_token": "fresh-" + email, "chatgpt_account_id": account}}}}}}})
			}))
			defer website.Close()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+fresh {
					t.Error("probe used old secret")
				}
				if mode == "concurrent_edit" {
					_, err := db.SetTokenActive(ctx, token.ID, false, true)
					if err != nil {
						t.Error(err)
					}
				}
				if mode == "probe_failure" {
					w.WriteHeader(401)
					_, _ = w.Write([]byte(`{"error":{"code":"token_invalidated"}}`))
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"test\"}]}]}}\n\n")
			}))
			defer upstream.Close()
			a := NewApp(config.Config{Recovery401: config.Recovery401Config{Enabled: true, BaseURL: website.URL}, Upstream: config.UpstreamConfig{ResponsesURL: upstream.URL}}, nil, db, nil, nil, nil)
			if mode == "manual_disabled" {
				if _, err := db.SetTokenActive(ctx, token.ID, false, true); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "other_plan" {
				if _, err := db.Pool().Exec(ctx, `update codex_tokens set plan_type='business' where id=$1`, token.ID); err != nil {
					t.Fatal(err)
				}
			}
			ids, err := db.ListRecovery401Candidates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			selected := false
			for _, id := range ids {
				if id == token.ID {
					selected = true
				}
			}
			expected := mode != "manual_disabled" && mode != "other_plan"
			if selected != expected {
				t.Fatalf("candidate eligibility=%t expected=%t", selected, expected)
			}
			a.run401Recovery(ctx, token.ID)
			latest, err := db.GetToken(ctx, token.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "success" {
				if !latest.IsActive || latest.AccessToken != fresh || latest.DisabledAt != nil || latest.LastError != nil {
					t.Fatal("complete recovery was not committed")
				}
				// An in-flight response using the old credential must not disable the new one.
				fence := store.QuotaRecoveryCredentialFence{AccessToken: token.AccessToken, RefreshToken: token.RefreshToken, AccountID: account}
				if err := db.MarkTokenErrorWithContext(ctx, token.ID, "stale 401", true, nil, store.TokenStateEventContext{StatusCode: &status, CredentialFence: &fence}); err != nil {
					t.Fatal(err)
				}
				latest, _ = db.GetToken(ctx, token.ID)
				if !latest.IsActive {
					t.Fatal("stale 401 disabled recovered credential")
				}
				var secret string
				if err := db.Pool().QueryRow(ctx, `select access_token from token_secrets where token_id=$1`, token.ID).Scan(&secret); err != nil || secret != fresh {
					t.Fatal("token_secrets not atomically updated")
				}
			} else if latest.IsActive || latest.AccessToken != token.AccessToken {
				t.Fatal("failed or conflicting recovery changed original credentials")
			}
			a.run401Recovery(ctx, token.ID)
			wantPosts := 1
			if mode == "manual_disabled" || mode == "other_plan" {
				wantPosts = 0
			}
			if posts != wantPosts {
				t.Fatalf("duplicate or ineligible submission: %d", posts)
			}
		})
	}
}
