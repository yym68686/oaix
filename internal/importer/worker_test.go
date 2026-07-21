package importer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"testing"

	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

type fakeRefreshClient struct{}

func (fakeRefreshClient) Refresh(ctx context.Context, refreshToken string) (oauth.RefreshResult, error) {
	return oauth.RefreshResult{
		AccessToken:  "access-" + refreshToken,
		RefreshToken: "rt-next",
		AccountID:    "acct-1",
		Email:        "user@example.com",
		PlanType:     "pro",
	}, nil
}

func TestWorkerValidateBatchAgentIdentityWithoutOAuthRefresh(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{Validator: TokenPayloadValidator{}}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{{
		ID: 9,
		Payload: map[string]any{
			"credentials": map[string]any{
				"auth_mode":         "agentIdentity",
				"agent_runtime_id":  "runtime-9",
				"agent_private_key": base64.StdEncoding.EncodeToString(der),
				"task_id":           "task-9",
				"account_id":        "workspace-1",
				"chatgpt_user_id":   "user-9",
				"email":             "agent-9@example.invalid",
			},
		},
	}})
	if len(updates) != 1 || updates[0].Status != string(ItemValidated) || updates[0].Action != "upsert_agent_identity" {
		t.Fatalf("agent identity update = %+v", updates)
	}
	if updates[0].ValidatedPayload["agent_runtime_id"] != "runtime-9" || updates[0].ValidatedPayload["agent_private_key"] == "" {
		t.Fatalf("agent identity payload = %#v", updates[0].ValidatedPayload)
	}
	if updates[0].ValidatedPayload["access_token"] != nil || updates[0].ValidatedPayload["refresh_token"] != nil {
		t.Fatalf("agent identity unexpectedly became OAuth credentials: %#v", updates[0].ValidatedPayload)
	}
}

type fakeRefreshClientWithClientID struct {
	clientID string
}

func (c *fakeRefreshClientWithClientID) Refresh(ctx context.Context, refreshToken string) (oauth.RefreshResult, error) {
	return oauth.RefreshResult{AccessToken: "default-" + refreshToken, RefreshToken: "rt-default"}, nil
}

func (c *fakeRefreshClientWithClientID) RefreshWithClientID(ctx context.Context, refreshToken string, clientID string) (oauth.RefreshResult, error) {
	c.clientID = clientID
	return oauth.RefreshResult{AccessToken: "client-" + refreshToken, RefreshToken: "rt-client"}, nil
}

func TestJobAndItemTransitions(t *testing.T) {
	if !CanTransitionJob(JobQueued, JobRunning) || CanTransitionJob(JobCompleted, JobRunning) {
		t.Fatal("unexpected job transition result")
	}
	if !CanTransitionItem(ItemValidating, ItemValidated) || CanTransitionItem(ItemFailed, ItemQueued) {
		t.Fatal("unexpected item transition result")
	}
}

func TestWorkerValidateBatchAccessTokens(t *testing.T) {
	worker := &Worker{MaxConcurrency: 2}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{
		{ID: 1, Payload: map[string]any{"access_token": "at-1"}},
		{ID: 2, Payload: map[string]any{"bad": "payload"}},
	})
	if len(updates) != 2 {
		t.Fatalf("updates len = %d", len(updates))
	}
	if updates[0].Status != string(ItemValidated) || updates[0].ValidatedPayload["access_token"] != "at-1" {
		t.Fatalf("unexpected first update: %+v", updates[0])
	}
	if updates[0].ValidatedPayload["auth_mode"] != store.CodexPersonalAccessTokenAuthMode ||
		updates[0].ValidatedPayload["openai_auth_mode"] != store.CodexPersonalAccessTokenLegacyAuthMode {
		t.Fatalf("personal access token mode was not normalized: %+v", updates[0].ValidatedPayload)
	}
	if updates[1].Status != string(ItemFailed) || updates[1].ErrorMessage == "" {
		t.Fatalf("unexpected second update: %+v", updates[1])
	}
	stats := worker.Stats()
	if stats.Claimed != 2 || stats.Validated != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestWorkerValidateBatchRefreshTokens(t *testing.T) {
	worker := &Worker{
		MaxConcurrency: 2,
		Validator: TokenPayloadValidator{
			Refresh: OAuthRefreshValidator{Client: fakeRefreshClient{}},
		},
	}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{
		{ID: 1, Payload: map[string]any{"account_id": "acct-input", "refresh_token": "rt-1"}},
	})
	if len(updates) != 1 {
		t.Fatalf("updates len = %d", len(updates))
	}
	if updates[0].Status != string(ItemValidated) {
		t.Fatalf("unexpected status: %+v", updates[0])
	}
	if updates[0].ValidatedPayload["access_token"] != "access-rt-1" {
		t.Fatalf("access token was not refreshed: %+v", updates[0].ValidatedPayload)
	}
	if updates[0].ValidatedPayload["refresh_token"] != "rt-next" ||
		updates[0].ValidatedPayload["account_id"] != "acct-1" ||
		updates[0].ValidatedPayload["plan_type"] != "pro" {
		t.Fatalf("refresh metadata was not preserved: %+v", updates[0].ValidatedPayload)
	}
}

func TestWorkerValidateBatchPersonalAccessTokenPreservesNestedMetadata(t *testing.T) {
	worker := &Worker{Validator: TokenPayloadValidator{}}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{{
		ID: 1,
		Payload: map[string]any{
			"name":     "pat@example.com",
			"platform": "openai",
			"type":     "oauth",
			"credentials": map[string]any{
				"access_token":               "at-personal-token",
				"refresh_token":              "",
				"chatgpt_account_id":         "workspace-123",
				"chatgpt_user_id":            "user-123",
				"chatgpt_plan_type":          "team",
				"email":                      "pat@example.com",
				"chatgpt_account_is_fedramp": false,
			},
		},
	}})
	if len(updates) != 1 || updates[0].Status != string(ItemValidated) {
		t.Fatalf("unexpected update: %+v", updates)
	}
	payload := updates[0].ValidatedPayload
	if payload["access_token"] != "at-personal-token" ||
		payload["chatgpt_account_id"] != "workspace-123" ||
		payload["chatgpt_user_id"] != "user-123" ||
		payload["plan_type"] != "team" ||
		payload["email"] != "pat@example.com" ||
		payload["auth_mode"] != store.CodexPersonalAccessTokenAuthMode ||
		payload["openai_auth_mode"] != store.CodexPersonalAccessTokenLegacyAuthMode {
		t.Fatalf("personal access token metadata was not preserved: %#v", payload)
	}
	if payload["chatgpt_account_is_fedramp"] != false {
		t.Fatalf("FedRAMP metadata was not preserved: %#v", payload)
	}
}

func TestWorkerValidateBatchNestedCredentialsRefreshTokens(t *testing.T) {
	refreshClient := &fakeRefreshClientWithClientID{}
	worker := &Worker{
		MaxConcurrency: 2,
		Validator: TokenPayloadValidator{
			Refresh: OAuthRefreshValidator{Client: refreshClient},
		},
	}
	updates := worker.ValidateBatch(context.Background(), []store.ImportItem{
		{
			ID: 1,
			Payload: map[string]any{
				"name":     "nested@example.com",
				"platform": "openai",
				"type":     "oauth",
				"credentials": map[string]any{
					"access_token":       "stale-access-token",
					"refresh_token":      "rt-nested",
					"chatgpt_account_id": "acct-input",
					"client_id":          "payload-client",
					"email":              "nested@example.com",
				},
			},
		},
	})
	if len(updates) != 1 {
		t.Fatalf("updates len = %d", len(updates))
	}
	if updates[0].Status != string(ItemValidated) {
		t.Fatalf("unexpected status: %+v", updates[0])
	}
	if updates[0].Action != "oauth_refresh" {
		t.Fatalf("nested credentials should prefer refresh token, got %+v", updates[0])
	}
	if refreshClient.clientID != "payload-client" {
		t.Fatalf("nested client_id was not used: %q", refreshClient.clientID)
	}
	if updates[0].ValidatedPayload["access_token"] != "client-rt-nested" ||
		updates[0].ValidatedPayload["refresh_token"] != "rt-client" ||
		updates[0].ValidatedPayload["client_id"] != "payload-client" {
		t.Fatalf("nested credentials were not refreshed: %+v", updates[0].ValidatedPayload)
	}
}
