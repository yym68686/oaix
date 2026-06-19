package importer

import (
	"context"
	"testing"

	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

type fakeRefreshClient struct{}

func (fakeRefreshClient) Refresh(ctx context.Context, refreshToken string) (oauth.RefreshResult, error) {
	return oauth.RefreshResult{
		AccessToken:  "at-" + refreshToken,
		RefreshToken: "rt-next",
		AccountID:    "acct-1",
		Email:        "user@example.com",
		PlanType:     "pro",
	}, nil
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
	if updates[0].ValidatedPayload["access_token"] != "at-rt-1" {
		t.Fatalf("access token was not refreshed: %+v", updates[0].ValidatedPayload)
	}
	if updates[0].ValidatedPayload["refresh_token"] != "rt-next" ||
		updates[0].ValidatedPayload["account_id"] != "acct-1" ||
		updates[0].ValidatedPayload["plan_type"] != "pro" {
		t.Fatalf("refresh metadata was not preserved: %+v", updates[0].ValidatedPayload)
	}
}
