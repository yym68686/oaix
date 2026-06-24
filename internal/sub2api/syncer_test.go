package sub2api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func TestCreateAccountRequestCopiesCodexMetadata(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	raw := json.RawMessage(`{"chatgpt_user_id":"user-1","organization_id":"org-1"}`)
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:                 7,
		TargetGroupIDs:     []int64{10, 11},
		AccountConcurrency: 12,
		AccountPriority:    3,
		ProxyID:            99,
	}, store.Sub2APITokenCandidate{
		ID:           42,
		OwnerUserID:  9,
		Email:        "a@example.com",
		AccountID:    "acct-1",
		IDToken:      "id-token",
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		PlanType:     "pro",
		RawPayload:   raw,
		CreatedAt:    time.Now(),
	})

	if request.Platform != "openai" || request.Type != "oauth" {
		t.Fatalf("platform/type = %s/%s", request.Platform, request.Type)
	}
	if got := request.Credentials["client_id"]; got != "client-fixture" {
		t.Fatalf("client_id = %#v", got)
	}
	if got := request.Credentials["chatgpt_account_id"]; got != "acct-1" {
		t.Fatalf("chatgpt_account_id = %#v", got)
	}
	if got := request.Credentials["organization_id"]; got != "org-1" {
		t.Fatalf("organization_id = %#v", got)
	}
	if len(request.GroupIDs) != 2 || request.GroupIDs[0] != 10 || request.GroupIDs[1] != 11 {
		t.Fatalf("GroupIDs = %#v", request.GroupIDs)
	}
	if request.ConfirmMixedChannelRisk == nil || !*request.ConfirmMixedChannelRisk {
		t.Fatalf("ConfirmMixedChannelRisk = %#v", request.ConfirmMixedChannelRisk)
	}
	if request.Concurrency != 12 {
		t.Fatalf("Concurrency = %d, want 12", request.Concurrency)
	}
	if request.Priority != 3 {
		t.Fatalf("Priority = %d, want 3", request.Priority)
	}
	if request.ProxyID == nil || *request.ProxyID != 99 {
		t.Fatalf("ProxyID = %#v, want 99", request.ProxyID)
	}
	if request.Extra["oaix_token_id"] != int64(42) || request.Extra["oaix_sub2api_target_id"] != int64(7) {
		t.Fatalf("extra = %#v", request.Extra)
	}
	if _, ok := request.Extra["access_token"]; ok {
		t.Fatalf("extra leaked access token: %#v", request.Extra)
	}
}

func TestCreateAccountRequestDefaultsConcurrency(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:             7,
		TargetGroupIDs: []int64{10},
	}, store.Sub2APITokenCandidate{
		ID:           42,
		OwnerUserID:  9,
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		CreatedAt:    time.Now(),
	})

	if request.Concurrency != store.Sub2APIDefaultAccountConcurrency {
		t.Fatalf("Concurrency = %d, want %d", request.Concurrency, store.Sub2APIDefaultAccountConcurrency)
	}
	if request.ProxyID != nil {
		t.Fatalf("ProxyID = %#v, want nil", request.ProxyID)
	}
}

func TestTokenAvailableForSub2API(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)

	if !tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true}, now) {
		t.Fatal("active token without cooldown should be available")
	}
	if !tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true, CooldownUntil: &past}, now) {
		t.Fatal("expired cooldown should be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true, CooldownUntil: &future}, now) {
		t.Fatal("future cooldown should not be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 1, IsActive: false}, now) {
		t.Fatal("inactive token should not be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 0, IsActive: true}, now) {
		t.Fatal("token without id should not be available")
	}
}
