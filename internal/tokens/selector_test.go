package tokens

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestQuotaAwareSelectorPrefersNonFree(t *testing.T) {
	free := "free"
	pro := "pro"
	rows := makeTokens(2)
	rows[0].PlanType = &free
	rows[1].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := QuotaAwareSelector{}.Select(context.Background(), manager.Snapshot(), Intent{}, 1, &cursor)
	if token == nil || token.Token.ID != 2 {
		t.Fatalf("selected token=%v reason=%s, want pro token", token, reason)
	}
}

func TestRequireNonFreeIsHardConstraintAcrossSelectionModes(t *testing.T) {
	free := " FREE "
	pro := "pro"
	rows := makeTokens(2)
	rows[0].PlanType = &pro
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = true
	rows[0].ShareStatus = "active"
	rows[1].PlanType = &free
	rows[1].OwnerUserID = 20
	rows[1].ShareEnabled = true
	rows[1].ShareStatus = "active"
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	tests := []struct {
		name   string
		intent Intent
	}{
		{name: "default", intent: Intent{RequireNonFree: true}},
		{name: "marketplace", intent: Intent{SelectionMode: "marketplace", RequireNonFree: true}},
		{name: "marketplace priced", intent: Intent{SelectionMode: "marketplace-priced", RequireNonFree: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cursor uint64
			token, reason := FillFirstSelector{}.Select(context.Background(), manager.Snapshot(), tt.intent, 1, &cursor)
			if token == nil || token.Token.ID != 1 {
				t.Fatalf("selected token=%v reason=%s, want non-free token 1", token, reason)
			}
		})
	}
}

func TestRequireNonFreeSkipsFreePromptAffinityAndTargetToken(t *testing.T) {
	pro := "pro"
	free := "free"
	rows := makeTokens(2)
	rows[0].PlanType = &pro
	rows[1].PlanType = &free
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	var cursor uint64
	token, reason := PromptAffinitySelector{
		PreferredTokenID: 2,
		Fallback:         FillFirstSelector{},
	}.Select(context.Background(), manager.Snapshot(), Intent{RequireNonFree: true}, 1, &cursor)
	if token == nil || token.Token.ID != 1 || reason == "prompt_affinity_preferred" {
		t.Fatalf("selected token=%v reason=%s, want non-free fallback token 1", token, reason)
	}

	claim, err := manager.Claim(context.Background(), Intent{TargetTokenID: 2, RequireNonFree: true})
	if err == nil || claim != nil {
		if claim != nil {
			claim.Release()
		}
		t.Fatalf("Claim returned claim=%v err=%v, want free target rejected", claim, err)
	}
}

func TestAlphaSearchSkipsPersonalAccessTokenWithoutRemovingItFromResponses(t *testing.T) {
	rows := makeTokens(2)
	rows[0].AccessToken = "at-personal-token"
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	searchClaim, err := manager.Claim(context.Background(), Intent{RequireAlphaSearch: true})
	if err != nil {
		t.Fatalf("alpha-search Claim returned error: %v", err)
	}
	if searchClaim.TokenID() != 2 {
		t.Fatalf("alpha-search selected token %d, want non-PAT token 2", searchClaim.TokenID())
	}
	searchClaim.Release()

	responsesClaim, err := manager.Claim(context.Background(), Intent{TargetTokenID: 1})
	if err != nil {
		t.Fatalf("responses Claim returned error: %v", err)
	}
	if responsesClaim.TokenID() != 1 {
		t.Fatalf("responses selected token %d, want PAT token 1", responsesClaim.TokenID())
	}
	responsesClaim.Release()
}

func TestRequireNonFreeAllowsUnknownPlan(t *testing.T) {
	rows := makeTokens(1)
	rows[0].PlanType = nil
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	claim, err := manager.Claim(context.Background(), Intent{RequireNonFree: true})
	if err != nil || claim == nil {
		t.Fatalf("Claim returned claim=%v err=%v, want unknown plan allowed", claim, err)
	}
	claim.Release()
}

func TestFastCapabilityIsHardConstraintAcrossSelectors(t *testing.T) {
	free := "free"
	k12 := "k12"
	pro := "pro"
	rows := makeTokens(3)
	for index := range rows {
		rows[index].OwnerUserID = 10
	}
	rows[0].PlanType = &free
	rows[1].PlanType = &k12
	rows[2].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.RefreshOwner(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	validUntil := time.Now().UTC().Add(time.Hour)
	manager.SetPlanModelCapabilities(10, free, "0.144.0", nil, validUntil)
	manager.SetPlanModelCapabilities(10, k12, "0.144.0", nil, validUntil)
	manager.SetPlanModelCapabilities(10, pro, "0.144.0", []string{"gpt-5.5"}, validUntil)

	base := Intent{OwnerUserID: 10}
	eligible, err := manager.FastEligibleTokenIDs(context.Background(), base, "gpt-5.5", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 1 {
		t.Fatalf("eligible = %#v, want only Pro token", eligible)
	}
	fastIntent := Intent{OwnerUserID: 10, RequireFast: true, FastEligibleTokens: eligible}
	claim, err := manager.Claim(context.Background(), fastIntent)
	if err != nil {
		t.Fatal(err)
	}
	if claim.TokenID() != 3 {
		t.Fatalf("selected token = %d, want Pro token 3", claim.TokenID())
	}
	claim.Release()

	var cursor uint64
	token, reason := PromptAffinitySelector{
		PreferredTokenID: 1,
		Fallback:         FillFirstSelector{},
	}.Select(context.Background(), manager.SnapshotForOwner(10), fastIntent, 1, &cursor)
	if token == nil || token.Token.ID != 3 || reason == "prompt_affinity_preferred" {
		t.Fatalf("selected token=%v reason=%s, want Pro fallback token 3", token, reason)
	}

	fastIntent.TargetTokenID = 2
	if claim, err := manager.Claim(context.Background(), fastIntent); err == nil || claim != nil {
		if claim != nil {
			claim.Release()
		}
		t.Fatalf("Fast target K12 returned claim=%v err=%v", claim, err)
	}
}

func TestFastCapabilityExpiresAndIsModelSpecific(t *testing.T) {
	pro := "pro"
	rows := makeTokens(1)
	rows[0].OwnerUserID = 10
	rows[0].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.RefreshOwner(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	manager.SetPlanModelCapabilities(10, pro, "0.144.0", []string{"gpt-5.5"}, time.Now().UTC().Add(time.Minute))

	base := Intent{OwnerUserID: 10}
	eligible, err := manager.FastEligibleTokenIDs(context.Background(), base, "gpt-5.4", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 0 {
		t.Fatalf("gpt-5.4 eligible = %#v, want none", eligible)
	}
	eligible, err = manager.FastEligibleTokenIDs(context.Background(), base, "gpt-5.5", time.Now().UTC().Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 0 {
		t.Fatalf("expired eligible = %#v, want none", eligible)
	}
}

func TestLatestPlanCapabilityReplacesOlderClientVersion(t *testing.T) {
	pro := "pro"
	rows := makeTokens(1)
	rows[0].OwnerUserID = 10
	rows[0].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.RefreshOwner(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	manager.SetPlanModelCapabilities(10, pro, "0.143.0", []string{"gpt-5.5"}, now.Add(time.Hour))
	manager.SetPlanModelCapabilities(10, pro, "0.144.0", nil, now.Add(time.Hour))
	manager.SetPlanModelCapabilities(10, pro, "0.143.9", []string{"gpt-5.5"}, now.Add(time.Hour))
	manager.SetPlanModelCapabilities(10, pro, "not-a-version", []string{"gpt-5.5"}, now.Add(time.Hour))

	eligible, err := manager.FastEligibleTokenIDs(context.Background(), Intent{OwnerUserID: 10}, "gpt-5.5", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(eligible) != 0 {
		t.Fatalf("eligible tokens = %#v, want newer no-Fast capability to revoke the old version", eligible)
	}
	if models := manager.FastModelsForOwner(10, "0.143.0", now); len(models) != 0 {
		t.Fatalf("old-version fallback models = %#v, want no stale cross-version capability", models)
	}
}

func TestFastEligibilityResolvesColdCapabilitiesOnDemand(t *testing.T) {
	pro := "pro"
	rows := makeTokens(1)
	rows[0].OwnerUserID = 10
	rows[0].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.RefreshOwner(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	resolverCalls := 0
	manager.SetFastCapabilityResolver(func(_ context.Context, ownerUserID int64) error {
		resolverCalls++
		manager.SetPlanModelCapabilities(ownerUserID, pro, "0.144.0", []string{"gpt-5.5"}, time.Now().UTC().Add(time.Hour))
		return nil
	})

	eligible, err := manager.FastEligibleTokenIDs(context.Background(), Intent{OwnerUserID: 10}, "gpt-5.5", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if resolverCalls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolverCalls)
	}
	if _, ok := eligible[1]; !ok || len(eligible) != 1 {
		t.Fatalf("eligible tokens = %#v, want token 1", eligible)
	}
}

func TestRequiredPlanFiltersCatalogProbeClaims(t *testing.T) {
	free := "free"
	pro := "pro"
	rows := makeTokens(2)
	rows[0].PlanType = &free
	rows[1].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	claim, err := manager.Claim(context.Background(), Intent{RequiredPlan: " PRO "})
	if err != nil {
		t.Fatal(err)
	}
	if claim.TokenID() != 2 {
		t.Fatalf("selected token = %d, want Pro token 2", claim.TokenID())
	}
	claim.Release()
}

func TestLatencyAwareSelectorPrefersLowestTTFT(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	manager.Snapshot().ByID[1].RecentTTFTMs.Store(200)
	manager.Snapshot().ByID[2].RecentTTFTMs.Store(10)
	var cursor uint64
	token, reason := LatencyAwareSelector{}.Select(context.Background(), manager.Snapshot(), Intent{}, 1, &cursor)
	if token == nil || token.Token.ID != 2 || reason != "snapshot_latency_aware" {
		t.Fatalf("selected token=%v reason=%s, want token 2 latency aware", token, reason)
	}
}

func TestPromptAffinitySelectorUsesPreferredToken(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := PromptAffinitySelector{PreferredTokenID: 2}.Select(context.Background(), manager.Snapshot(), Intent{}, 1, &cursor)
	if token == nil || token.Token.ID != 2 || reason != "prompt_affinity_preferred" {
		t.Fatalf("selected token=%v reason=%s, want preferred token", token, reason)
	}
}

func TestPromptAffinitySelectorSkipsExcludedPreferredToken(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := PromptAffinitySelector{
		PreferredTokenID: 2,
		Fallback:         FillFirstSelector{},
	}.Select(context.Background(), manager.Snapshot(), Intent{
		ExcludeTokenIDs: map[int64]struct{}{2: {}},
	}, 1, &cursor)
	if token == nil || token.Token.ID == 2 || reason == "prompt_affinity_preferred" {
		t.Fatalf("selected token=%v reason=%s, want fallback away from excluded preferred", token, reason)
	}
}

func TestMarketplaceSelectionIncludesOwnerPrivateAndSharedTokens(t *testing.T) {
	rows := makeTokens(3)
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = false
	rows[1].OwnerUserID = 20
	rows[1].ShareEnabled = false
	rows[2].OwnerUserID = 30
	rows[2].ShareEnabled = true
	rows[2].ShareStatus = "active"
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := FillFirstSelector{}.Select(context.Background(), manager.Snapshot(), Intent{
		OwnerUserID:   10,
		SelectionMode: "marketplace",
	}, 1, &cursor)
	if token == nil || token.Token.ID != 1 {
		t.Fatalf("selected token=%v reason=%s, want owner private token 1", token, reason)
	}

	cursor = 0
	token, reason = FillFirstSelector{}.Select(context.Background(), manager.Snapshot(), Intent{
		OwnerUserID:        10,
		SelectionMode:      "marketplace",
		ExcludeOwnerUserID: 10,
	}, 1, &cursor)
	if token == nil || token.Token.ID != 3 {
		t.Fatalf("selected token=%v reason=%s, want shared non-excluded token 3", token, reason)
	}
}

func TestMarketplacePriceSelectorUsesLowestPriceBucket(t *testing.T) {
	highPrice := 250
	lowPrice := 100
	rows := makeTokens(3)
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = true
	rows[0].ShareStatus = "active"
	rows[0].MarketplacePriceBPS = &highPrice
	rows[1].OwnerUserID = 20
	rows[1].ShareEnabled = true
	rows[1].ShareStatus = "active"
	rows[1].MarketplacePriceBPS = &lowPrice
	rows[2].OwnerUserID = 30
	rows[2].ShareEnabled = true
	rows[2].ShareStatus = "active"
	rows[2].MarketplacePriceBPS = &highPrice
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := MarketplacePriceSelector{Fallback: RoundRobinSelector{}}.Select(context.Background(), manager.Snapshot(), Intent{
		SelectionMode: "marketplace-priced",
	}, 1, &cursor)
	if token == nil || token.Token.ID != 2 || reason != "marketplace_price_snapshot_round_robin" {
		t.Fatalf("selected token=%v reason=%s, want low-price token 2", token, reason)
	}
}

func TestMarketplacePriceSelectorRequiresPricedMode(t *testing.T) {
	highPrice := 250
	lowPrice := 100
	rows := makeTokens(2)
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = true
	rows[0].ShareStatus = "active"
	rows[0].MarketplacePriceBPS = &highPrice
	rows[1].OwnerUserID = 20
	rows[1].ShareEnabled = true
	rows[1].ShareStatus = "active"
	rows[1].MarketplacePriceBPS = &lowPrice
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	var cursor uint64
	token, reason := MarketplacePriceSelector{Fallback: RoundRobinSelector{}}.Select(context.Background(), manager.Snapshot(), Intent{
		SelectionMode: "marketplace",
	}, 1, &cursor)
	if token == nil || strings.HasPrefix(reason, "marketplace_price_") {
		t.Fatalf("selected token=%v reason=%s, want fallback selector without price prefix", token, reason)
	}
}
