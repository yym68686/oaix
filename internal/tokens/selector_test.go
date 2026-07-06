package tokens

import (
	"context"
	"strings"
	"testing"
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
