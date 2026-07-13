package tokens

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/affinity"
)

func TestClaimPromptAffinityReusesPrimaryLane(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	opts := PromptAffinityOptions{
		AffinityKey:           "cache-family-a",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	}

	first, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Result != "lane_created" || first == nil {
		t.Fatalf("unexpected first claim: claim=%v result=%+v", first, result)
	}
	firstID := first.TokenID()
	first.Release()

	second, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if result.Result != "primary_hit" || second.TokenID() != firstID {
		t.Fatalf("unexpected second claim: token=%d want=%d result=%+v", second.TokenID(), firstID, result)
	}
}

func TestClaimPromptAffinityCreatesSecondaryLaneWhenPrimaryBusy(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	opts := PromptAffinityOptions{
		AffinityKey:           "cache-family-b",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	}

	first, _, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	second, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if result.Result != "lane_created" || second.TokenID() == first.TokenID() {
		t.Fatalf("expected secondary lane, got token=%d first=%d result=%+v", second.TokenID(), first.TokenID(), result)
	}
}

func TestClaimPromptAffinityPreviousOwnerStrictWhenBusy(t *testing.T) {
	manager := NewManager(&fakeSource{tokens: makeTokens(2)}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	store.BindResponseOwner("resp-owner", affinity.ResponseOwnerBinding{TokenID: 1}, time.Hour)
	owner, err := manager.Claim(context.Background(), Intent{ExcludeTokenIDs: map[int64]struct{}{2: {}}})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()

	claim, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{}, PromptAffinityOptions{
		AffinityKey:           "cache-family-c",
		PreviousResponseID:    "resp-owner",
		MaxLanesPerKey:        3,
		PreviousStrict:        true,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	})
	if !errors.Is(err, ErrNoToken) || claim != nil || result.Result != "previous_owner_busy" {
		t.Fatalf("expected strict previous owner busy, claim=%v result=%+v err=%v", claim, result, err)
	}
}

func TestClaimPromptAffinitySkipsFreePrimaryWhenNonFreeRequired(t *testing.T) {
	free := "free"
	pro := "pro"
	rows := makeTokens(2)
	rows[0].PlanType = &free
	rows[1].PlanType = &pro
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	store.Put("cache-family-non-free", affinity.Lane{PrimaryTokenID: 1}, time.Hour)

	claim, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{RequireNonFree: true}, PromptAffinityOptions{
		AffinityKey:           "cache-family-non-free",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Release()
	if claim.TokenID() != 2 || result.Result == "primary_hit" {
		t.Fatalf("selected token=%d result=%+v, want non-free token 2 outside free primary", claim.TokenID(), result)
	}
}

func TestMarketplacePriceAffinityLocksInitialExecutionPrice(t *testing.T) {
	lowPrice := 50
	highPrice := 250
	rows := makeTokens(1)
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = true
	rows[0].ShareStatus = "active"
	rows[0].MarketplacePriceBPS = &lowPrice
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	opts := PromptAffinityOptions{
		AffinityKey:           "cache-family-price-lock",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	}

	first, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Result != "lane_created" || !first.MarketplacePriceLocked || first.MarketplacePriceBPS != lowPrice {
		t.Fatalf("first claim did not lock low execution price: claim=%+v result=%+v", first, result)
	}
	first.Release()

	source.tokens[0].MarketplacePriceBPS = &highPrice
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if result.Result != "primary_hit" || second.TokenID() != 1 {
		t.Fatalf("expected primary lane hit after reprice: token=%d result=%+v", second.TokenID(), result)
	}
	if second.Token.Token.MarketplacePriceBPS == nil || *second.Token.Token.MarketplacePriceBPS != highPrice {
		t.Fatalf("test setup did not refresh token current price: %#v", second.Token.Token.MarketplacePriceBPS)
	}
	if !second.MarketplacePriceLocked || second.MarketplacePriceBPS != lowPrice {
		t.Fatalf("sticky lane price changed after owner reprice: locked=%v price=%d want=%d", second.MarketplacePriceLocked, second.MarketplacePriceBPS, lowPrice)
	}
}

func TestMarketplacePricePreviousResponseCarriesExecutionPrice(t *testing.T) {
	lowPrice := 70
	highPrice := 250
	rows := makeTokens(1)
	rows[0].OwnerUserID = 10
	rows[0].ShareEnabled = true
	rows[0].ShareStatus = "active"
	rows[0].MarketplacePriceBPS = &lowPrice
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	opts := PromptAffinityOptions{
		AffinityKey:           "cache-family-response-price-lock",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
		ResponseTTL:           time.Hour,
	}
	first, _, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	manager.BindPromptResponseOwner(store, "resp-price-lock", first, time.Hour)
	first.Release()

	source.tokens[0].MarketplacePriceBPS = &highPrice
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, PromptAffinityOptions{
		PreviousResponseID:    "resp-price-lock",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		ResponseTTL:           time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if result.Result != "previous_owner_hit" || !second.MarketplacePriceLocked || second.MarketplacePriceBPS != lowPrice {
		t.Fatalf("previous response did not retain execution price: claim=%+v result=%+v", second, result)
	}
}

func TestMarketplacePriceCreatesNewLaneWithoutStealingPrimary(t *testing.T) {
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
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	opts := PromptAffinityOptions{
		AffinityKey:           "cache-family-price",
		MaxLanesPerKey:        3,
		GlobalFallbackEnabled: true,
		LaneTTL:               time.Hour,
	}

	first, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Result != "lane_created" || first.TokenID() != 2 {
		t.Fatalf("new lane selected token=%d result=%+v, want low-price token 2", first.TokenID(), result)
	}
	first.Release()

	store.Put(opts.AffinityKey, affinity.Lane{PrimaryTokenID: 1}, time.Hour)
	second, result, err := manager.ClaimPromptAffinity(context.Background(), store, Intent{SelectionMode: "marketplace-priced"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if result.Result != "primary_hit" || second.TokenID() != 1 {
		t.Fatalf("existing primary selected token=%d result=%+v, want high-price primary token 1", second.TokenID(), result)
	}
}
