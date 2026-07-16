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

func TestClaimStrictAffinityPinsOneTokenAndFailsClosedWhenBusy(t *testing.T) {
	rows := makeTokens(2)
	accountA := "account-a"
	accountB := "account-b"
	rows[0].AccountID = &accountA
	rows[1].AccountID = &accountB
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	first, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-session", 0, time.Hour)
	if err != nil || first == nil || result.Result != "strict_affinity_created" {
		t.Fatalf("unexpected first strict claim: claim=%v result=%+v err=%v", first, result, err)
	}
	firstID := first.TokenID()

	busy, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-session", 0, time.Hour)
	if !errors.Is(err, ErrNoToken) || busy != nil || result.Result != "strict_affinity_busy" {
		t.Fatalf("strict affinity switched while busy: claim=%v result=%+v err=%v", busy, result, err)
	}
	first.Release()

	again, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-session", 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Release()
	if again.TokenID() != firstID || result.Result != "strict_affinity_hit" {
		t.Fatalf("strict affinity token=%d want=%d result=%+v", again.TokenID(), firstID, result)
	}
}

func TestClaimStrictAffinityFailsClosedAfterAccountIdentityChanges(t *testing.T) {
	rows := makeTokens(1)
	accountA := "account-a"
	rows[0].AccountID = &accountA
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	first, _, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-account-session", 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()

	accountB := "account-b"
	source.tokens[0].AccountID = &accountB
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	claim, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-account-session", 0, time.Hour)
	if !errors.Is(err, ErrNoToken) || claim != nil || result.Result != "strict_identity_mismatch" {
		t.Fatalf("account replacement did not fail closed: claim=%v result=%+v err=%v", claim, result, err)
	}
}

func TestClaimStrictAffinityValidatesIdentityWhenCASReturnsSameToken(t *testing.T) {
	rows := makeTokens(1)
	accountB := "account-b"
	rows[0].AccountID = &accountB
	manager := NewManager(&fakeSource{tokens: rows}, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	base := affinity.NewMemoryStore()
	if _, ok := base.BindPrimaryIfAbsent(
		"search-cas-session",
		1,
		strictIdentityHash("account", "account-a"),
		time.Hour,
		affinity.PolicyVersionStrictToken,
	); !ok {
		t.Fatal("failed to seed strict affinity")
	}
	store := &firstMissAffinityStore{Store: base}

	claim, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-cas-session", 0, time.Hour)
	if !errors.Is(err, ErrNoToken) || claim != nil || result.Result != "strict_identity_mismatch" {
		t.Fatalf("same-token CAS identity mismatch did not fail closed: claim=%v result=%+v err=%v", claim, result, err)
	}
}

func TestClaimStrictAffinityDoesNotFallbackToEmailWhenAccountAppears(t *testing.T) {
	rows := makeTokens(1)
	email := "same@example.com"
	rows[0].Email = &email
	source := &fakeSource{tokens: rows}
	manager := NewManager(source, nil, testMaxAge, testRefreshInterval, 1)
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := affinity.NewMemoryStore()
	first, _, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-email-session", 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()

	accountID := "new-account"
	source.tokens[0].AccountID = &accountID
	if err := manager.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	claim, result, err := manager.ClaimStrictAffinity(context.Background(), store, Intent{}, "search-email-session", 0, time.Hour)
	if !errors.Is(err, ErrNoToken) || claim != nil || result.Result != "strict_identity_mismatch" {
		t.Fatalf("email fallback accepted a new account: claim=%v result=%+v err=%v", claim, result, err)
	}
}

type firstMissAffinityStore struct {
	affinity.Store
	missed bool
}

func (s *firstMissAffinityStore) Get(promptKey string) (affinity.Lane, bool) {
	if !s.missed {
		s.missed = true
		return affinity.Lane{}, false
	}
	return s.Store.Get(promptKey)
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
