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
	store.BindResponseOwner("resp-owner", 1, time.Hour)
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
