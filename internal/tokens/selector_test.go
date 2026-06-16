package tokens

import (
	"context"
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
