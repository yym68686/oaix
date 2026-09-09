package modelaccess

import (
	"slices"
	"testing"
)

func TestAstraDefaultAccessByPlan(t *testing.T) {
	for _, plan := range []string{"free", " CHATGPT_FREE ", "plus", "team", "pro", "chatgpt_pro", "enterprise", "k12", "unknown", ""} {
		t.Run(plan, func(t *testing.T) {
			want := plan != "free" && plan != " CHATGPT_FREE "
			for _, model := range []string{"gpt-6-astra", "GPT-6-ASTRA-2026-09-03"} {
				if got := DefaultAllows(plan, model); got != want {
					t.Fatalf("DefaultAllows(%q, %q) = %v, want %v", plan, model, got, want)
				}
			}
			if got := slices.Contains(DefaultModels(plan, ModelIDs()), "gpt-6-astra"); got != want {
				t.Fatalf("static default Astra access = %v, want %v", got, want)
			}
			if got := slices.Contains(DefaultModels(plan, []string{"gpt-5.5"}), "gpt-6-astra"); got {
				t.Fatal("default policy must not invent an unavailable model")
			}
		})
	}
}

func TestDefaultFreePolicyMatchesLegacyRestriction(t *testing.T) {
	if DefaultAllows("free", "gpt-5.4") {
		t.Fatal("free plan should not allow gpt-5.4 by default")
	}
	if DefaultAllows("chatgpt_free", "gpt-image-2-2026-01-01") {
		t.Fatal("free plan should not allow versioned image model by default")
	}
	if !DefaultAllows("free", "gpt-5.6-sol") || !DefaultAllows("pro", "gpt-5.4") {
		t.Fatal("non-restricted defaults were denied")
	}
}

func TestNormalizeModelsDeduplicatesAndMatchesAliases(t *testing.T) {
	models, err := NormalizeModels([]string{" GPT-5.5 ", "gpt-5.5", "gpt-5.4"})
	if err != nil || len(models) != 2 || !Matches(models, "gpt-5.5-2026-01-01") {
		t.Fatalf("models=%#v err=%v", models, err)
	}
}
