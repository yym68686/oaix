package modelaccess

import (
	"slices"
	"strings"
	"testing"
)

func TestGPT6SolAndLunaDefaultAccessForEveryPlan(t *testing.T) {
	for _, plan := range []string{"free", " CHATGPT_FREE ", "go", "plus", "team", "business", "pro", "chatgpt_pro", "prolite", "promax", "enterprise", "edu", "k12", "unknown", "future-plan", ""} {
		t.Run(plan, func(t *testing.T) {
			for _, model := range []string{"gpt-6-sol", "gpt-6-luna"} {
				if !DefaultAllows(plan, model) || !DefaultAllows(plan, strings.ToUpper(model)+"-2026-09-22") {
					t.Fatalf("%s must default to enabled for %q", model, plan)
				}
				if !slices.Contains(DefaultModels(plan, ModelIDs()), model) || !slices.Contains(TextModelIDs(), model) {
					t.Fatalf("%s missing from default text catalog for %q", model, plan)
				}
				if slices.Contains(DefaultModels(plan, []string{"gpt-5.5"}), model) {
					t.Fatal("default policy must not invent upstream capabilities")
				}
			}
		})
	}
}

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
