package modelaccess

import "testing"

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
