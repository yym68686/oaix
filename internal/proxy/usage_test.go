package proxy

import (
	"math"
	"testing"
)

func TestExtractUsageMetricsGPT56PromptCacheBilling(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"usage": map[string]any{
				"input_tokens": 100,
				"input_tokens_details": map[string]any{
					"cache_write_tokens": 20,
					"cached_tokens":      30,
				},
				"output_tokens": 10,
				"total_tokens":  110,
			},
		},
	}, "gpt-5.6-luna")

	if usage == nil {
		t.Fatal("usage is nil")
	}
	if usage.InputTokens != 100 || usage.NonCachedInputTokens != 50 || usage.CacheWriteInputTokens != 20 || usage.CachedInputTokens != 30 {
		t.Fatalf("unexpected token split: %+v", usage)
	}
	if usage.BillingMode != usageBillingModeOpenAIPromptCache || usage.PricingModel != "gpt-5.6-luna" {
		t.Fatalf("unexpected pricing metadata: %+v", usage)
	}
	if usage.CacheWriteTokensSource != "response.usage.input_tokens_details.cache_write_tokens" {
		t.Fatalf("cache write source = %q", usage.CacheWriteTokensSource)
	}
	if usage.CachedInputTokensSource != "response.usage.input_tokens_details.cached_tokens" {
		t.Fatalf("cached source = %q", usage.CachedInputTokensSource)
	}
	if usage.InputPricePerMillionUSD != 1 || usage.OutputPricePerMillionUSD != 6 || usage.CacheWritePricePerMillionUSD == nil || *usage.CacheWritePricePerMillionUSD != 1.25 {
		t.Fatalf("unexpected official pricing metadata: %+v", usage)
	}
	assertCost(t, usage.EstimatedCostUSD, 0.000138)
}

func TestGPT56PricingFamilies(t *testing.T) {
	tests := []struct {
		model      string
		pricing    string
		input      float64
		cacheWrite float64
		cached     float64
		output     float64
	}{
		{model: "gpt-5.6-sol", pricing: "gpt-5.6-sol", input: 5, cacheWrite: 6.25, cached: 0.5, output: 30},
		{model: "gpt-5.6-terra-2026-07-01", pricing: "gpt-5.6-terra", input: 2.5, cacheWrite: 3.125, cached: 0.25, output: 15},
		{model: "gpt-5.6-luna", pricing: "gpt-5.6-luna", input: 1, cacheWrite: 1.25, cached: 0.1, output: 6},
	}
	for _, test := range tests {
		t.Run(test.model, func(t *testing.T) {
			pricing, ok := pricingForModel(test.model)
			if !ok {
				t.Fatalf("pricing not found for %q", test.model)
			}
			if pricing.name != test.pricing || pricing.billingMode != usageBillingModeOpenAIPromptCache {
				t.Fatalf("unexpected pricing: %+v", pricing)
			}
			if pricing.cacheWrite == nil || pricing.cached == nil {
				t.Fatalf("missing cache pricing: %+v", pricing)
			}
			if pricing.input != test.input || *pricing.cacheWrite != test.cacheWrite || *pricing.cached != test.cached || pricing.output != test.output {
				t.Fatalf("unexpected rates: %+v", pricing)
			}
		})
	}
}

func TestUnknownGPT56VariantIsNotGuessed(t *testing.T) {
	if pricing, ok := pricingForModel("gpt-5.6-unknown"); ok {
		t.Fatalf("unexpected pricing for unknown GPT-5.6 variant: %+v", pricing)
	}
}

func TestLegacyModelBillingIgnoresCacheWriteField(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"usage": map[string]any{
			"input_tokens": 100,
			"input_tokens_details": map[string]any{
				"cache_write_tokens": 20,
				"cached_tokens":      30,
			},
			"output_tokens": 10,
			"total_tokens":  110,
		},
	}, "gpt-5.5")

	if usage == nil {
		t.Fatal("usage is nil")
	}
	if usage.NonCachedInputTokens != 70 {
		t.Fatalf("legacy non-cached input = %d, want 70", usage.NonCachedInputTokens)
	}
	if usage.BillingMode != usageBillingModeOpenAICachedInput {
		t.Fatalf("legacy billing mode = %q", usage.BillingMode)
	}
	assertCost(t, usage.EstimatedCostUSD, 0.000665)
}

func TestExtractUsageMetricsSupportsTopLevelCacheFields(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"usage": map[string]any{
			"input_tokens":                100,
			"cache_creation_input_tokens": 20,
			"cache_read_input_tokens":     30,
			"output_tokens":               10,
		},
	}, "gpt-5.6-terra")

	if usage == nil {
		t.Fatal("usage is nil")
	}
	if usage.CacheWriteInputTokens != 20 || usage.CachedInputTokens != 30 || usage.TotalTokens != 110 {
		t.Fatalf("unexpected fallback usage: %+v", usage)
	}
	if usage.CacheWriteTokensSource != "usage.cache_creation_input_tokens" || usage.CachedInputTokensSource != "usage.cache_read_input_tokens" {
		t.Fatalf("unexpected fallback sources: %+v", usage)
	}
}

func TestGPT56MissingCacheWriteFieldIsObservableButNotInferred(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"usage": map[string]any{
			"input_tokens": 100,
			"input_tokens_details": map[string]any{
				"cached_tokens": 80,
			},
			"output_tokens": 1,
		},
	}, "gpt-5.6-sol")

	if usage == nil {
		t.Fatal("usage is nil")
	}
	if usage.CacheWriteInputTokens != 0 || usage.CacheWriteTokensSource != "" {
		t.Fatalf("cache write value was inferred: %+v", usage)
	}
	if !containsString(usage.Anomalies, "gpt56_cache_write_tokens_missing") {
		t.Fatalf("missing anomaly: %+v", usage.Anomalies)
	}
	trace := usage.Trace()
	raw, _ := trace["raw"].(map[string]any)
	if reported, _ := raw["cache_write_reported"].(bool); reported {
		t.Fatalf("trace claims cache write was reported: %#v", trace)
	}
}

func TestUsageComponentsAreClampedWithoutChangingRawTokens(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"usage": map[string]any{
			"input_tokens": 10,
			"input_tokens_details": map[string]any{
				"cache_write_tokens": 8,
				"cached_tokens":      7,
			},
			"output_tokens": 1,
		},
	}, "gpt-5.6-luna")

	if usage == nil {
		t.Fatal("usage is nil")
	}
	if usage.InputTokens != 10 || usage.NonCachedInputTokens != 0 {
		t.Fatalf("raw input was changed: %+v", usage)
	}
	if !containsString(usage.Anomalies, "cache_components_exceed_input_tokens") {
		t.Fatalf("missing component anomaly: %+v", usage.Anomalies)
	}
}

func assertCost(t *testing.T, actual *float64, expected float64) {
	t.Helper()
	if actual == nil {
		t.Fatalf("cost is nil, want %.8f", expected)
	}
	if math.Abs(*actual-expected) > 0.000000001 {
		t.Fatalf("cost = %.8f, want %.8f", *actual, expected)
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
