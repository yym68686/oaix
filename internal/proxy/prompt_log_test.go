package proxy

import (
	"testing"

	"github.com/yym68686/oaix/internal/tokens"
)

func TestUpdatePromptTracePreservesUsageWithoutPromptContext(t *testing.T) {
	usage := extractUsageMetrics(map[string]any{
		"response": map[string]any{
			"usage": map[string]any{
				"input_tokens": 100,
				"input_tokens_details": map[string]any{
					"cache_write_tokens": 0,
					"cached_tokens":      80,
				},
				"output_tokens": 1,
			},
		},
	}, "gpt-5.6-sol")
	if usage == nil {
		t.Fatal("usage is nil")
	}

	trace := updatePromptTrace(nil, tokens.PromptAffinityResult{}, usage, "resp_test", 200)
	if trace == nil {
		t.Fatal("trace is nil")
	}
	usageTrace, ok := trace["usage"].(map[string]any)
	if !ok {
		t.Fatalf("usage trace missing: %#v", trace)
	}
	if got := usageTrace["cache_write_tokens_source"]; got != "response.usage.input_tokens_details.cache_write_tokens" {
		t.Fatalf("cache write source = %#v", got)
	}
	raw, ok := usageTrace["raw"].(map[string]any)
	if !ok {
		t.Fatalf("raw usage trace missing: %#v", usageTrace)
	}
	if reported, ok := raw["cache_write_reported"].(bool); !ok || !reported {
		t.Fatalf("cache write reported flag = %#v", raw["cache_write_reported"])
	}
	billing, ok := usageTrace["billing"].(map[string]any)
	if !ok {
		t.Fatalf("billing trace missing: %#v", usageTrace)
	}
	if got := billing["mode"]; got != usageBillingModeOpenAIPromptCache {
		t.Fatalf("billing mode = %#v", got)
	}
}
