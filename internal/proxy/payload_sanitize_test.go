package proxy

import (
	"encoding/json"
	"testing"
)

func TestSanitizeReasoningContentBodyRecursive(t *testing.T) {
	body := []byte(`{
		"model": "gpt-5.5",
		"reasoning_content": "top-level hidden chain",
		"input": [
			{
				"role": "assistant",
				"content": [
					{
						"type": "output_text",
						"text": "visible",
						"reasoning_content": "nested hidden chain"
					}
				]
			},
			{
				"type": "reasoning",
				"summary": "visible summary",
				"reasoning_content": "reasoning hidden chain"
			}
		],
		"metadata": {
			"keep": true,
			"reasoning_content": "metadata hidden chain"
		}
	}`)
	sanitized, changed := sanitizeReasoningContentBody(body)
	if !changed {
		t.Fatal("expected reasoning_content removal")
	}
	var payload any
	if err := json.Unmarshal(sanitized, &payload); err != nil {
		t.Fatal(err)
	}
	if containsReasoningContent(payload) {
		t.Fatalf("sanitized payload still contains reasoning_content: %s", sanitized)
	}
	mapping := payload.(map[string]any)
	if mapping["model"] != "gpt-5.5" {
		t.Fatalf("model was changed: %v", mapping["model"])
	}
	metadata := mapping["metadata"].(map[string]any)
	if metadata["keep"] != true {
		t.Fatalf("metadata keep field was changed: %v", metadata)
	}
}

func TestSanitizeReasoningContentBodyNoChangeKeepsBytes(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":"hello"}`)
	sanitized, changed := sanitizeReasoningContentBody(body)
	if changed {
		t.Fatal("did not expect sanitizer change")
	}
	if string(sanitized) != string(body) {
		t.Fatalf("body changed without reasoning_content: %s", sanitized)
	}
}

func containsReasoningContent(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if _, ok := typed["reasoning_content"]; ok {
			return true
		}
		for _, nested := range typed {
			if containsReasoningContent(nested) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsReasoningContent(nested) {
				return true
			}
		}
	}
	return false
}
