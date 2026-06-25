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

func TestSanitizeMalformedCompactionEncryptedContentBody(t *testing.T) {
	body := []byte(`{
		"model": "gpt-5.5",
		"input": [
			{"type": "reasoning", "encrypted_content": "plain reasoning is not touched"},
			{"type": "compaction", "encrypted_content": "plain handoff text is not encrypted"},
			{"type": "compaction", "encrypted_content": "gAAAAABvalid-encrypted-content"},
			{"role": "user", "content": "say test"}
		]
	}`)
	sanitized, removed, changed := sanitizeMalformedCompactionEncryptedContentBody(body)
	if !changed || removed != 1 {
		t.Fatalf("changed=%v removed=%d, want one malformed compaction removed", changed, removed)
	}
	var payload map[string]any
	if err := json.Unmarshal(sanitized, &payload); err != nil {
		t.Fatal(err)
	}
	input := payload["input"].([]any)
	if len(input) != 3 {
		t.Fatalf("input length = %d, want 3: %v", len(input), input)
	}
	for _, item := range input {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if object["type"] == "compaction" && object["encrypted_content"] == "plain handoff text is not encrypted" {
			t.Fatalf("malformed compaction was not removed: %v", input)
		}
	}
	if input[0].(map[string]any)["type"] != "reasoning" {
		t.Fatalf("reasoning item should be preserved: %v", input)
	}
}

func TestShouldRetryWithoutMalformedCompactionRequiresSpecific400(t *testing.T) {
	intent := RequestIntent{Endpoint: "/v1/responses"}
	raw := []byte(`event: error
data: {"error":{"message":"The encrypted content plain handoff text could not be verified. Reason: Encrypted content could not be decrypted or parsed.","status":400}}

`)
	if !shouldRetryWithoutMalformedCompaction(intent, 400, nil, raw) {
		t.Fatal("expected exact encrypted content parse failure to trigger retry")
	}
	if shouldRetryWithoutMalformedCompaction(intent, 400, nil, []byte(`{"error":{"message":"unsupported content type"}}`)) {
		t.Fatal("unsupported content type must not trigger compaction retry")
	}
	if shouldRetryWithoutMalformedCompaction(RequestIntent{Endpoint: "/v1/chat/completions"}, 400, nil, raw) {
		t.Fatal("chat completions must not trigger compaction retry")
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
