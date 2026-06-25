package proxy

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/yym68686/oaix/internal/protocol/openai"
)

func sanitizeReasoningContentBody(body []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return body, false
	}
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return body, false
	}
	if !removeReasoningContentFields(payload) {
		return body, false
	}
	encoded, err := openai.EncodeJSON(payload)
	if err != nil {
		return body, false
	}
	return encoded, true
}

func removeReasoningContentFields(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		if _, ok := typed["reasoning_content"]; ok {
			delete(typed, "reasoning_content")
			changed = true
		}
		for _, nested := range typed {
			if removeReasoningContentFields(nested) {
				changed = true
			}
		}
		return changed
	case []any:
		changed := false
		for _, nested := range typed {
			if removeReasoningContentFields(nested) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

func shouldRetryWithoutMalformedCompaction(intent RequestIntent, status int, err error, raw []byte) bool {
	if status != 400 || !isResponsesEndpoint(intent) {
		return false
	}
	message := strings.ToLower(string(raw))
	if err != nil {
		message += "\n" + strings.ToLower(err.Error())
	}
	return strings.Contains(message, "encrypted content") &&
		strings.Contains(message, "could not be verified") &&
		strings.Contains(message, "could not be decrypted or parsed")
}

func isResponsesEndpoint(intent RequestIntent) bool {
	switch intent.Endpoint {
	case "/v1/responses", "/v1/responses/compact":
		return true
	default:
		return intent.UpstreamEndpoint == "/v1/responses" || intent.UpstreamEndpoint == "/v1/responses/compact"
	}
}

func sanitizeMalformedCompactionEncryptedContentBody(body []byte) ([]byte, int, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return body, 0, false
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return body, 0, false
	}
	input, ok := payload["input"].([]any)
	if !ok || len(input) == 0 {
		return body, 0, false
	}
	filtered := input[:0]
	removed := 0
	for _, item := range input {
		if shouldRemoveMalformedCompactionItem(item) {
			removed++
			continue
		}
		filtered = append(filtered, item)
	}
	if removed == 0 {
		return body, 0, false
	}
	payload["input"] = filtered
	encoded, err := openai.EncodeJSON(payload)
	if err != nil {
		return body, 0, false
	}
	return encoded, removed, true
}

func shouldRemoveMalformedCompactionItem(item any) bool {
	object, ok := item.(map[string]any)
	if !ok || text(object["type"]) != "compaction" {
		return false
	}
	encryptedContent := strings.TrimSpace(text(object["encrypted_content"]))
	if encryptedContent == "" {
		return false
	}
	return !looksLikeOpenAIEncryptedContent(encryptedContent)
}

func looksLikeOpenAIEncryptedContent(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), "gAAAA")
}
