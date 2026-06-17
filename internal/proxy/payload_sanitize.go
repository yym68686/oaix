package proxy

import (
	"bytes"
	"encoding/json"

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
