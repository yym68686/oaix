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

type encryptedContentRemoval struct {
	Index int
	Type  string
}

func rejectedEncryptedContentMarker(intent RequestIntent, status int, err error, raw []byte) (string, bool) {
	if status != 400 || !isResponsesEndpoint(intent) {
		return "", false
	}
	message := string(raw)
	if err != nil {
		message += "\n" + err.Error()
	}
	lower := strings.ToLower(message)
	if !(strings.Contains(lower, "encrypted content") &&
		strings.Contains(lower, "could not be verified") &&
		strings.Contains(lower, "could not be decrypted or parsed")) {
		return "", false
	}
	marker := extractRejectedEncryptedContentMarker(message)
	return marker, marker != ""
}

func extractRejectedEncryptedContentMarker(message string) string {
	const prefix = "the encrypted content "
	const suffix = " could not be verified"
	lower := strings.ToLower(message)
	searchStart := 0
	for {
		prefixIndex := strings.Index(lower[searchStart:], prefix)
		if prefixIndex < 0 {
			return ""
		}
		start := searchStart + prefixIndex + len(prefix)
		suffixIndex := strings.Index(lower[start:], suffix)
		if suffixIndex >= 0 {
			return strings.TrimSpace(message[start : start+suffixIndex])
		}
		searchStart = start
	}
}

func isResponsesEndpoint(intent RequestIntent) bool {
	switch intent.Endpoint {
	case "/v1/responses", "/v1/responses/compact":
		return true
	default:
		return intent.UpstreamEndpoint == "/v1/responses" || intent.UpstreamEndpoint == "/v1/responses/compact"
	}
}

func sanitizeRejectedEncryptedContentBody(body []byte, marker string) ([]byte, encryptedContentRemoval, bool) {
	removal := encryptedContentRemoval{Index: -1}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return body, removal, false
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return body, removal, false
	}
	input, ok := payload["input"].([]any)
	if !ok || len(input) == 0 {
		return body, removal, false
	}
	matchIndex := -1
	matchType := ""
	for index, item := range input {
		if removeType, ok := rejectedEncryptedContentItemType(item, marker); ok {
			if matchIndex >= 0 {
				return body, removal, false
			}
			matchIndex = index
			matchType = removeType
		}
	}
	if matchIndex < 0 {
		return body, removal, false
	}
	filtered := make([]any, 0, len(input)-1)
	filtered = append(filtered, input[:matchIndex]...)
	filtered = append(filtered, input[matchIndex+1:]...)
	payload["input"] = filtered
	encoded, err := openai.EncodeJSON(payload)
	if err != nil {
		return body, removal, false
	}
	return encoded, encryptedContentRemoval{Index: matchIndex, Type: matchType}, true
}

func rejectedEncryptedContentItemType(item any, marker string) (string, bool) {
	object, ok := item.(map[string]any)
	if !ok {
		return "", false
	}
	itemType := text(object["type"])
	if itemType != "reasoning" && itemType != "compaction" {
		return "", false
	}
	encryptedContent := strings.TrimSpace(text(object["encrypted_content"]))
	if encryptedContent == "" || !encryptedContentMatchesMarker(encryptedContent, marker) {
		return "", false
	}
	return itemType, true
}

func encryptedContentMatchesMarker(encryptedContent string, marker string) bool {
	encryptedContent = strings.TrimSpace(encryptedContent)
	marker = strings.TrimSpace(marker)
	if encryptedContent == "" || marker == "" {
		return false
	}
	if encryptedContent == marker {
		return true
	}
	prefix, suffix, ok := strings.Cut(marker, "...")
	if !ok {
		return false
	}
	prefix = strings.TrimSpace(prefix)
	suffix = strings.TrimSpace(suffix)
	if prefix != "" && !strings.HasPrefix(encryptedContent, prefix) {
		return false
	}
	if suffix != "" && !strings.HasSuffix(encryptedContent, suffix) {
		return false
	}
	return prefix != "" || suffix != ""
}
