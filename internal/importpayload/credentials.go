package importpayload

import "strings"

// Redacted credential markers are emitted by account exporters when a secret is
// intentionally omitted. They must never be treated as OAuth credentials.
func IsRedactedCredential(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "…", "[redacted]", "<redacted>", "(redacted)", "redacted", "***redacted***":
		return true
	}
	return normalized != "" && strings.Trim(normalized, ".*•") == ""
}

func NormalizeCredential(value any) string {
	token, ok := value.(string)
	if !ok {
		return ""
	}
	token = strings.TrimSpace(token)
	if token == "" || IsRedactedCredential(token) {
		return ""
	}
	return token
}

func String(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if token := NormalizeCredential(payload[key]); token != "" {
			return token
		}
	}
	return ""
}

// CleanCredentials removes redacted values while retaining all other payload
// fields. It returns the number of removed credential fields.
func CleanCredentials(payload map[string]any) (map[string]any, int) {
	cleaned := make(map[string]any, len(payload))
	for key, value := range payload {
		cleaned[key] = value
	}
	removed := 0
	for _, key := range []string{
		"refresh_token", "refreshToken", "access_token", "accessToken", "token",
	} {
		value, ok := cleaned[key].(string)
		if ok && IsRedactedCredential(value) {
			delete(cleaned, key)
			removed++
		}
	}
	return cleaned, removed
}
