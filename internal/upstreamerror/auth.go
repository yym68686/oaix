package upstreamerror

import (
	"encoding/json"
	"net/http"
	"strings"
)

// IsTokenInvalidated reports whether an upstream HTTP response contains the
// explicit, permanent token_invalidated protocol signal. Free-form messages
// are intentionally ignored so localized or unrelated 401 errors cannot
// deactivate a token.
func IsTokenInvalidated(status int, body []byte) bool {
	if status != http.StatusUnauthorized || len(body) == 0 {
		return false
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(payload.Error.Code), "token_invalidated")
}
