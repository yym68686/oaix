package upstreamerror

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	inactiveWorkspaceMemberCode    = "biscuit_baker_service_auth_credential_error_status"
	inactiveWorkspaceMemberMessage = "Personal access token owner is not an active member of the selected workspace."
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

// IsInactiveWorkspaceMember reports whether an upstream HTTP response
// explicitly says that the personal access token owner is no longer an active
// member of the selected workspace. Both structured fields are required so an
// unrelated biscuit baker credential error cannot permanently disable a token.
func IsInactiveWorkspaceMember(status int, body []byte) bool {
	if status != http.StatusForbidden || len(body) == 0 {
		return false
	}
	var payload struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(payload.Error.Code), inactiveWorkspaceMemberCode) &&
		strings.EqualFold(strings.TrimSpace(payload.Error.Message), inactiveWorkspaceMemberMessage)
}
