package upstreamerror

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	inactiveWorkspaceMemberCode       = "biscuit_baker_service_auth_credential_error_status"
	inactiveWorkspaceMemberMessage    = "Personal access token owner is not an active member of the selected workspace."
	inactiveAccessTokenOwnerMessage   = "Personal access token owner is inactive."
	inactivePersonalTokenMessage      = "Personal access token is inactive."
	deletedAgentRuntimeCode           = "biscuit_baker_service_agent_error_status"
	deletedAgentRuntimeMessage        = "Agent runtime has been deleted."
	expiredAuthenticationTokenCode    = "token_expired"
	expiredAuthenticationTokenMessage = "Provided authentication token is expired. Please try signing in again."
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

// IsTokenExpired reports whether an upstream HTTP response contains the
// explicit token_expired protocol signal and its permanent authentication
// message. Both fields are required so an unrelated 401 cannot disable a
// refreshable token.
func IsTokenExpired(status int, body []byte) bool {
	if status != http.StatusUnauthorized || len(body) == 0 {
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
	return strings.EqualFold(strings.TrimSpace(payload.Error.Code), expiredAuthenticationTokenCode) &&
		strings.EqualFold(strings.TrimSpace(payload.Error.Message), expiredAuthenticationTokenMessage)
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
	if !strings.EqualFold(strings.TrimSpace(payload.Error.Code), inactiveWorkspaceMemberCode) {
		return false
	}
	message := strings.TrimSpace(payload.Error.Message)
	return strings.EqualFold(message, inactiveWorkspaceMemberMessage) ||
		strings.EqualFold(message, inactiveAccessTokenOwnerMessage)
}

// IsInactivePersonalAccessToken reports whether an upstream HTTP response
// explicitly says that the personal access token itself is inactive. Both
// structured fields are required so another credential error cannot
// permanently disable an otherwise usable token.
func IsInactivePersonalAccessToken(status int, body []byte) bool {
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
		strings.EqualFold(strings.TrimSpace(payload.Error.Message), inactivePersonalTokenMessage)
}

// IsAgentRuntimeDeleted reports whether an upstream HTTP response explicitly
// says that the Agent runtime backing the credential has been deleted. Both
// structured fields are required so another biscuit baker agent error cannot
// permanently disable an otherwise usable token.
func IsAgentRuntimeDeleted(status int, body []byte) bool {
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
	return strings.EqualFold(strings.TrimSpace(payload.Error.Code), deletedAgentRuntimeCode) &&
		strings.EqualFold(strings.TrimSpace(payload.Error.Message), deletedAgentRuntimeMessage)
}
