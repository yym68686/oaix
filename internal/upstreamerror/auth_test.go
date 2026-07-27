package upstreamerror

import (
	"net/http"
	"testing"
)

func TestIsTokenInvalidated(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{
			name:   "explicit invalidated token",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Your authentication token has been invalidated. Please try signing in again.","type":"invalid_request_error","code":"token_invalidated","param":null},"status":401}`,
			want:   true,
		},
		{
			name:   "code comparison is case insensitive",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":" TOKEN_INVALIDATED "}}`,
			want:   true,
		},
		{
			name:   "message alone is not a protocol signal",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Your authentication token has been invalidated. Please try signing in again."}}`,
		},
		{
			name:   "ordinary unauthorized response",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"no_matching_rule","message":"Unauthorized"}}`,
		},
		{
			name:   "same code under a different status",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"token_invalidated"}}`,
		},
		{
			name:   "top level code is not accepted",
			status: http.StatusUnauthorized,
			body:   `{"code":"token_invalidated"}`,
		},
		{
			name:   "malformed response",
			status: http.StatusUnauthorized,
			body:   `{"error":`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsTokenInvalidated(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsTokenInvalidated() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestIsTokenExpired(t *testing.T) {
	const exact = `{"error":{"message":"Provided authentication token is expired. Please try signing in again.","type":null,"code":"token_expired","param":null},"status":401}`
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "exact expired authentication token", status: http.StatusUnauthorized, body: exact, want: true},
		{
			name:   "structured fields are case insensitive and trimmed",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":" TOKEN_EXPIRED ","message":" provided AUTHENTICATION token is EXPIRED. please TRY signing in AGAIN. "}}`,
			want:   true,
		},
		{name: "same payload under another status", status: http.StatusForbidden, body: exact},
		{
			name:   "code without exact message",
			status: http.StatusUnauthorized,
			body:   `{"error":{"code":"token_expired","message":"Session expired."}}`,
		},
		{
			name:   "message without protocol code",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Provided authentication token is expired. Please try signing in again."}}`,
		},
		{name: "malformed response", status: http.StatusUnauthorized, body: `{"error":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsTokenExpired(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsTokenExpired() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestIsInactiveWorkspaceMember(t *testing.T) {
	const exact = `{"error":{"message":"Personal access token owner is not an active member of the selected workspace.","type":null,"code":"biscuit_baker_service_auth_credential_error_status","param":null},"status":403}`
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "exact permanent membership failure", status: http.StatusForbidden, body: exact, want: true},
		{
			name:   "inactive personal access token owner",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Personal access token owner is inactive."},"status":403}`,
			want:   true,
		},
		{
			name:   "structured fields are case insensitive and trimmed",
			status: http.StatusForbidden,
			body:   `{"error":{"code":" BISCUIT_BAKER_SERVICE_AUTH_CREDENTIAL_ERROR_STATUS ","message":" personal ACCESS token OWNER is not an ACTIVE member of the selected WORKSPACE. "}}`,
			want:   true,
		},
		{name: "same payload under another status", status: http.StatusUnauthorized, body: exact},
		{
			name:   "code without exact membership message",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"biscuit_baker_service_auth_credential_error_status","message":"Credential rejected."}}`,
		},
		{
			name:   "message without protocol code",
			status: http.StatusForbidden,
			body:   `{"error":{"message":"Personal access token owner is not an active member of the selected workspace."}}`,
		},
		{name: "malformed response", status: http.StatusForbidden, body: `{"error":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsInactiveWorkspaceMember(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsInactiveWorkspaceMember() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestIsAgentRuntimeDeleted(t *testing.T) {
	const exact = `{"error":{"message":"Agent runtime has been deleted.","type":null,"code":"biscuit_baker_service_agent_error_status","param":null},"status":403}`
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "exact deleted agent runtime", status: http.StatusForbidden, body: exact, want: true},
		{
			name:   "structured fields are case insensitive and trimmed",
			status: http.StatusForbidden,
			body:   `{"error":{"code":" BISCUIT_BAKER_SERVICE_AGENT_ERROR_STATUS ","message":" agent RUNTIME has been DELETED. "}}`,
			want:   true,
		},
		{name: "same payload under another status", status: http.StatusUnauthorized, body: exact},
		{
			name:   "code without exact deletion message",
			status: http.StatusForbidden,
			body:   `{"error":{"code":"biscuit_baker_service_agent_error_status","message":"Agent runtime is temporarily unavailable."}}`,
		},
		{
			name:   "message without protocol code",
			status: http.StatusForbidden,
			body:   `{"error":{"message":"Agent runtime has been deleted."}}`,
		},
		{name: "malformed response", status: http.StatusForbidden, body: `{"error":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsAgentRuntimeDeleted(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsAgentRuntimeDeleted() = %t, want %t", got, test.want)
			}
		})
	}
}
