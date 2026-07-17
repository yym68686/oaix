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
