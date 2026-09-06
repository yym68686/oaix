package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
)

func TestFingerprintToggleReturnsSuccessWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	user, key := h.createUser(t, "fingerprint-toggle")
	token := h.createToken(t, user.ID, "fingerprint-toggle")
	if token.CodexFingerprintEnabled == nil || !*token.CodexFingerprintEnabled {
		t.Fatal("new token must default to fingerprint convergence enabled")
	}
	for _, route := range []struct {
		path string
		key  string
	}{
		{path: "/api/tokens/" + strconv.FormatInt(token.ID, 10), key: key.PlaintextKey},
		{path: "/admin/tokens/" + strconv.FormatInt(token.ID, 10), key: "service-test-key"},
	} {
		for _, enabled := range []bool{false, true} {
			payload := expectStatus(t, h.request(t, http.MethodPatch, route.path, route.key,
				fmt.Sprintf(`{"codex_fingerprint_enabled":%t}`, enabled)), http.StatusOK)
			if wrapped, ok := payload["token"].(map[string]any); ok {
				payload = wrapped
			}
			if got, ok := payload["codex_fingerprint_enabled"].(bool); !ok || got != enabled {
				t.Fatalf("PATCH %s returned fingerprint=%v, want %v", route.path, payload["codex_fingerprint_enabled"], enabled)
			}
			saved, err := h.db.GetToken(context.Background(), token.ID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.CodexFingerprintEnabled == nil || *saved.CodexFingerprintEnabled != enabled {
				t.Fatalf("PATCH %s did not persist fingerprint=%v", route.path, enabled)
			}
		}
	}
}
