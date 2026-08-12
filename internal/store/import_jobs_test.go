package store

import (
	"strings"
	"testing"
)

func TestImportItemSelectColumnsMatchesScanner(t *testing.T) {
	parts := importItemSelectColumnList("i")
	if len(parts) != 28 {
		t.Fatalf("import item scanner expects 28 columns, got %d: %v", len(parts), parts)
	}
	columns := importItemSelectColumns("i")
	required := []string{
		"i.matched_existing_token_id",
		"i.publish_attempted",
		"i.publish_skipped_reason",
		"i.reactivated",
		"i.previous_is_active",
		"i.next_is_active",
		"i.previous_disabled_at",
		"i.next_disabled_at",
		"i.refresh_error_code",
		"i.refresh_error_message_excerpt",
	}
	for _, fragment := range required {
		if !strings.Contains(columns, fragment) {
			t.Fatalf("missing import item column %q in %s", fragment, columns)
		}
	}
}

func TestImportCredentialIdentityIgnoresRedactedRefreshToken(t *testing.T) {
	payload := map[string]any{
		"refresh_token": "...",
		"access_token":  "eyJ.access.signature",
	}
	if got := tokenIdentityFromPayload(payload); got != "eyJ.access.signature" {
		t.Fatalf("token identity = %q", got)
	}
	if got := storedRefreshTokenFromPayload(payload); got != accessTokenOnlyRefreshToken("eyJ.access.signature") {
		t.Fatalf("stored refresh token = %q", got)
	}
	if hash := refreshHashFromPayload(map[string]any{"refresh_token": "."}); hash != nil {
		t.Fatalf("redacted-only payload received a hash: %q", *hash)
	}
}

func TestNormalizeTokenPayloadFallsBackFromRedactedRefreshToken(t *testing.T) {
	payload := normalizeTokenPayload(map[string]any{
		"refresh_token": "...",
		"access_token":  "eyJ.access.signature",
		"account_id":    "acct-1",
	})
	if _, exists := payload["refresh_token"]; exists {
		t.Fatalf("redacted refresh token leaked: %#v", payload)
	}
	if payload["access_token"] != "eyJ.access.signature" || payload["account_id"] != "acct-1" {
		t.Fatalf("payload = %#v", payload)
	}
}
