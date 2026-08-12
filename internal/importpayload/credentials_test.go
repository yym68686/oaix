package importpayload

import "testing"

func TestIsRedactedCredential(t *testing.T) {
	for _, value := range []string{".", "...", " … ", "***", "[REDACTED]", "<redacted>", "(redacted)"} {
		if !IsRedactedCredential(value) {
			t.Fatalf("%q was not recognized as redacted", value)
		}
	}
	for _, value := range []string{"", "rt-real", "eyJ.token.signature", "rt...suffix"} {
		if IsRedactedCredential(value) {
			t.Fatalf("%q was incorrectly recognized as redacted", value)
		}
	}
}

func TestCleanCredentialsRemovesOnlyRedactedValues(t *testing.T) {
	cleaned, removed := CleanCredentials(map[string]any{
		"refresh_token": "...",
		"access_token":  "eyJ.token.signature",
		"account_id":    "acct-1",
	})
	if removed != 1 || cleaned["refresh_token"] != nil {
		t.Fatalf("cleaned payload = %#v, removed = %d", cleaned, removed)
	}
	if cleaned["access_token"] != "eyJ.token.signature" || cleaned["account_id"] != "acct-1" {
		t.Fatalf("usable values were not preserved: %#v", cleaned)
	}
}
