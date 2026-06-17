package httpapi

import "testing"

func TestParseImportPayloadPreservesRefreshTokenInputs(t *testing.T) {
	payloads, queuePosition, err := parseImportPayload(map[string]any{
		"import_queue_position": "back",
		"tokens": []any{
			"rt-refresh-only",
			"rt.1.refresh-token-with-dots",
			map[string]any{"account_id": "acct_123", "refresh_token": "rt-with-account"},
			map[string]any{"token": "eyJhbGciOi.test.signature"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queuePosition != "back" {
		t.Fatalf("queue position = %q", queuePosition)
	}
	if len(payloads) != 4 {
		t.Fatalf("payload count = %d", len(payloads))
	}
	if payloads[0]["refresh_token"] != "rt-refresh-only" {
		t.Fatalf("bare refresh token parsed as %#v", payloads[0])
	}
	if payloads[1]["refresh_token"] != "rt.1.refresh-token-with-dots" {
		t.Fatalf("account refresh payload parsed as %#v", payloads[1])
	}
	if payloads[2]["account_id"] != "acct_123" || payloads[2]["refresh_token"] != "rt-with-account" {
		t.Fatalf("account refresh payload parsed as %#v", payloads[2])
	}
	if payloads[3]["access_token"] != "eyJhbGciOi.test.signature" {
		t.Fatalf("access token parsed as %#v", payloads[3])
	}
}
