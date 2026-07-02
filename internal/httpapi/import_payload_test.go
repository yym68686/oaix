package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yym68686/oaix/internal/config"
)

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

func TestParseImportPayloadExtendedStampsSharingOptions(t *testing.T) {
	payloads, queuePosition, err := parseImportPayloadExtended(map[string]any{
		"import_queue_position": "back",
		"share_enabled":         true,
		"share_status":          "active",
		"tokens": []any{
			map[string]any{"refresh_token": "rt-shared"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queuePosition != "back" || len(payloads) != 1 {
		t.Fatalf("queue=%q payloads=%#v", queuePosition, payloads)
	}
	if payloads[0]["_share_enabled"] != true || payloads[0]["_share_status"] != "active" {
		t.Fatalf("sharing options = %#v", payloads[0])
	}

	textPayloads, _, err := parseImportPayloadExtended(map[string]any{
		"share_enabled": false,
		"text":          `{"tokens":[{"refresh_token":"rt-private"}]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(textPayloads) != 1 || textPayloads[0]["_share_enabled"] != false || textPayloads[0]["_share_status"] != "private" {
		t.Fatalf("text sharing options = %#v", textPayloads)
	}
}

func TestParseImportTextPayloadsExpandsSub2APIDataExport(t *testing.T) {
	payloads, err := parseImportTextPayloads(`{
		"type": "sub2api-data",
		"version": 1,
		"accounts": [
			{
				"name": "first@example.com",
				"platform": "openai",
				"type": "oauth",
				"credentials": {
					"access_token": "eyJhbGciOi.first.signature",
					"refresh_token": "rt-first",
					"id_token": "id-first",
					"account_id": "acct-first",
					"chatgpt_account_id": "acct-first",
					"chatgpt_user_id": "user-first",
					"organization_id": "org-first",
					"email": "first@example.com",
					"type": "codex",
					"disabled": false
				}
			},
			{
				"name": "second@example.com",
				"platform": "openai",
				"type": "oauth",
				"credentials": {
					"access_token": "eyJhbGciOi.second.signature",
					"disabled": true
				}
			},
			{
				"name": "empty@example.com",
				"credentials": {}
			}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 2 {
		t.Fatalf("payload count = %d: %#v", len(payloads), payloads)
	}
	first := payloads[0]
	if first["refresh_token"] != "rt-first" {
		t.Fatalf("first refresh token = %#v", first)
	}
	if _, ok := first["access_token"]; ok {
		t.Fatalf("refresh-capable sub2api account should not import stale access token: %#v", first)
	}
	if first["account_id"] != "acct-first" || first["chatgpt_account_id"] != "acct-first" || first["email"] != "first@example.com" {
		t.Fatalf("first identity metadata = %#v", first)
	}
	if first["id_token"] != "id-first" || first["type"] != "codex" || first["platform"] != "openai" || first["account_type"] != "oauth" {
		t.Fatalf("first metadata = %#v", first)
	}
	second := payloads[1]
	if second["access_token"] != "eyJhbGciOi.second.signature" {
		t.Fatalf("second access token = %#v", second)
	}
	if second["email"] != "second@example.com" || second["is_active"] != false {
		t.Fatalf("second fallback metadata = %#v", second)
	}
}

func TestParseImportPayloadExpandsNestedCredentialTokenItems(t *testing.T) {
	payloads, queuePosition, err := parseImportPayload(map[string]any{
		"import_queue_position": "back",
		"tokens": []any{
			map[string]any{
				"name":     "nested@example.com",
				"platform": "openai",
				"type":     "oauth",
				"credentials": map[string]any{
					"access_token":       "eyJhbGciOi.nested.signature",
					"refresh_token":      "rt-nested",
					"chatgpt_account_id": "acct-nested",
					"email":              "nested@example.com",
					"client_id":          "payload-client",
					"plan_type":          "plus",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queuePosition != "back" || len(payloads) != 1 {
		t.Fatalf("queue=%q payloads=%#v", queuePosition, payloads)
	}
	payload := payloads[0]
	if payload["refresh_token"] != "rt-nested" {
		t.Fatalf("nested refresh token not expanded: %#v", payload)
	}
	if _, ok := payload["credentials"]; ok {
		t.Fatalf("nested credentials leaked into normalized payload: %#v", payload)
	}
	if payload["chatgpt_account_id"] != "acct-nested" || payload["email"] != "nested@example.com" || payload["client_id"] != "payload-client" || payload["plan_type"] != "plus" {
		t.Fatalf("nested metadata not preserved: %#v", payload)
	}
}

func TestPrepareImportPayloadsRefreshesRefreshTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("client_id") != "client-fixture" || r.Form.Get("refresh_token") != "rt-original" {
			t.Fatalf("unexpected form: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "eyJhbGciOi.fixture.signature",
			"refresh_token": "rt-next",
			"id_token":      "id-token",
			"account_id":    "acct-refresh",
			"email":         "acct@example.com",
		})
	}))
	defer server.Close()

	app := NewApp(config.Config{
		Upstream: config.UpstreamConfig{
			OAuthTokenURL: server.URL,
			OAuthClientID: "client-fixture",
			OAuthScope:    "openid profile email",
		},
	}, nil, nil, nil, nil, nil)
	payloads, result := app.prepareImportPayloads(context.Background(), []map[string]any{
		{"account_id": "acct-import", "refresh_token": "rt-original"},
		{"access_token": "eyJhbGciOi.access.signature"},
	})
	if result.Failed != 0 {
		t.Fatalf("preflight result = %+v", result)
	}
	if len(payloads) != 2 {
		t.Fatalf("payload count = %d", len(payloads))
	}
	if payloads[0]["access_token"] == "" || payloads[0]["refresh_token"] != "rt-next" {
		t.Fatalf("refresh payload = %#v", payloads[0])
	}
	if payloads[0]["_previous_refresh_token"] != "rt-original" || payloads[0]["_import_index"] != 0 {
		t.Fatalf("refresh metadata = %#v", payloads[0])
	}
	if payloads[0]["account_id"] != "acct-refresh" || payloads[0]["email"] != "acct@example.com" {
		t.Fatalf("identity metadata = %#v", payloads[0])
	}
}

func TestPrepareImportPayloadsUsesPayloadOAuthClientID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("client_id") != "payload-client" || r.Form.Get("refresh_token") != "rt-original" {
			t.Fatalf("unexpected form: %v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "eyJhbGciOi.fixture.signature",
			"refresh_token": "rt-next",
		})
	}))
	defer server.Close()

	app := NewApp(config.Config{
		Upstream: config.UpstreamConfig{
			OAuthTokenURL: server.URL,
			OAuthClientID: "default-client",
			OAuthScope:    "openid profile email",
		},
	}, nil, nil, nil, nil, nil)
	payloads, result := app.prepareImportPayloads(context.Background(), []map[string]any{
		{"refresh_token": "rt-original", "client_id": "payload-client"},
	})
	if result.Failed != 0 || len(payloads) != 1 {
		t.Fatalf("preflight result=%+v payloads=%#v", result, payloads)
	}
	if payloads[0]["client_id"] != "payload-client" || payloads[0]["refresh_token"] != "rt-next" {
		t.Fatalf("payload metadata not preserved: %#v", payloads[0])
	}
}

func TestPrepareImportPayloadsSkipsInvalidRefreshTokens(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	app := NewApp(config.Config{
		Upstream: config.UpstreamConfig{
			OAuthTokenURL: server.URL,
			OAuthClientID: "client-fixture",
			OAuthScope:    "openid profile email",
		},
	}, nil, nil, nil, nil, nil)
	payloads, result := app.prepareImportPayloads(context.Background(), []map[string]any{
		{"refresh_token": "rt-invalid"},
		{"access_token": "eyJhbGciOi.access.signature"},
	})
	if len(payloads) != 1 {
		t.Fatalf("only access-token payload should remain, got %#v", payloads)
	}
	if result.Failed != 1 || len(result.Items) != 1 || result.Items[0].Index != 0 {
		t.Fatalf("preflight result = %+v", result)
	}
}
