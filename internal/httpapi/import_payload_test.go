package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestSub2APIImportFallsBackFromRedactedRefreshTokensWithoutCollapsingAccounts(t *testing.T) {
	accounts := make([]any, 0, 20)
	for index := 0; index < 20; index++ {
		refreshToken := fmt.Sprintf("rt-real-%d", index)
		if index < 17 {
			refreshToken = "..."
		} else if index == 17 {
			refreshToken = "."
		}
		accounts = append(accounts, map[string]any{
			"name": fmt.Sprintf("account-%d@example.com", index),
			"credentials": map[string]any{
				"access_token":  fmt.Sprintf("eyJ.account-%d.signature", index),
				"refresh_token": refreshToken,
				"account_id":    fmt.Sprintf("acct-%d", index),
				"email":         fmt.Sprintf("account-%d@example.com", index),
			},
		})
	}
	data, err := json.Marshal(map[string]any{"type": "sub2api-data", "accounts": accounts})
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := parseImportTextPayloads(string(data))
	if err != nil {
		t.Fatal(err)
	}
	payloads, summary := finalizeImportParse(payloads, importTextInputCount(string(data), payloads))
	if len(payloads) != 20 || summary.InputCount != 20 || summary.Total != 20 {
		t.Fatalf("payloads=%d summary=%+v", len(payloads), summary)
	}
	if summary.RedactedCredentialCount != 18 || summary.AccessTokenFallbackCount != 18 || summary.DeduplicatedCount != 0 || summary.RejectedCount != 0 {
		t.Fatalf("summary=%+v", summary)
	}
	for index, payload := range payloads {
		if _, leaked := payload[importRedactedCredentialCountKey]; leaked {
			t.Fatalf("payload %d leaked parser metadata: %#v", index, payload)
		}
		if index < 18 {
			if payload["access_token"] != fmt.Sprintf("eyJ.account-%d.signature", index) || payload["refresh_token"] != nil {
				t.Fatalf("payload %d did not use access fallback: %#v", index, payload)
			}
		} else if payload["refresh_token"] != fmt.Sprintf("rt-real-%d", index) || payload["access_token"] != nil {
			t.Fatalf("payload %d did not preserve real refresh preference: %#v", index, payload)
		}
	}
}

func TestSub2APIImportFallsBackWhenRefreshTokenCollidesAcrossDistinctAccessTokens(t *testing.T) {
	accounts := make([]any, 0, 10)
	for index := 0; index < 10; index++ {
		accounts = append(accounts, map[string]any{
			"name":     fmt.Sprintf("account-%d@example.com", index),
			"platform": "openai",
			"type":     "oauth",
			"credentials": map[string]any{
				// The incident input carried the same non-empty value here while
				// each account still had a distinct usable access token.
				"refresh_token": "111",
				"access_token":  fmt.Sprintf("eyJ.account-%d.signature", index),
				"account_id":    fmt.Sprintf("acct-%d", index),
				"email":         fmt.Sprintf("account-%d@example.com", index),
			},
		})
	}
	data, err := json.Marshal(map[string]any{"type": "sub2api-data", "accounts": accounts})
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := parseImportTextPayloads(string(data))
	if err != nil {
		t.Fatal(err)
	}
	payloads, summary := finalizeImportParse(payloads, 10)
	if len(payloads) != 10 || summary.Total != 10 || summary.DeduplicatedCount != 0 {
		t.Fatalf("payloads=%d summary=%+v", len(payloads), summary)
	}
	for index, payload := range payloads {
		if payload["access_token"] != fmt.Sprintf("eyJ.account-%d.signature", index) {
			t.Fatalf("payload %d access token = %#v", index, payload["access_token"])
		}
		if _, ok := payload["refresh_token"]; ok {
			t.Fatalf("payload %d retained colliding refresh token: %#v", index, payload)
		}
	}
}

func TestSub2APIImportKeepsRefreshIdentityForSameAccountVariants(t *testing.T) {
	accounts := []any{
		map[string]any{
			"name": "account@example.com",
			"credentials": map[string]any{
				"refresh_token": "rt-shared",
				"access_token":  "eyJ.account-one.signature",
				"account_id":    "acct-same",
			},
		},
		map[string]any{
			"name": "account@example.com",
			"credentials": map[string]any{
				"refresh_token": "rt-shared",
				"access_token":  "eyJ.account-two.signature",
				"account_id":    "acct-same",
			},
		},
	}
	data, err := json.Marshal(map[string]any{"type": "sub2api-data", "accounts": accounts})
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := parseImportTextPayloads(string(data))
	if err != nil {
		t.Fatal(err)
	}
	payloads, summary := finalizeImportParse(payloads, 2)
	if len(payloads) != 1 || summary.DeduplicatedCount != 1 {
		t.Fatalf("payloads=%d summary=%+v", len(payloads), summary)
	}
	if payloads[0]["refresh_token"] != "rt-shared" {
		t.Fatalf("same-account refresh identity changed: %#v", payloads[0])
	}
}

func TestImportParseSummaryRejectsRedactedOnlyCredentials(t *testing.T) {
	payloads, err := parseImportTextPayloads(`{
		"type": "sub2api-data",
		"accounts": [{"name":"redacted@example.com","credentials":{"refresh_token":"..."}}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	payloads, summary := finalizeImportParse(payloads, 1)
	if len(payloads) != 0 || summary.Total != 0 || summary.RejectedCount != 1 || summary.RedactedCredentialCount != 1 {
		t.Fatalf("payloads=%#v summary=%+v", payloads, summary)
	}
}

func TestParseImportTextPayloadsExpandsSub2APIAgentIdentityExportByRuntime(t *testing.T) {
	accounts := make([]any, 0, 50)
	for index := 0; index < 50; index++ {
		accounts = append(accounts, map[string]any{
			"name":     fmt.Sprintf("agent-%d@example.invalid", index),
			"platform": "openai",
			"type":     "oauth",
			"credentials": map[string]any{
				"auth_mode":          "agentIdentity",
				"agent_runtime_id":   fmt.Sprintf("runtime-%d", index),
				"agent_private_key":  fmt.Sprintf("private-key-%d", index),
				"task_id":            fmt.Sprintf("task-%d", index),
				"account_id":         "shared-workspace",
				"chatgpt_account_id": "shared-workspace",
				"chatgpt_user_id":    fmt.Sprintf("user-%d", index),
				"email":              fmt.Sprintf("agent-%d@example.invalid", index),
				"plan_type":          "pro",
			},
		})
	}
	data, err := json.MarshalIndent(map[string]any{
		"type":     "sub2api-data",
		"version":  1,
		"proxies":  []any{},
		"accounts": accounts,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := parseImportTextPayloads(string(data))
	if err != nil {
		t.Fatal(err)
	}
	payloads = dedupeImportPayloads(payloads)
	if len(payloads) != 50 {
		t.Fatalf("payload count = %d, want 50", len(payloads))
	}
	for index, payload := range payloads {
		if payload["auth_mode"] != "agentIdentity" || payload["agent_runtime_id"] != fmt.Sprintf("runtime-%d", index) {
			t.Fatalf("payload %d = %#v", index, payload)
		}
		if payload["chatgpt_account_id"] != "shared-workspace" || payload["chatgpt_user_id"] != fmt.Sprintf("user-%d", index) {
			t.Fatalf("payload %d identity = %#v", index, payload)
		}
		if payload["access_token"] != nil || payload["refresh_token"] != nil {
			t.Fatalf("payload %d unexpectedly became OAuth credentials: %#v", index, payload)
		}
	}
}

func TestParseImportTextPayloadsDoesNotTreatEmptyJSONExportAsLineTokens(t *testing.T) {
	payloads, err := parseImportTextPayloads(`{
		"type": "sub2api-data",
		"version": 1,
		"proxies": [],
		"accounts": []
	}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 0 {
		t.Fatalf("empty JSON export parsed as line tokens: %#v", payloads)
	}
}

func TestParseImportPayloadExpandsAccountsExportWithoutType(t *testing.T) {
	payloads, queuePosition, err := parseImportPayload(map[string]any{
		"import_queue_position": "front",
		"_share_enabled":        true,
		"_share_status":         "active",
		"source_file":           "accounts.json",
		"accounts": []any{
			map[string]any{
				"name":     "account@example.com",
				"platform": "openai",
				"type":     "oauth",
				"credentials": map[string]any{
					"access_token":       "eyJhbGciOi.account.signature",
					"refresh_token":      "rt-account",
					"chatgpt_account_id": "acct-account",
					"email":              "account@example.com",
					"plan_type":          "plus",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queuePosition != "front" || len(payloads) != 1 {
		t.Fatalf("queue=%q payloads=%#v", queuePosition, payloads)
	}
	payload := payloads[0]
	if payload["refresh_token"] != "rt-account" {
		t.Fatalf("account refresh token not expanded: %#v", payload)
	}
	if _, ok := payload["accounts"]; ok {
		t.Fatalf("top-level accounts leaked into normalized payload: %#v", payload)
	}
	if payload["email"] != "account@example.com" || payload["plan_type"] != "plus" {
		t.Fatalf("account metadata not preserved: %#v", payload)
	}
	if payload["_share_enabled"] != true || payload["_share_status"] != "active" || payload["source_file"] != "accounts.json" {
		t.Fatalf("import control fields not preserved: %#v", payload)
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
