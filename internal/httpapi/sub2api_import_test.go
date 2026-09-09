package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func TestSub2APIImportWireAndAuthorization(t *testing.T) {
	app := &App{authKeys: []string{"existing-service-key"}}
	mux := http.NewServeMux()
	app.registerSub2APIImportRoutes(mux)
	for _, tc := range []struct {
		name, key, bearer, body string
		status                  int
	}{
		{"missing", "", "", `{"data":{"accounts":[],"proxies":[]}}`, 401},
		{"service header", "existing-service-key", "", `{"data":{"accounts":[],"proxies":[]}}`, 200},
		{"service bearer", "", "existing-service-key", `{"data":{"accounts":[],"proxies":[]}}`, 200},
		{"header wins", "existing-service-key", "invalid", `{"data":{"accounts":[],"proxies":[]}}`, 200},
		{"bad header wins", "invalid", "existing-service-key", `{"data":{"accounts":[],"proxies":[]}}`, 401},
		{"invalid JSON", "existing-service-key", "", `{"refresh_token":"DO-NOT-ECHO",`, 400},
		{"trailing JSON", "existing-service-key", "", `{"data":{"accounts":[],"proxies":[]}} {}`, 400},
		{"unsupported fields ignored", "existing-service-key", "", `{"skip_default_group_bind":true,"data":{"type":"sub2api-data","version":1,"accounts":[],"proxies":[],"foreign_feature":123}}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/admin/accounts/data", strings.NewReader(tc.body))
			req.Header.Set("X-API-Key", tc.key)
			if tc.bearer != "" {
				req.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			var body map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status {
				t.Fatalf("status=%d response=%s", w.Code, w.Body.String())
			}
			if _, ok := body["code"]; !ok {
				t.Fatal("missing code")
			}
			if body["message"] == nil || strings.Contains(w.Body.String(), "DO-NOT-ECHO") {
				t.Fatal("invalid error envelope")
			}
			if tc.status == 200 {
				want := `{"account_created":0,"account_failed":0,"proxy_created":0,"proxy_failed":0,"proxy_reused":0}`
				got, _ := json.Marshal(body["data"])
				if string(got) != want {
					t.Fatalf("data=%s", got)
				}
			}
		})
	}
	userID := int64(123)
	for _, tc := range []struct {
		auth    *AuthContext
		allowed bool
	}{
		{nil, false}, {&AuthContext{IsService: true}, true}, {&AuthContext{UserID: &userID}, true},
		{&AuthContext{UserID: &userID, ReadOnly: true}, false},
		{&AuthContext{IsAdmin: true, ReadOnly: true}, false},
		{&AuthContext{UserID: &userID, User: &store.PlatformUser{Status: "disabled"}}, false},
		{&AuthContext{}, false},
	} {
		if got := sub2APIImportAllowed(tc.auth); got != tc.allowed {
			t.Fatalf("permission=%v", got)
		}
	}
}

func TestSub2APIImportCredentialBoundary(t *testing.T) {
	var item sub2APIAccountInput
	err := json.Unmarshal([]byte(`{"name":"display label","platform":"openai","type":"oauth","notes":"native remark","group_ids":[12],"concurrency":-1,"priority":-10,"rate_multiplier":-7,"expires_at":1,"extra":{"base_url":"https://foreign.invalid","share_enabled":true},"credentials":{"access_token":"at-valid","refresh_token":"rt-valid","email":"real@example.test","chatgpt_account_id":"acct-test","client_id":"custom-client","is_active":false,"owner_user_id":999,"_share_enabled":true,"proxy_channel_id":888,"type":"foreign","base_url":"https://foreign.invalid"}}`), &item)
	if err != nil {
		t.Fatal(err)
	}
	got, err := item.nativePayload()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"type": "codex", "access_token": "at-valid", "refresh_token": "rt-valid", "email": "real@example.test", "account_id": "acct-test", "client_id": "custom-client", "remark": "native remark"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native payload=%#v", got)
	}
	item.Credentials = map[string]any{"access_token": "at-valid"}
	got, err = item.nativePayload()
	if err != nil {
		t.Fatal(err)
	}
	if got["email"] != nil {
		t.Fatal("display name must not become identity/email")
	}
	for _, platform := range []string{"anthropic", "gemini", "codex", ""} {
		item.Platform = platform
		if _, err := item.nativePayload(); err == nil {
			t.Fatal("non-Codex platform accepted")
		}
	}
}

func TestSub2APISessionInputFormats(t *testing.T) {
	cases := []struct {
		content string
		count   int
	}{
		{"at-one\nat-two", 2},
		{`[{"accessToken":"one"},["two",{"tokens":{"access_token":"three"}}]]`, 3},
		{`{"access_token":"one"} {"access_token":"two"}`, 2},
		{"{\"accessToken\":\"one\"}\nat-two", 2},
		{`{"access_token":`, 0},
	}
	for _, tc := range cases {
		values, err := sub2APISessionEntries(sub2APISessionRequest{Content: tc.content})
		if tc.count == 0 {
			if err == nil {
				t.Fatal("malformed input accepted")
			}
			continue
		}
		if err != nil || len(values) != tc.count {
			t.Fatalf("entries=%d err=%v", len(values), err)
		}
	}
	got := sub2APINativeCredentials(map[string]any{"tokens": map[string]any{"accessToken": "nested-at", "refresh_token": "nested-rt"}, "user": map[string]any{"email": "session@example.test"}, "account": map[string]any{"id": "session-account", "planType": "pro"}})
	if got["access_token"] != "nested-at" || got["email"] != "session@example.test" || got["account_id"] != "session-account" || got["plan_type"] != "pro" {
		t.Fatalf("session mapping=%#v", got)
	}
}

func sub2APIIntegrationRequest(t *testing.T, h *multiUserHarness, path, key string, body any, headers map[string]string, status int) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if headers == nil {
		headers = map[string]string{}
	}
	headers["X-API-Key"] = key
	resp := h.requestWithHeaders(t, "POST", path, "", string(encoded), headers)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("invalid JSON: %s", raw)
	}
	if resp.StatusCode != status {
		t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
	}
	if status < 400 && envelope["code"] != float64(0) {
		t.Fatalf("code=%v", envelope["code"])
	}
	if status >= 400 && envelope["code"] != float64(status) {
		t.Fatalf("error envelope=%s", raw)
	}
	data, _ := envelope["data"].(map[string]any)
	return data
}

func TestSub2APIImportIntegrationOwnerIsolationAndFormats(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	userA, keyA := h.createUser(t, "compat-a")
	userB, keyB := h.createUser(t, "compat-b")
	platformID, err := h.db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	base := map[string]any{"name": "Codex " + suffix, "platform": "openai", "type": "oauth", "notes": "account note", "credentials": map[string]any{"access_token": "at-shared-" + suffix, "refresh_token": "rt-shared-" + suffix, "email": "account-" + suffix + "@example.test"}, "group_ids": []int{999}, "priority": -1, "concurrency": -1, "rate_multiplier": -5, "expires_at": 1, "auto_pause_on_expired": true, "extra": map[string]any{"share_enabled": true, "base_url": "https://ignore.invalid"}}
	ids := []int64{}
	for _, tc := range []struct {
		key   string
		owner int64
	}{{keyA.PlaintextKey, userA.ID}, {keyB.PlaintextKey, userB.ID}, {"service-test-key", platformID}} {
		data := sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts", tc.key, base, nil, 200)
		id := int64(data["id"].(float64))
		ids = append(ids, id)
		token, err := h.db.GetToken(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if token.OwnerUserID != tc.owner || !token.IsActive || token.ShareEnabled || token.ActiveStreamCapOverride != nil || token.Remark == nil || *token.Remark != "account note" {
			t.Fatalf("owner/settings mismatch token=%+v", token)
		}
		encoded, _ := json.Marshal(data)
		if strings.Contains(string(encoded), "at-shared-") || strings.Contains(string(encoded), "rt-shared-") {
			t.Fatal("credential leaked")
		}
	}
	if ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
		t.Fatal("owners shared an account row")
	}
	// Body fields and headers cannot make a user key write another user's pool.
	base["owner_user_id"] = userB.ID
	base["user_id"] = userB.ID
	sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts", keyA.PlaintextKey, base, map[string]string{"X-OAIX-Act-As-User": strconv.FormatInt(userB.ID, 10)}, 403)
	repeated := sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts", keyA.PlaintextKey, base, nil, 200)
	if int64(repeated["id"].(float64)) != ids[0] {
		t.Fatal("repeat did not use native owner-scoped upsert")
	}
	// User import compatibility never grants access to unrelated administration.
	resp := h.request(t, "GET", "/api/admin/users", keyA.PlaintextKey, "")
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal("user gained platform admin access")
	}
	readOnly, err := h.db.CreateAdminAPIKey(ctx, "compat-readonly", "readonly_admin", "test")
	if err != nil {
		t.Fatal(err)
	}
	sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts", readOnly.PlaintextKey, base, nil, 403)
	if err := h.db.RevokeAPIKey(ctx, store.OwnerResources(userB.ID), keyB.ID); err != nil {
		t.Fatal(err)
	}
	sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts", keyB.PlaintextKey, base, nil, 401)
	// Mixed results retain original positions, and unsupported options are no-ops.
	batch := map[string]any{"accounts": []any{map[string]any{"name": "bad", "platform": "anthropic", "type": "oauth", "credentials": map[string]any{"access_token": "ignored"}}, base}}
	result := sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/batch", keyA.PlaintextKey, batch, nil, 200)
	if result["success"] != float64(1) || result["failed"] != float64(1) {
		t.Fatalf("batch=%v", result)
	}
	session := map[string]any{"name": "sessions", "content": fmt.Sprintf(`[{"tokens":{"access_token":"at-session-%s"},"user":{"email":"session-%s@example.test"}},false]`, suffix, suffix), "contents": []string{"at-lines-" + suffix}, "update_existing": false, "group_ids": []int{12}, "credential_extras": map[string]any{"owner_user_id": userB.ID}, "extra": map[string]any{"base_rpm": -100}}
	result = sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/import/codex-session", keyA.PlaintextKey, session, nil, 200)
	if result["total"] != float64(3) || result["created"] != float64(2) || result["failed"] != float64(1) {
		t.Fatalf("session=%v", result)
	}
	items := result["items"].([]any)
	for i, v := range items {
		if v.(map[string]any)["index"] != float64(i+1) {
			t.Fatal("wrong item index")
		}
	}
	// Native import only receives supported fields, even inside credentials.
	var raw map[string]any
	if err := h.db.Pool().QueryRow(ctx, `select raw_payload from codex_tokens where id=$1`, ids[0]).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"group_ids", "extra", "concurrency", "rate_multiplier", "owner_user_id", "sub2api", "credentials"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("foreign setting persisted: %s", key)
		}
	}
}

func TestSub2APIImportIntegrationProxyMappingAndAtomicity(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	user, key := h.createUser(t, "compat-proxy")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	proxyInput := map[string]any{"proxy_key": "foreign-key", "name": "proxy " + suffix, "protocol": "http", "host": "proxy.example.com", "port": 9999, "username": "user", "password": "password", "status": "active", "expires_at": 1, "fallback_mode": "does-not-exist"}
	account := map[string]any{"name": "proxy account", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "at-proxy-" + suffix, "refresh_token": "rt-proxy-" + suffix}, "proxy_key": "foreign-key", "concurrency": 2, "priority": 50}
	payload := map[string]any{"data": map[string]any{"type": "sub2api-data", "version": 1, "exported_at": "2026-09-09T00:00:00Z", "proxies": []any{proxyInput}, "accounts": []any{account}}, "skip_default_group_bind": false}
	result := sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/data", key.PlaintextKey, payload, nil, 200)
	if result["proxy_created"] != float64(1) || result["account_created"] != float64(1) {
		t.Fatalf("data=%v", result)
	}
	result = sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/data", key.PlaintextKey, payload, nil, 200)
	if result["proxy_reused"] != float64(1) || result["account_created"] != float64(1) {
		t.Fatalf("repeat=%v", result)
	}
	channels, err := h.db.ListProxyChannels(ctx, user.ID)
	if err != nil || len(channels) != 1 {
		t.Fatalf("channels=%v err=%v", channels, err)
	}
	accounts, _, err := h.db.ListTokensScoped(ctx, store.OwnerResources(user.ID), store.TokenListOptions{Limit: 10})
	if err != nil || len(accounts) != 1 {
		t.Fatalf("accounts=%d err=%v", len(accounts), err)
	}
	token := accounts[0]
	bound, err := h.db.TokenProxyChannelID(ctx, token.ID)
	if err != nil || bound != channels[0].ID {
		t.Fatalf("binding=%d err=%v", bound, err)
	}
	// Cross-owner proxy binding errors must roll back a credential update too.
	other, _ := h.createUser(t, "compat-other-proxy")
	foreign, err := h.db.SaveProxyChannel(ctx, other.ID, 0, "foreign", "http://proxy.example.org:8888")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.db.UpsertTokenPayloadsForOwner(ctx, user.ID, []map[string]any{{"access_token": "at-should-rollback", "refresh_token": "rt-proxy-" + suffix, "proxy_channel_id": foreign, "remark": "should roll back"}}, "test")
	if err == nil {
		t.Fatal("cross-owner binding accepted")
	}
	after, err := h.db.GetToken(ctx, token.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.AccessToken != token.AccessToken || *after.Remark != *token.Remark {
		t.Fatal("failed import changed live credentials/remark")
	}
	bound, err = h.db.TokenProxyChannelID(ctx, token.ID)
	if err != nil || bound != channels[0].ID {
		t.Fatal("failed import changed previous binding")
	}
	// Missing foreign proxy fails this account rather than routing it directly.
	account["proxy_key"] = "missing"
	result = sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/data", key.PlaintextKey, payload, nil, 200)
	if result["account_failed"] != float64(1) || result["account_created"] != float64(0) {
		t.Fatalf("missing proxy=%v", result)
	}
}

func TestSub2APIImportIntegrationRefreshFailureIndices(t *testing.T) {
	h := newMultiUserHarness(t)
	_, key := h.createUser(t, "compat-refresh")
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("client_id") != "native-client" {
			t.Error("OAuth client ID was not mapped")
		}
		w.WriteHeader(400)
		_, _ = io.WriteString(w, `{"error":"invalid_grant","error_description":"secret-refresh-token must not be echoed"}`)
	}))
	defer mock.Close()
	h.app.cfg.Upstream.OAuthTokenURL = mock.URL
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	req := map[string]any{"accounts": []any{
		map[string]any{"name": "wrong provider", "platform": "anthropic", "type": "oauth", "credentials": map[string]any{"access_token": "unused"}},
		map[string]any{"name": "refresh failed", "platform": "openai", "type": "oauth", "credentials": map[string]any{"refresh_token": "secret-refresh-token", "client_id": "native-client"}},
		map[string]any{"name": "valid third", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "at-third-" + suffix}},
	}}
	result := sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts/batch", key.PlaintextKey, req, nil, 200)
	if result["success"] != float64(1) || result["failed"] != float64(2) {
		t.Fatalf("result=%v", result)
	}
	items := result["results"].([]any)
	if items[2].(map[string]any)["success"] != true || items[2].(map[string]any)["name"] != "valid third" {
		t.Fatal("native preflight results mapped to wrong input")
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "secret-refresh-token") {
		t.Fatal("OAuth error leaked a credential")
	}
}
