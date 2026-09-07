package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/yym68686/oaix/internal/egress"
)

func TestUserProxyChannelsIsolationPersistenceAndRouting(t *testing.T) {
	h := newMultiUserHarness(t)
	owner, key := h.createUser(t, "proxy-owner")
	other, otherKey := h.createUser(t, "proxy-other")
	token := h.createToken(t, owner.ID, "proxy-owner")
	otherToken := h.createToken(t, other.ID, "proxy-other")
	request := func(method, path, auth, body string, status int) map[string]any {
		t.Helper()
		resp := h.request(t, method, path, auth, body)
		data := decodeResponseBody(t, resp)
		if resp.StatusCode != status {
			t.Fatalf("%s %s status=%d want=%d body=%v", method, path, resp.StatusCode, status, data)
		}
		encoded, _ := json.Marshal(data)
		if strings.Contains(string(encoded), "secret-password") {
			t.Fatalf("API leaked proxy password: %s", encoded)
		}
		return data
	}
	created := request("POST", "/api/proxies", key.PlaintextKey, `{"name":"US proxy","proxy":"proxy.example.invalid:9999:demo-user:secret-password"}`, 201)
	id := int64(created["id"].(float64))
	path := fmt.Sprintf("/api/proxies/%d", id)
	tokenPath := fmt.Sprintf("/api/tokens/%d/proxy", token.ID)
	request("POST", "/api/proxies/parse", key.PlaintextKey, `{"proxy":"proxy.example.com:9999:user:secret-password"}`, 200)
	request("POST", "/api/proxies", key.PlaintextKey, `{"name":"bad","proxy":"127.0.0.1:80"}`, 400)
	request("GET", "/api/proxies", "", "", 401)
	request("POST", path, otherKey.PlaintextKey, `{"name":"stolen"}`, 404)
	request("DELETE", path, otherKey.PlaintextKey, "", 404)
	request("POST", path+"/test", otherKey.PlaintextKey, `{}`, 404)
	if len(request("GET", "/api/proxies", otherKey.PlaintextKey, "", 200)["items"].([]any)) != 0 {
		t.Fatal("another owner can list proxies")
	}
	request("GET", tokenPath, otherKey.PlaintextKey, "", 404)
	request("POST", fmt.Sprintf("/api/tokens/%d/proxy", otherToken.ID), otherKey.PlaintextKey, fmt.Sprintf(`{"proxy_channel_id":%d}`, id), 404)
	request("POST", fmt.Sprintf("/api/admin/tokens/%d/proxy", otherToken.ID), "service-test-key", fmt.Sprintf(`{"proxy_channel_id":%d}`, id), 404)
	request("POST", tokenPath, key.PlaintextKey, `{}`, 400)
	request("POST", tokenPath, key.PlaintextKey, fmt.Sprintf(`{"proxy_channel_id":%d}`, id), 200)
	request("DELETE", path, key.PlaintextKey, "", 409)
	u, err := h.db.ResolveTokenProxy(context.Background(), token.ID)
	if err != nil || u == nil || u.Hostname() != "proxy.example.invalid" {
		t.Fatalf("persisted proxy=%v err=%v", u, err)
	}
	var ciphertext string
	if err := h.db.Pool().QueryRow(context.Background(), `select url_ciphertext from proxy_channels where id=$1`, id).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ciphertext, "secret-password") || strings.Contains(ciphertext, "demo-user") {
		t.Fatal("credentials not encrypted at rest")
	}
	if _, err := h.db.Pool().Exec(context.Background(), `update schema_migrations set version=28 where name='oaix_go'`); err != nil {
		t.Fatal(err)
	}
	if err := h.db.MigrateForStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.db.MigrateForStartup(context.Background()); err != nil {
		t.Fatal(err)
	}
	request("POST", path, key.PlaintextKey, `{"name":"renamed"}`, 200)
	afterRename, err := h.db.ResolveTokenProxy(context.Background(), token.ID)
	if err != nil || afterRename.String() != u.String() {
		t.Fatal("rename changed proxy credentials")
	}

	// A real gateway request must fail through the configured invalid proxy;
	// the local upstream must not receive the account credentials directly.
	if err := h.app.tokens.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := len(h.upstream.Auths())
	resp := h.request(t, "POST", "/v1/responses", key.PlaintextKey, `{"model":"gpt-5.5","input":"proxy test","stream":false}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 400 || len(h.upstream.Auths()) != before {
		t.Fatalf("proxy failure bypassed via direct: status=%d upstream calls=%d", resp.StatusCode, len(h.upstream.Auths())-before)
	}
	checked := request("POST", path+"/test", key.PlaintextKey, `{}`, 200)
	if checked["ok"] != false {
		t.Fatal("invalid proxy tested as available")
	}

	request("POST", tokenPath, key.PlaintextKey, `{"proxy_channel_id":0}`, 200)
	if direct, err := h.db.ResolveTokenProxy(context.Background(), token.ID); err != nil || direct != nil {
		t.Fatalf("unbind not immediately effective: %v", err)
	}
	request("DELETE", path, key.PlaintextKey, "", 200)
	request("POST", tokenPath, key.PlaintextKey, fmt.Sprintf(`{"proxy_channel_id":%d}`, id), 404)
}

func TestProxyParserDoesNotAcceptPrivateDestination(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:8000", "http://10.42.0.1:8000", "http://100.64.0.1:8000"} {
		if _, err := egress.Parse(raw); err == nil {
			t.Fatalf("accepted private proxy %q", raw)
		}
	}
}
