package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/codexticket"
)

func TestCodexTicketSettingsPermissionsPersistenceAndRedaction(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	h.db.Pool().Exec(ctx, `delete from gateway_settings where key=$1`, codexticket.SettingKey)
	t.Cleanup(func() {
		h.db.Pool().Exec(context.Background(), `delete from gateway_settings where key=$1`, codexticket.SettingKey)
	})
	user, key := h.createUser(t, "ticket-policy")
	expectStatus(t, h.request(t, "GET", "/admin/codex-tickets", key.PlaintextKey, ""), 403)
	readonly, err := h.db.CreateAPIKey(ctx, &user.ID, "user", "ticket-readonly", "readonly_admin", &user.ID)
	if err != nil {
		t.Fatal(err)
	}
	expectStatus(t, h.request(t, "POST", "/admin/codex-tickets", readonly.PlaintextKey, `{"enabled":false}`), 403)
	initial := expectStatus(t, h.request(t, "GET", "/admin/codex-tickets", "service-test-key", ""), 200)
	policy := initial["policy"].(map[string]any)
	if policy["enabled"] != true || policy["fail_closed"] != false {
		t.Fatal("wrong default policy")
	}
	proxy := "http://ticket-user:private-ticket-password@proxy.example.com:8080"
	body, _ := json.Marshal(map[string]any{"harvest_proxy_url": proxy, "fail_closed": true})
	saved := expectStatus(t, h.request(t, "POST", "/admin/codex-tickets", "service-test-key", string(body)), 200)
	if strings.Contains(fmt.Sprint(saved), "private-ticket-password") {
		t.Fatal("proxy password exposed")
	}
	masked := saved["harvest_proxy_url"].(string)
	for _, payload := range []string{`{"enabled":true}`, `{"harvest_proxy_url":""}`, fmt.Sprintf(`{"harvest_proxy_url":%q}`, masked)} {
		expectStatus(t, h.request(t, "POST", "/admin/codex-tickets", "service-test-key", payload), 200)
		got, err := h.db.LoadCodexTicketPolicy(ctx)
		if err != nil || got.HarvestProxyURL != proxy {
			t.Fatal("partial/masked update lost proxy")
		}
	}
	for _, payload := range []string{`{"models":[]}`, `{"harvest_proxy_url":"http://ticket-user:private-ticket-password@127.0.0.1:8080"}`, `{"harvest_proxy_url":"http://other:***@proxy.example.com:8080"}`, `{"enabled":true} {}`} {
		result := expectStatus(t, h.request(t, "POST", "/admin/codex-tickets", "service-test-key", payload), 400)
		if strings.Contains(fmt.Sprint(result), "private-ticket-password") {
			t.Fatal("error exposed secret")
		}
	}
	listing := expectStatus(t, h.request(t, "GET", "/admin/settings", "service-test-key", ""), 200)
	if strings.Contains(fmt.Sprint(listing), "proxy_ciphertext") || strings.Contains(fmt.Sprint(listing), "private-ticket-password") {
		t.Fatal("generic settings exposed proxy")
	}
	expectStatus(t, h.request(t, "POST", "/admin/settings/codex_tickets", "service-test-key", `{"enabled":false}`), 400)
	expectStatus(t, h.request(t, "DELETE", "/admin/settings/codex_tickets", "service-test-key", ""), 400)
	var persisted string
	if err := h.db.Pool().QueryRow(ctx, `select value from gateway_settings where key=$1`, codexticket.SettingKey).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(persisted, "private-ticket-password") || !strings.Contains(persisted, "proxy_ciphertext") {
		t.Fatal("proxy not encrypted at rest")
	}
	expectStatus(t, h.request(t, "POST", "/admin/codex-tickets", "service-test-key", `{"clear_harvest_proxy":true,"fail_closed":false}`), 200)
	got, _ := h.db.LoadCodexTicketPolicy(ctx)
	if got.HarvestProxyURL != "" || got.FailClosed {
		t.Fatal("clear or live policy failed")
	}
}

func TestCodexTicketStorageRoundTripMonotonicAndAccountEdits(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	user, _ := h.createUser(t, "ticket-storage")
	token := h.createToken(t, user.ID, "ticket-storage")
	t.Cleanup(func() {
		h.db.Pool().Exec(context.Background(), `delete from codex_turn_tickets where token_id=$1`, token.ID)
	})
	now := time.Now().UTC().Truncate(time.Microsecond)
	ticket := codexticket.Ticket{TokenID: token.ID, Model: "gpt-6-astra", Identity: strings.Repeat("a", 64), State: "gAAAAA" + strings.Repeat("A", 286), CapturedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := h.db.SaveCodexTicket(ctx, ticket); err != nil {
		t.Fatal(err)
	}
	older := ticket
	older.CapturedAt = now.Add(-time.Minute)
	older.ExpiresAt = older.CapturedAt.Add(time.Hour)
	older.State = "gAAAAA" + strings.Repeat("B", 286)
	if err := h.db.SaveCodexTicket(ctx, older); err != nil {
		t.Fatal(err)
	}
	// Reimport and an ordinary account edit must not overwrite server-managed state.
	if _, err := h.db.Pool().Exec(ctx, `update codex_tokens set remark='edited',updated_at=now() where id=$1`, token.ID); err != nil {
		t.Fatal(err)
	}
	rows, err := h.db.LoadCodexTickets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, got := range rows {
		if got.TokenID == token.ID {
			found = true
			if got.State != ticket.State || !got.CapturedAt.Equal(now) {
				t.Fatal("stale save overwrote current ticket")
			}
		}
	}
	if !found {
		t.Fatal("ticket not restored")
	}
	var ciphertext string
	if err := h.db.Pool().QueryRow(ctx, `select state_ciphertext from codex_turn_tickets where token_id=$1`, token.ID).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(ciphertext, ticket.State) || !strings.HasPrefix(ciphertext, "v1:") {
		t.Fatal("ticket not encrypted")
	}
	// Rollback-compatible additive migration supports old binaries and preserves tickets.
	if _, err := h.db.Pool().Exec(ctx, `update schema_migrations set version=33 where name='oaix_go'`); err != nil {
		t.Fatal(err)
	}
	if err := h.db.MigrateForStartup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}
}
