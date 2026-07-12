package sub2api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientCountsActiveOAuthAccounts(t *testing.T) {
	var accountRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-API-Key"); got != "admin-fixture" {
			t.Fatalf("X-API-Key = %q", got)
		}
		switch r.URL.Path {
		case "/api/v1/admin/groups/all":
			writeSub2APISuccess(t, w, []Group{{ID: 10, Name: "codex", Platform: "openai", Status: "active"}})
		case "/api/v1/admin/accounts":
			accountRequests++
			if r.URL.Query().Get("platform") != "openai" || r.URL.Query().Get("type") != "oauth" || r.URL.Query().Get("status") != "active" {
				t.Fatalf("unexpected account query: %s", r.URL.RawQuery)
			}
			page := AccountPage{
				Items: []Account{
					{ID: 1, Name: "a", Status: "active"},
					{ID: 2, Name: "b", Status: "active", Schedulable: boolPtr(false)},
				},
				Total: 2, Page: 1, PageSize: 1000, Pages: 1,
			}
			writeSub2APISuccess(t, w, page)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.Client())
	groups, err := client.ListGroups(t.Context(), server.URL, "admin-fixture", "openai")
	if err != nil {
		t.Fatalf("ListGroups returned error: %v", err)
	}
	if len(groups) != 1 || groups[0].ID != 10 {
		t.Fatalf("groups = %#v", groups)
	}
	count, err := client.CountActiveOAuthAccounts(t.Context(), server.URL, "admin-fixture", []int64{10})
	if err != nil {
		t.Fatalf("CountActiveOAuthAccounts returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if accountRequests != 1 {
		t.Fatalf("accountRequests = %d, want 1", accountRequests)
	}
}

func TestClientCreateAccountsUnwrapsBatchResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/accounts/batch" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var payload struct {
			Accounts []CreateAccountRequest `json:"accounts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(payload.Accounts) != 1 || payload.Accounts[0].Credentials["refresh_token"] != "rt" {
			t.Fatalf("unexpected payload: %#v", payload)
		}
		writeSub2APISuccess(t, w, BatchCreateResult{
			Success: 1,
			Results: []BatchAccountResult{{Name: payload.Accounts[0].Name, ID: 99, Success: true}},
		})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	result, err := client.CreateAccounts(t.Context(), server.URL, "admin-fixture", []CreateAccountRequest{{
		Name: "account", Platform: "openai", Type: "oauth", Credentials: map[string]any{"refresh_token": "rt"},
	}})
	if err != nil {
		t.Fatalf("CreateAccounts returned error: %v", err)
	}
	if result.Success != 1 || len(result.Results) != 1 || result.Results[0].ID != 99 {
		t.Fatalf("result = %#v", result)
	}
}

func TestClientGetAccountUsageTotalsBatch(t *testing.T) {
	computedAt := time.Date(2026, 7, 13, 3, 4, 5, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/admin/accounts/usage-totals/batch" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var payload struct {
			AccountIDs []int64 `json:"account_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(payload.AccountIDs) != 1 || payload.AccountIDs[0] != 42 {
			t.Fatalf("payload = %#v", payload)
		}
		writeSub2APISuccess(t, w, AccountUsageTotalsBatch{
			Stats: map[int64]*AccountUsageTotal{
				42: {AccountID: 42, TotalRequests: 7, TotalTokens: 99, AccountCost: 1.25, StandardCost: 1, UserCost: 1.5, ComputedAt: computedAt},
			},
			BackfillComplete: true,
			ComputedAt:       computedAt,
		})
	}))
	defer server.Close()

	got, err := NewClient(server.Client()).GetAccountUsageTotalsBatch(t.Context(), server.URL, "admin-fixture", []int64{42})
	if err != nil {
		t.Fatalf("GetAccountUsageTotalsBatch returned error: %v", err)
	}
	if got.Stats[42] == nil || got.Stats[42].AccountCost != 1.25 || got.Stats[42].TotalRequests != 7 {
		t.Fatalf("result = %#v", got)
	}
}

func TestClientGetAccountUsageTotalsBatchFallsBackWhenUnavailable(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	_, err := NewClient(server.Client()).GetAccountUsageTotalsBatch(t.Context(), server.URL, "admin-fixture", []int64{42})
	if !errors.Is(err, ErrUsageTotalsBatchUnsupported) {
		t.Fatalf("err = %v, want ErrUsageTotalsBatchUnsupported", err)
	}
}

func TestClientGetAccountUsageFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/admin/usage/stats" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("account_id") != "42" || r.URL.Query().Get("timezone") != "UTC" || r.URL.Query().Get("nocache") != "true" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}
		writeSub2APISuccess(t, w, map[string]any{
			"total_requests":     10,
			"total_tokens":       200,
			"total_cost":         3.5,
			"total_actual_cost":  4.5,
			"total_account_cost": 4.0,
		})
	}))
	defer server.Close()

	got, err := NewClient(server.Client()).GetAccountUsageFallback(t.Context(), server.URL, "admin-fixture", 42, time.Now().UTC().AddDate(0, 0, -2))
	if err != nil {
		t.Fatalf("GetAccountUsageFallback returned error: %v", err)
	}
	if got.AccountCost != 4 || got.StandardCost != 3.5 || got.UserCost != 4.5 || got.TotalTokens != 200 {
		t.Fatalf("result = %#v", got)
	}
}

func TestClientGetAccountUsageFallbackRejectsUnsafeWindow(t *testing.T) {
	_, err := NewClient(nil).GetAccountUsageFallback(t.Context(), "https://example.com", "admin-fixture", 42, time.Now().UTC().AddDate(0, 0, -100))
	if !errors.Is(err, ErrUsageFallbackWindowExceeded) {
		t.Fatalf("err = %v, want ErrUsageFallbackWindowExceeded", err)
	}
}

func TestClientGetAccount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/accounts/42" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeSub2APISuccess(t, w, Account{ID: 42, Name: "mapped", Status: "active", Schedulable: boolPtr(true)})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	account, err := client.GetAccount(t.Context(), server.URL, "admin-fixture", 42)
	if err != nil {
		t.Fatalf("GetAccount returned error: %v", err)
	}
	if account.ID != 42 || account.Status != "active" || account.Schedulable == nil || !*account.Schedulable {
		t.Fatalf("account = %#v", account)
	}
}

func TestClientGetAccountNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(server.Client())
	_, err := client.GetAccount(t.Context(), server.URL, "admin-fixture", 42)
	if !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("err = %v, want ErrAccountNotFound", err)
	}
}

func TestClientApplyOAuthCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/admin/accounts/42/apply-oauth-credentials" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var payload ApplyOAuthCredentialsRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.Type != "oauth" || payload.Credentials["access_token"] != "new-access" || payload.Credentials["refresh_token"] != "new-refresh" {
			t.Fatalf("payload = %#v", payload)
		}
		if payload.Extra["oaix_token_id"] != float64(9434) {
			t.Fatalf("extra = %#v", payload.Extra)
		}
		writeSub2APISuccess(t, w, map[string]any{"id": 42, "status": "active"})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	err := client.ApplyOAuthCredentials(t.Context(), server.URL, "admin-fixture", 42,
		map[string]any{"access_token": "new-access", "refresh_token": "new-refresh"},
		map[string]any{"oaix_token_id": int64(9434)},
	)
	if err != nil {
		t.Fatalf("ApplyOAuthCredentials returned error: %v", err)
	}
}

func TestClientRestoreAccountAvailability(t *testing.T) {
	seen := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		if got := r.Header.Get("X-API-Key"); got != "admin-fixture" {
			t.Fatalf("X-API-Key = %q", got)
		}
		switch r.URL.Path {
		case "/api/v1/admin/accounts/42/clear-error":
			if r.Method != http.MethodPost {
				t.Fatalf("method = %s", r.Method)
			}
			writeSub2APISuccess(t, w, map[string]any{"id": 42, "status": "active"})
		case "/api/v1/admin/accounts/42/schedulable":
			if r.Method != http.MethodPost {
				t.Fatalf("method = %s", r.Method)
			}
			var payload map[string]bool
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode schedulable payload: %v", err)
			}
			if !payload["schedulable"] {
				t.Fatalf("payload = %#v", payload)
			}
			writeSub2APISuccess(t, w, map[string]any{"id": 42, "schedulable": true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.Client())
	if err := client.RestoreAccountAvailability(t.Context(), server.URL, "admin-fixture", 42); err != nil {
		t.Fatalf("RestoreAccountAvailability returned error: %v", err)
	}
	want := []string{
		"POST /api/v1/admin/accounts/42/clear-error",
		"POST /api/v1/admin/accounts/42/schedulable",
	}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("seen = %#v, want %#v", seen, want)
	}
}

func TestClientRestoreAccountAvailabilityIgnoresMissingAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(server.Client())
	if err := client.RestoreAccountAvailability(t.Context(), server.URL, "admin-fixture", 42); err != nil {
		t.Fatalf("RestoreAccountAvailability returned error: %v", err)
	}
}

func TestClientListProxiesUsesActiveProxyEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/admin/proxies/all" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.URL.Query().Get("with_count") != "true" {
			t.Fatalf("unexpected query %s", r.URL.RawQuery)
		}
		writeSub2APISuccess(t, w, []map[string]any{{
			"id":            7,
			"name":          "ush1",
			"protocol":      "http",
			"host":          "proxy.example.com",
			"port":          8080,
			"status":        "active",
			"account_count": 42,
			"password":      "secret",
		}})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	proxies, err := client.ListProxies(t.Context(), server.URL, "admin-fixture")
	if err != nil {
		t.Fatalf("ListProxies returned error: %v", err)
	}
	if len(proxies) != 1 || proxies[0].ID != 7 || proxies[0].Name != "ush1" || proxies[0].AccountCount != 42 {
		t.Fatalf("proxies = %#v", proxies)
	}
}

func TestClientReportsSub2APIEnvelopeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeSub2APIResponse(t, w, map[string]any{"code": 401, "message": "bad admin key"})
	}))
	defer server.Close()

	client := NewClient(server.Client())
	_, err := client.ListGroups(t.Context(), server.URL, "bad", "openai")
	if err == nil || !strings.Contains(err.Error(), "bad admin key") {
		t.Fatalf("err = %v, want bad admin key", err)
	}
}

func writeSub2APISuccess(t *testing.T, w http.ResponseWriter, data any) {
	t.Helper()
	writeSub2APIResponse(t, w, map[string]any{"code": 0, "message": "success", "data": data})
}

func writeSub2APIResponse(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func boolPtr(value bool) *bool {
	return &value
}
