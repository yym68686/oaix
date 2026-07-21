package sub2api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/store"
)

func TestCreateAccountRequestCopiesCodexMetadata(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	raw := json.RawMessage(`{"chatgpt_user_id":"user-1","organization_id":"org-1"}`)
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:                 7,
		TargetGroupIDs:     []int64{10, 11},
		AccountConcurrency: 12,
		AccountPriority:    3,
		ProxyID:            99,
	}, store.Sub2APITokenCandidate{
		ID:           42,
		OwnerUserID:  9,
		Email:        "a@example.com",
		AccountID:    "acct-1",
		IDToken:      "id-token",
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		PlanType:     "pro",
		RawPayload:   raw,
		CreatedAt:    time.Now(),
	})

	if request.Platform != "openai" || request.Type != "oauth" {
		t.Fatalf("platform/type = %s/%s", request.Platform, request.Type)
	}
	if got := request.Credentials["client_id"]; got != "client-fixture" {
		t.Fatalf("client_id = %#v", got)
	}
	if got := request.Credentials["chatgpt_account_id"]; got != "acct-1" {
		t.Fatalf("chatgpt_account_id = %#v", got)
	}
	if got := request.Credentials["organization_id"]; got != "org-1" {
		t.Fatalf("organization_id = %#v", got)
	}
	if len(request.GroupIDs) != 2 || request.GroupIDs[0] != 10 || request.GroupIDs[1] != 11 {
		t.Fatalf("GroupIDs = %#v", request.GroupIDs)
	}
	if request.ConfirmMixedChannelRisk == nil || !*request.ConfirmMixedChannelRisk {
		t.Fatalf("ConfirmMixedChannelRisk = %#v", request.ConfirmMixedChannelRisk)
	}
	if request.Concurrency != 12 {
		t.Fatalf("Concurrency = %d, want 12", request.Concurrency)
	}
	if request.Priority != 3 {
		t.Fatalf("Priority = %d, want 3", request.Priority)
	}
	if request.ProxyID == nil || *request.ProxyID != 99 {
		t.Fatalf("ProxyID = %#v, want 99", request.ProxyID)
	}
	if request.Extra["oaix_token_id"] != int64(42) || request.Extra["oaix_sub2api_target_id"] != int64(7) {
		t.Fatalf("extra = %#v", request.Extra)
	}
	if _, ok := request.Extra["access_token"]; ok {
		t.Fatalf("extra leaked access token: %#v", request.Extra)
	}
}

func TestCreateAccountRequestCopiesAgentIdentityCredentials(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:             7,
		TargetGroupIDs: []int64{10},
	}, store.Sub2APITokenCandidate{
		ID:          42,
		OwnerUserID: 9,
		Email:       "agent@example.invalid",
		AccountID:   "account-fixture",
		PlanType:    "k12",
		AgentIdentity: &agentidentity.Credentials{
			RuntimeID:   "runtime-fixture",
			PrivateKey:  "private-key-fixture",
			TaskID:      "task-fixture",
			AccountID:   "account-fixture",
			UserID:      "user-fixture",
			Email:       "agent@example.invalid",
			PlanType:    "k12",
			WorkspaceID: "workspace-fixture",
			FedRAMP:     true,
		},
	})

	credentials := request.Credentials
	if credentials["auth_mode"] != agentidentity.AuthMode || credentials["agent_runtime_id"] != "runtime-fixture" || credentials["agent_private_key"] != "private-key-fixture" {
		t.Fatalf("agent identity credentials = %#v", credentials)
	}
	if credentials["task_id"] != "task-fixture" || credentials["chatgpt_account_id"] != "account-fixture" || credentials["chatgpt_user_id"] != "user-fixture" {
		t.Fatalf("agent identity metadata = %#v", credentials)
	}
	if credentials["workspace_id"] != "workspace-fixture" || credentials["chatgpt_account_is_fedramp"] != true {
		t.Fatalf("agent identity workspace metadata = %#v", credentials)
	}
	for _, key := range []string{"access_token", "refresh_token", "client_id"} {
		if _, ok := credentials[key]; ok {
			t.Fatalf("agent identity credentials unexpectedly contain %s: %#v", key, credentials)
		}
	}
}

func TestUsageSnapshotInputUsesBatchTimestampFallback(t *testing.T) {
	computedAt := time.Date(2026, 7, 13, 3, 4, 5, 0, time.FixedZone("fixture", 8*60*60))
	throughDate := time.Date(2026, 7, 12, 0, 0, 0, 0, time.FixedZone("fixture", 8*60*60))
	got := usageSnapshotInput(
		store.Sub2APIUsageSyncMapping{TokenID: 9, RemoteAccountID: 42},
		&AccountUsageTotal{AccountCost: 1.5, StandardCost: 1.25, UserCost: 2, TotalRequests: 3, TotalTokens: 4},
		computedAt,
		throughDate,
	)
	if got.TokenID != 9 || got.RemoteAccountID != 42 || got.AccountCostUSD != 1.5 || got.TotalRequests != 3 {
		t.Fatalf("snapshot = %#v", got)
	}
	if got.SourceComputedAt == nil || !got.SourceComputedAt.Equal(computedAt.UTC()) {
		t.Fatalf("source computed at = %#v", got.SourceComputedAt)
	}
	if got.ThroughDate == nil || !got.ThroughDate.Equal(throughDate) {
		t.Fatalf("through date = %#v", got.ThroughDate)
	}
}

func TestStartOfUsageDayUsesTargetTimezone(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	got := startOfUsageDay(time.Date(2026, 7, 20, 18, 30, 0, 0, time.UTC), location)
	want := time.Date(2026, 7, 21, 0, 0, 0, 0, location)
	if !got.Equal(want) || got.Location() != location {
		t.Fatalf("startOfUsageDay = %s (%s), want %s (%s)", got, got.Location(), want, want.Location())
	}
}

func TestCreateAccountRequestDefaultsConcurrency(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:             7,
		TargetGroupIDs: []int64{10},
	}, store.Sub2APITokenCandidate{
		ID:           42,
		OwnerUserID:  9,
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		CreatedAt:    time.Now(),
	})

	if request.Concurrency != store.Sub2APIDefaultAccountConcurrency {
		t.Fatalf("Concurrency = %d, want %d", request.Concurrency, store.Sub2APIDefaultAccountConcurrency)
	}
	if request.ProxyID != nil {
		t.Fatalf("ProxyID = %#v, want nil", request.ProxyID)
	}
}

func TestCreateAccountRequestCapsConcurrency(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	request := syncer.createAccountRequest(store.Sub2APISyncTarget{
		ID:                 7,
		TargetGroupIDs:     []int64{10},
		AccountConcurrency: store.Sub2APIMaxAccountConcurrency + 1,
	}, store.Sub2APITokenCandidate{
		ID:           42,
		OwnerUserID:  9,
		AccessToken:  "access-token",
		RefreshToken: "refresh-token",
		CreatedAt:    time.Now(),
	})

	if request.Concurrency != store.Sub2APIMaxAccountConcurrency {
		t.Fatalf("Concurrency = %d, want %d", request.Concurrency, store.Sub2APIMaxAccountConcurrency)
	}
}

func TestTokenAvailableForSub2API(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Minute)
	past := now.Add(-time.Minute)

	if !tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true}, now) {
		t.Fatal("active token without cooldown should be available")
	}
	if !tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true, CooldownUntil: &past}, now) {
		t.Fatal("expired cooldown should be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 1, IsActive: true, CooldownUntil: &future}, now) {
		t.Fatal("future cooldown should not be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 1, IsActive: false}, now) {
		t.Fatal("inactive token should not be available")
	}
	if tokenAvailableForSub2API(store.Token{ID: 0, IsActive: true}, now) {
		t.Fatal("token without id should not be available")
	}
}

func TestOAuthCredentialsCopiesReimportedTokenValues(t *testing.T) {
	syncer := NewSyncer(nil, nil, nil, "client-fixture")
	credentials := syncer.oauthCredentials(store.Sub2APITokenCandidate{
		ID:           9434,
		Email:        "s1071734018+hocen@gmail.com",
		AccountID:    "account-fixture",
		IDToken:      "new-id-token",
		AccessToken:  "new-access-token",
		RefreshToken: "new-refresh-token",
		PlanType:     "pro",
		RawPayload:   json.RawMessage(`{"organization_id":"org-fixture"}`),
	})

	if credentials["access_token"] != "new-access-token" || credentials["refresh_token"] != "new-refresh-token" {
		t.Fatalf("credentials = %#v", credentials)
	}
	if credentials["id_token"] != "new-id-token" || credentials["client_id"] != "client-fixture" {
		t.Fatalf("credentials = %#v", credentials)
	}
	if credentials["chatgpt_account_id"] != "account-fixture" || credentials["organization_id"] != "org-fixture" {
		t.Fatalf("credentials = %#v", credentials)
	}
}

func TestShouldReconcileMappedAccounts(t *testing.T) {
	if !shouldReconcileMappedAccounts("schedule", 1) {
		t.Fatal("scheduled top-up should reconcile mapped accounts")
	}
	if !shouldReconcileMappedAccounts("manual", 0) {
		t.Fatal("manual sync should reconcile mapped accounts above the threshold")
	}
	if shouldReconcileMappedAccounts("schedule", 0) {
		t.Fatal("routine scheduled sync above the threshold should not scan mapped accounts")
	}
}

func TestSyncMappedAccountUpdatesCredentialsBeforeRestoringAvailability(t *testing.T) {
	seen := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/admin/accounts/42/apply-oauth-credentials":
			var payload ApplyOAuthCredentialsRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode credentials payload: %v", err)
			}
			if payload.Credentials["access_token"] != "new-access" || payload.Credentials["refresh_token"] != "new-refresh" {
				t.Fatalf("credentials = %#v", payload.Credentials)
			}
		case "/api/v1/admin/accounts/42/clear-error", "/api/v1/admin/accounts/42/schedulable":
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeSub2APISuccess(t, w, map[string]any{"id": 42})
	}))
	defer server.Close()

	syncer := NewSyncer(nil, NewClient(server.Client()), nil, "client-fixture")
	err := syncer.syncMappedAccount(t.Context(), server.URL, "admin-fixture", 2, 42, store.Sub2APITokenCandidate{
		ID:           9434,
		OwnerUserID:  1,
		AccessToken:  "new-access",
		RefreshToken: "new-refresh",
	})
	if err != nil {
		t.Fatalf("syncMappedAccount returned error: %v", err)
	}
	want := []string{
		"POST /api/v1/admin/accounts/42/apply-oauth-credentials",
		"POST /api/v1/admin/accounts/42/clear-error",
		"POST /api/v1/admin/accounts/42/schedulable",
	}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("seen = %#v, want %#v", seen, want)
	}
}

func TestSyncMappedAccountRejectsAgentIdentityForOldTargetBeforeMutation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/settings/public" {
			t.Fatalf("unexpected mutation request %s %s", r.Method, r.URL.Path)
		}
		writeSub2APISuccess(t, w, map[string]any{"server_timezone": "UTC", "version": "0.1.135"})
	}))
	defer server.Close()

	syncer := NewSyncer(nil, NewClient(server.Client()), nil, "client-fixture")
	err := syncer.syncMappedAccount(t.Context(), server.URL, "admin-fixture", 2, 42, store.Sub2APITokenCandidate{
		ID: 9434,
		AgentIdentity: &agentidentity.Credentials{
			RuntimeID:  "runtime-fixture",
			PrivateKey: "private-key-fixture",
			AccountID:  "account-fixture",
			UserID:     "user-fixture",
		},
	})
	if err == nil || !strings.Contains(err.Error(), MinimumAgentIdentityTargetVersion) {
		t.Fatalf("err = %v, want target version compatibility error", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want only the public version check", requests)
	}
}

func TestSyncMappedAccountUpdatesAgentIdentityOnSupportedTarget(t *testing.T) {
	seen := make([]string, 0, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/settings/public":
			writeSub2APISuccess(t, w, map[string]any{"server_timezone": "UTC", "version": MinimumAgentIdentityTargetVersion})
		case "/api/v1/admin/accounts/42/apply-oauth-credentials":
			var payload ApplyOAuthCredentialsRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode credentials payload: %v", err)
			}
			if payload.Credentials["auth_mode"] != agentidentity.AuthMode || payload.Credentials["agent_runtime_id"] != "runtime-fixture" {
				t.Fatalf("credentials = %#v", payload.Credentials)
			}
			if _, ok := payload.Credentials["access_token"]; ok {
				t.Fatalf("agent identity payload leaked OAuth access token: %#v", payload.Credentials)
			}
		case "/api/v1/admin/accounts/42/clear-error", "/api/v1/admin/accounts/42/schedulable":
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		writeSub2APISuccess(t, w, map[string]any{"id": 42})
	}))
	defer server.Close()

	syncer := NewSyncer(nil, NewClient(server.Client()), nil, "client-fixture")
	err := syncer.syncMappedAccount(t.Context(), server.URL, "admin-fixture", 2, 42, store.Sub2APITokenCandidate{
		ID: 9434,
		AgentIdentity: &agentidentity.Credentials{
			RuntimeID:  "runtime-fixture",
			PrivateKey: "private-key-fixture",
			AccountID:  "account-fixture",
			UserID:     "user-fixture",
		},
	})
	if err != nil {
		t.Fatalf("syncMappedAccount returned error: %v", err)
	}
	want := []string{
		"GET /api/v1/settings/public",
		"POST /api/v1/admin/accounts/42/apply-oauth-credentials",
		"POST /api/v1/admin/accounts/42/clear-error",
		"POST /api/v1/admin/accounts/42/schedulable",
	}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("seen = %#v, want %#v", seen, want)
	}
}
