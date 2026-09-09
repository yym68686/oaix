package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func sub2APIGetTest(t *testing.T, h *multiUserHarness, path, key string, status int) any {
	t.Helper()
	resp := h.requestWithHeaders(t, http.MethodGet, path, "", "", map[string]string{"X-API-Key": key})
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("%s returned non-JSON: %s", path, raw)
	}
	if resp.StatusCode != status {
		t.Fatalf("%s status=%d data=%s", path, resp.StatusCode, raw)
	}
	if status < 400 && (data["code"] != float64(0) || resp.Header.Get("Cache-Control") != "private, no-store") {
		t.Fatalf("invalid envelope/cache for %s: %s", path, raw)
	}
	return data["data"]
}
func TestSub2APIQueryIntegrationIsolationAndStates(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	u, k := h.createUser(t, "query-a")
	other, otherKey := h.createUser(t, "query-b")
	healthy := h.createToken(t, u.ID, "healthy")
	cool := h.createToken(t, u.ID, "cool")
	backoff := h.createToken(t, u.ID, "backoff")
	paused := h.createToken(t, u.ID, "paused")
	broken := h.createToken(t, u.ID, "broken")
	empty := h.createToken(t, u.ID, "empty")
	foreign := h.createToken(t, other.ID, "foreign")
	for _, tc := range []struct {
		id  int64
		sql string
	}{
		{healthy.ID, "remark='Alpha',plan_type='pro'"},
		{cool.ID, "cooldown_until=now()+interval '1 hour',last_error='usage_limit_reached: access-SECRET'"},
		{backoff.ID, "cooldown_until=now()+interval '50 seconds',last_error='retryable upstream failure: access-SECRET'"},
		{paused.ID, "is_active=false"}, {broken.ID, "is_active=false,last_error='refresh_token=SECRET'"}, {empty.ID, "access_token=''"},
	} {
		if _, err := h.db.Pool().Exec(ctx, "update codex_tokens set "+tc.sql+" where id=$1", tc.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.db.UpsertUserTokenConcurrency(ctx, u.ID, map[string]int64{"pro": 7}); err != nil {
		t.Fatal(err)
	}
	proxy, err := h.db.SaveProxyChannel(ctx, u.ID, 0, "owned proxy", "http://proxy-user:proxy-SECRET@8.8.8.8:8888")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.db.SetTokenProxyChannel(ctx, store.OwnerResources(u.ID), healthy.ID, proxy); err != nil {
		t.Fatal(err)
	}
	base := "/api/v1/admin/accounts"
	for _, tc := range []struct {
		status string
		total  int
	}{{"", 6}, {"active", 1}, {"rate_limited", 1}, {"temp_unschedulable", 1}, {"inactive", 1}, {"error", 2}, {"unschedulable", 0}} {
		data := sub2APIGetTest(t, h, base+"?status="+tc.status, k.PlaintextKey, 200).(map[string]any)
		if data["total"] != float64(tc.total) || len(data["items"].([]any)) != tc.total {
			t.Fatalf("status=%s data=%v", tc.status, data)
		}
		raw, _ := json.Marshal(data)
		if strings.Contains(string(raw), "SECRET") || strings.Contains(string(raw), healthy.AccessToken) {
			t.Fatal("query leaked a credential/error payload")
		}
	}
	data := sub2APIGetTest(t, h, base+"?platform=openai&type=oauth&status=active&group=999&privacy_mode=training_off&lite=1&include_scheduler_score=1", k.PlaintextKey, 200).(map[string]any)
	item := data["items"].([]any)[0].(map[string]any)
	if item["id"] != float64(healthy.ID) || item["concurrency"] != float64(7) || item["proxy_id"] != float64(proxy) || item["schedulable"] != true {
		t.Fatalf("mapped item=%v", item)
	}
	if _, ok := item["groups"]; ok {
		t.Fatal("lite repeated group data")
	}
	detail := sub2APIGetTest(t, h, fmt.Sprintf("%s/%d", base, healthy.ID), k.PlaintextKey, 200).(map[string]any)
	if detail["name"] != "Alpha" {
		t.Fatalf("native remark not mapped: %v", detail)
	}
	for _, q := range []string{"platform=anthropic", "type=apikey", "search=does-not-exist", "page=999"} {
		result := sub2APIGetTest(t, h, base+"?"+q, k.PlaintextKey, 200).(map[string]any)
		if len(result["items"].([]any)) != 0 {
			t.Fatalf("filter %s=%v", q, result)
		}
	}
	if result := sub2APIGetTest(t, h, base+"?search=Alpha&owner_user_id="+fmt.Sprint(other.ID), k.PlaintextKey, 200).(map[string]any); result["total"] != float64(1) {
		t.Fatal("search/owner filter mismatch")
	}
	for _, suffix := range []string{"", "/usage", "/stats", "/today-stats", "/temp-unschedulable", "/models", "/ollama-cloud-usage"} {
		sub2APIGetTest(t, h, fmt.Sprintf("%s/%d%s", base, foreign.ID, suffix), k.PlaintextKey, 404)
	}
	sub2APIGetTest(t, h, fmt.Sprintf("/api/v1/admin/openai/accounts/%d/quota", foreign.ID), k.PlaintextKey, 404)
	sub2APIGetTest(t, h, fmt.Sprintf("/api/v1/admin/proxies/%d", proxy), otherKey.PlaintextKey, 404)
	sub2APIGetTest(t, h, base, "bad", 401)
	sub2APIGetTest(t, h, "/api/v1/admin/accounts/not-a-route", k.PlaintextKey, 400)
	sub2APIGetTest(t, h, "/api/v1/admin/not-a-route", k.PlaintextKey, 404)
	resp := h.requestWithHeaders(t, "GET", base, "", "", map[string]string{"X-API-Key": k.PlaintextKey, "X-OAIX-Act-As-User": fmt.Sprint(other.ID)})
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatal("user impersonation was allowed")
	}
	platform, err := h.db.BootstrapUserID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pt := h.createToken(t, platform, "query-platform")
	sub2APIGetTest(t, h, fmt.Sprintf("%s/%d", base, pt.ID), "service-test-key", 200)
	sub2APIGetTest(t, h, fmt.Sprintf("%s/%d", base, healthy.ID), "service-test-key", 404)
	readonly, err := h.db.CreateAdminAPIKey(ctx, "query-readonly", "readonly_admin", "test")
	if err != nil {
		t.Fatal(err)
	}
	sub2APIGetTest(t, h, fmt.Sprintf("%s/%d", base, pt.ID), readonly.PlaintextKey, 200)
	sub2APIIntegrationRequest(t, h, base+"/data", readonly.PlaintextKey, map[string]any{"data": map[string]any{"accounts": []any{}, "proxies": []any{}}}, nil, 403)
}

func TestSub2APIQueryIntegrationCompleteAccountReadSurface(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	u, k := h.createUser(t, "query-surface")
	token := h.createToken(t, u.ID, "surface")
	proxy, err := h.db.SaveProxyChannel(ctx, u.ID, 0, "surface proxy", "http://8.8.4.4:8088")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.db.SetTokenProxyChannel(ctx, store.OwnerResources(u.ID), token.ID, proxy); err != nil {
		t.Fatal(err)
	}
	// Every GET in source registerAccountRoutes/registerOpenAIOAuthRoutes,
	// registerProxyRoutes/registerGroupRoutes is represented here, plus the two
	// account query POSTs. This guards against SPA success responses.
	paths := []string{
		"/accounts", "/accounts/{id}", "/accounts/data", "/accounts/{id}/usage", "/accounts/{id}/stats", "/accounts/{id}/today-stats", "/accounts/{id}/temp-unschedulable", "/accounts/{id}/models",
		"/accounts/upstream-billing-rates", "/accounts/upstream-billing-probe/settings", "/accounts/ollama-cloud-usage/settings", "/accounts/{id}/ollama-cloud-usage", "/accounts/antigravity/default-model-mapping", "/openai/accounts/{id}/quota",
		"/proxies", "/proxies/all", "/proxies/data", "/proxies/{proxy}", "/proxies/{proxy}/stats", "/proxies/{proxy}/accounts",
		"/groups", "/groups/all", "/groups/usage-summary", "/groups/capacity-summary", "/groups/live-capability", "/groups/999", "/groups/999/models-list-candidates", "/groups/999/composite-routes", "/groups/999/stats", "/groups/999/rate-multipliers", "/groups/999/api-keys",
	}
	for _, path := range paths {
		path = strings.ReplaceAll(strings.ReplaceAll(path, "{id}", fmt.Sprint(token.ID)), "{proxy}", fmt.Sprint(proxy))
		sub2APIGetTest(t, h, "/api/v1/admin"+path, k.PlaintextKey, 200)
	}
	for _, path := range []string{"/usage/batch", "/today-stats/batch"} {
		sub2APIIntegrationRequest(t, h, "/api/v1/admin/accounts"+path, k.PlaintextKey, map[string]any{"account_ids": []int64{token.ID}}, nil, 200)
	}
	bundle := sub2APIGetTest(t, h, fmt.Sprintf("/api/v1/admin/accounts/data?ids=%d&ids=%d", token.ID, token.ID), k.PlaintextKey, 200).(map[string]any)
	raw, _ := json.Marshal(bundle)
	if strings.Contains(string(raw), token.AccessToken) || strings.Contains(string(raw), token.RefreshToken) || bundle["credentials_redacted"] != true {
		t.Fatal("native export credential boundary changed")
	}
	if len(bundle["accounts"].([]any)) != 1 || len(bundle["proxies"].([]any)) != 1 {
		t.Fatalf("export mappings=%v", bundle)
	}
}

func TestSub2APIQueryIntegrationLargePagination(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	u, k := h.createUser(t, "query-pages")
	_, err := h.db.Pool().Exec(ctx, `insert into codex_tokens(owner_user_id,access_token,refresh_token,is_active,remark)
 select $1::bigint,'query-page-access-'||g,'query-page-refresh-'||($1::bigint)::text||'-'||g,true,lpad(g::text,4,'0') from generate_series(1,1005) g`, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	data := sub2APIGetTest(t, h, "/api/v1/admin/accounts?page_size=1000&sort_by=id&sort_order=asc", k.PlaintextKey, 200).(map[string]any)
	items := data["items"].([]any)
	if len(items) != 1000 || data["total"] != float64(1005) || data["pages"] != float64(2) {
		t.Fatalf("large pagination mismatch %v count=%d", data["total"], len(items))
	}
	next := sub2APIGetTest(t, h, "/api/v1/admin/accounts?limit=1000&page=2&sort_by=id&sort_order=asc", k.PlaintextKey, 200).(map[string]any)
	tail := next["items"].([]any)
	if len(tail) != 5 || items[999].(map[string]any)["id"].(float64) >= tail[0].(map[string]any)["id"].(float64) {
		t.Fatal("second page overlaps or truncates")
	}
}

func TestSub2APIQueryIntegrationUsageFacts(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	u, k := h.createUser(t, "query-usage")
	other, _ := h.createUser(t, "query-usage-other")
	token := h.createToken(t, u.ID, "usage")
	_, err := h.db.Pool().Exec(ctx, `insert into gateway_request_logs(request_id,endpoint,token_id,owner_user_id,token_owner_user_id,started_at,model_name,success,input_tokens,output_tokens,total_tokens,estimated_cost_usd,duration_ms)
 values ($1,'/v1/responses',$2,$3,$4,now(),'gpt-5.4',true,100,20,120,1.5,200),($1||'-foreign','/v1/responses',$2,$3,$3,now(),'gpt-5.4',true,100,20,120,9,200)`, fmt.Sprintf("query-usage-%d", time.Now().UnixNano()), token.ID, other.ID, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().UTC().Add(time.Hour)
	pct := 25.0
	window := quotaWindow5HSeconds
	allowed := true
	reached := false
	snapshot := codexQuotaSnapshot{FetchedAt: time.Now().UTC(), Allowed: &allowed, LimitReached: &reached, Windows: []codexQuotaWindow{{ID: "primary", LimitWindowSeconds: &window, UsedPercent: &pct, ResetAt: &until}}}
	if err := h.db.SaveQuotaSnapshot(ctx, token.ID, snapshot, token.PlanType, nil); err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("/api/v1/admin/accounts/%d", token.ID)
	stats := sub2APIGetTest(t, h, base+"/today-stats", k.PlaintextKey, 200).(map[string]any)
	if stats["requests"] != float64(1) || stats["tokens"] != float64(120) || stats["cost"] != 1.5 {
		t.Fatalf("owner usage=%v", stats)
	}
	daily := sub2APIGetTest(t, h, base+"/stats?days=3", k.PlaintextKey, 200).(map[string]any)
	if daily["summary"].(map[string]any)["total_cost"] != 1.5 || len(daily["history"].([]any)) != 3 {
		t.Fatal("history totals differ")
	}
	usage := sub2APIGetTest(t, h, base+"/usage?force=true", k.PlaintextKey, 200).(map[string]any)
	if usage["five_hour"].(map[string]any)["utilization"] != 25.0 {
		t.Fatal("quota projection mismatch")
	}
	quota := sub2APIGetTest(t, h, fmt.Sprintf("/api/v1/admin/openai/accounts/%d/quota", token.ID), k.PlaintextKey, 200).(map[string]any)
	if quota["rate_limit"].(map[string]any)["allowed"] != true {
		t.Fatal("quota state missing")
	}
	count := 0
	if err := h.db.Pool().QueryRow(ctx, "select count(*) from token_quota_snapshots where token_id=$1", token.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("GET query mutated quota facts")
	}
}
