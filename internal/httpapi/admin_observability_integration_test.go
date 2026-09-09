package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/admindiag"
)

func TestAdminObservationPolicyPermissionsAndCompleteResponse(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	h.db.Pool().Exec(ctx, `delete from gateway_settings where key=$1`, admindiag.PolicyKey)
	t.Cleanup(func() { h.db.Pool().Exec(ctx, `delete from gateway_settings where key=$1`, admindiag.PolicyKey) })
	if _, err := h.db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatal(err)
	}
	owner, key := h.createUser(t, "diagnostics-user")
	readonly, err := h.db.CreateAPIKey(ctx, &owner.ID, "user", "read diagnostics", "readonly_admin", &owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, key, body string, status int) map[string]any {
		t.Helper()
		return expectStatus(t, h.request(t, method, path, key, body), status)
	}
	request("GET", "/admin/query-observability", "", "", 401)
	request("GET", "/admin/query-observations?request_id=x", key.PlaintextKey, "", 403)
	request("POST", "/admin/query-observability", key.PlaintextKey, `{"enabled":true,"max_records_per_minute":60}`, 403)
	request("GET", "/admin/query-observability", readonly.PlaintextKey, "", 200)
	request("POST", "/admin/query-observability", readonly.PlaintextKey, `{"enabled":true,"max_records_per_minute":60}`, 403)
	request("POST", "/admin/query-observability", "service-test-key", `{"enabled":true,"max_records_per_minute":1000}`, 400)
	request("POST", "/admin/query-observability", "service-test-key", `{"arbitrary":"secret"}`, 400)
	request("POST", "/admin/settings/admin_query_observability", "service-test-key", `{}`, 400)
	request("DELETE", "/admin/settings/admin_query_observability", "service-test-key", "", 400)
	request("POST", "/admin/query-observability", "service-test-key", `{"enabled":true,"success_sample_percent":100,"max_records_per_minute":60}`, 200)
	c, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := make(chan struct{})
	go func() { defer close(worker); h.app.adminRecorder.Run(c, h.db.SaveAdminObservation, nil) }()
	defer func() { cancel(); <-worker }()
	requestID := fmt.Sprintf("admin-observation-test-%d", time.Now().UnixNano())
	req, _ := http.NewRequest("GET", h.server.URL+"/api/admin/pool-summary/by-user?limit=5", nil)
	req.Header.Set("Authorization", "Bearer service-test-key")
	req.Header.Set("X-Request-ID", requestID)
	req.Header.Set("X-OAIX-Page-Load-ID", "page-one")
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	payload := decodeResponseBody(t, resp)
	if resp.StatusCode != 200 || payload["usage_included"] != true {
		t.Fatal(resp.StatusCode, payload)
	}
	deadline := time.Now().Add(time.Second)
	for h.app.adminRecorder.Stats().Stored == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	items, err := h.db.ListAdminObservations(ctx, requestID)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	var d admindiag.Record
	if err = json.Unmarshal(items[0], &d); err != nil {
		t.Fatal(err)
	}
	if d.PageLoadID != "page-one" || d.Status != 200 || len(d.Events) == 0 {
		t.Fatal(d)
	}
	stages := map[string]bool{}
	for _, e := range d.Events {
		stages[e.Stage] = true
	}
	for _, name := range []string{"user_list", "pool", "usage", "cost_aggregate", "cost_reconcile_marker", "sub2api_cost"} {
		if !stages[name] {
			t.Fatalf("missing %s: %v", name, stages)
		}
	}
	request("POST", "/admin/query-observability", "service-test-key", `{"enabled":false,"success_sample_percent":0,"max_records_per_minute":0}`, 200)
	if h.app.adminRecorder.Policy().Enabled {
		t.Fatal("disable requires restart")
	}
}
