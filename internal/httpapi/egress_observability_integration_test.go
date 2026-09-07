package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/egress"
)

func TestEgressObservationPolicyStorageAndPermissions(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	_, _ = h.db.Pool().Exec(ctx, `delete from gateway_settings where key=$1`, egress.PolicyKey)
	t.Cleanup(func() { _, _ = h.db.Pool().Exec(ctx, `delete from gateway_settings where key=$1`, egress.PolicyKey) })
	owner, key := h.createUser(t, "egress-owner")
	token := h.createToken(t, owner.ID, "egress-observer")
	request := func(method, path, auth, body string, status int) map[string]any {
		t.Helper()
		resp := h.request(t, method, path, auth, body)
		data := decodeResponseBody(t, resp)
		if resp.StatusCode != status {
			t.Fatalf("%s %s status=%d body=%v", method, path, resp.StatusCode, data)
		}
		return data
	}
	request("GET", "/admin/egress-observability", "", "", 401)
	request("POST", "/admin/egress-observability", key.PlaintextKey, `{"enabled":true}`, 403)
	request("GET", "/admin/egress-observations?request_id=private", key.PlaintextKey, "", 403)
	request("POST", "/admin/egress-observability", "service-test-key", `{"enabled":true,"success_sample_percent":101}`, 400)
	request("POST", "/admin/egress-observability", "service-test-key", `{"enabled":true,"arbitrary":"secret"}`, 400)
	request("POST", "/admin/settings/egress_observability", "service-test-key", `{"enabled":true}`, 400)
	request("DELETE", "/admin/settings/egress_observability", "service-test-key", "", 400)
	body := fmt.Sprintf(`{"enabled":true,"token_ids":[%d],"success_sample_percent":10}`, token.ID)
	request("POST", "/admin/egress-observability", "service-test-key", body, 200)
	loaded, err := h.db.GetEgressPolicy(ctx)
	if err != nil || !loaded.Enabled || len(loaded.TokenIDs) != 1 || loaded.TokenIDs[0] != token.ID {
		t.Fatalf("policy not persisted: %+v %v", loaded, err)
	}
	if !h.app.egressRecorder.Policy().Enabled {
		t.Fatal("policy not applied to runtime")
	}
	channelID, err := h.db.SaveProxyChannel(ctx, owner.ID, 0, "observed", "8.8.8.8:9999:fixture-user:fixture-password")
	if err != nil {
		t.Fatal(err)
	}
	request("POST", fmt.Sprintf("/api/tokens/%d/proxy", token.ID), key.PlaintextKey, fmt.Sprintf(`{"proxy_channel_id":%d}`, channelID), 200)
	u, snapshot, err := h.db.ResolveTokenProxySnapshot(ctx, token.ID)
	if err != nil || u == nil || snapshot.ChannelID != channelID || snapshot.BindingRevision == "" || snapshot.ChannelRevision == "" {
		t.Fatalf("snapshot: %+v %v", snapshot, err)
	}
	raw, _ := json.Marshal(snapshot)
	if string(raw) == "" {
		t.Fatal("snapshot missing")
	}
	id := fmt.Sprintf("egress-integration-%d", token.ID)
	record := egress.TraceRecord{SchemaVersion: 1, AttemptID: fmt.Sprintf("%032x", token.ID), RequestID: id, TraceID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", TokenID: token.ID, StartedAt: time.Now().UTC(), Route: snapshot, LocalStatus: 502, UpstreamStatus: 200, BodyReadError: "unexpected_eof"}
	data, _ := json.Marshal(record)
	if err := h.db.SaveEgressObservation(ctx, record, data); err != nil {
		t.Fatal(err)
	}
	if err := h.db.SaveEgressObservation(ctx, record, data); err != nil {
		t.Fatal(err)
	}
	got := request("GET", "/admin/egress-observations?request_id="+id, "service-test-key", "", 200)
	if len(got["items"].([]any)) != 1 {
		t.Fatal("duplicate attempt observation")
	}
	_, err = h.db.Pool().Exec(ctx, `update gateway_egress_observations set created_at=now()-interval '25 hours' where attempt_id=$1`, record.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.db.CleanupEgressObservations(ctx); err != nil {
		t.Fatal(err)
	}
	items, err := h.db.ListEgressObservations(ctx, id)
	if err != nil || len(items) != 0 {
		t.Fatalf("retention failed: %v %v", items, err)
	}
	// Observability can be disabled without modifying the serving proxy binding.
	request("POST", "/admin/egress-observability", "service-test-key", `{"enabled":false,"token_ids":[],"success_sample_percent":0}`, 200)
	bound, err := h.db.ResolveTokenProxy(ctx, token.ID)
	if err != nil || bound.String() != u.String() {
		t.Fatal("observation policy changed serving intent")
	}
}
