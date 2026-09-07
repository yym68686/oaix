package httpapi

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func TestTokenDetailsLoadConcurrentlyAndWaitForCompleteData(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := h.db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatal(err)
	}
	user, _ := h.createUser(t, "parallel-details")
	token := h.createToken(t, user.ID, "parallel-details")
	finished := time.Now().UTC()
	cost := 12.345678
	if err := h.db.UpsertRequestLogs(ctx, []store.RequestLog{{
		RequestID: fmt.Sprintf("parallel-details-%d", token.ID), OwnerUserID: &user.ID,
		TokenID: &token.ID, EstimatedCostUSD: &cost, Endpoint: "/v1/responses",
		StartedAt: finished.Add(-time.Second), FinishedAt: &finished,
	}}); err != nil {
		t.Fatal(err)
	}
	quotaStarted := make(chan struct{}, 1)
	quotaRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseQuota := func() { releaseOnce.Do(func() { close(quotaRelease) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		quotaStarted <- struct{}{}
		select {
		case <-quotaRelease:
			_, _ = w.Write([]byte(`{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000}}}`))
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	defer releaseQuota()
	h.app.quota.usageURL = upstream.URL
	tx, err := h.db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "lock table gateway_request_token_costs in access exclusive mode"); err != nil {
		t.Fatal(err)
	}
	done := make(chan []adminTokenItem, 1)
	go func() {
		items, _ := h.app.adminTokenItems(ctx, []store.Token{token}, true)
		done <- items
	}()
	select {
	case <-quotaStarted:
	case <-ctx.Done():
		t.Fatal("quota request did not start")
	}
	// The cost lookup must reach its lock while quota is still blocked.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := h.db.Pool().QueryRow(ctx, `select exists (
			select 1 from pg_locks where relation = 'gateway_request_token_costs'::regclass and not granted
		)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cost lookup waited for quota instead of starting concurrently")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("returned incomplete data while quota was still blocked")
	case <-time.After(50 * time.Millisecond):
	}
	releaseQuota()
	select {
	case items := <-done:
		if len(items) != 1 || items[0].Quota == nil || items[0].QuotaFetchState != quotaFetchStateReady {
			t.Fatalf("incomplete quota response: %+v", items)
		}
		if got := items[0].LocalObservedCostUSD; got == nil || math.Abs(*got-cost) > 1e-9 {
			t.Fatalf("local cost = %v, want %v", got, cost)
		}
		if items[0].CombinedObservedCostUSD == nil || items[0].Sub2APIObservedCostUSD == nil || items[0].ActiveStreamCap <= 0 {
			t.Fatalf("incomplete usage or concurrency response: %+v", items[0])
		}
	case <-ctx.Done():
		t.Fatal("complete data did not return")
	}
}

func TestTokenCostAPIsRepairLateFinalizedRequestIDReuse(t *testing.T) {
	h := newMultiUserHarness(t)
	ctx := context.Background()
	if _, err := h.db.ReconcileRecordedTokenCosts(ctx); err != nil {
		t.Fatalf("reconcile recorded token costs: %v", err)
	}
	user, key := h.createUser(t, "late-token-cost")
	token := h.createToken(t, user.ID, "late-token-cost")

	requestID := fmt.Sprintf("late-token-cost-%d", time.Now().UnixNano())
	startedAt := time.Now().UTC().Add(-2 * time.Minute)
	earlyFinishedAt := startedAt.Add(time.Second)
	earlyStatus := http.StatusServiceUnavailable
	earlySuccess := false
	if err := h.db.UpsertRequestLogs(ctx, []store.RequestLog{{
		RequestID:   requestID,
		OwnerUserID: &user.ID,
		Endpoint:    "/v1/responses",
		StartedAt:   startedAt,
		FinishedAt:  &earlyFinishedAt,
		StatusCode:  &earlyStatus,
		Success:     &earlySuccess,
	}}); err != nil {
		t.Fatalf("write early request log: %v", err)
	}
	if _, err := h.db.AggregateRequestHourlyStats(ctx); err != nil {
		t.Fatalf("aggregate early request log: %v", err)
	}

	lateFinishedAt := time.Now().UTC().Add(time.Minute)
	lateStatus := http.StatusOK
	lateSuccess := true
	wantCost := 5.769245
	if err := h.db.UpsertRequestLogs(ctx, []store.RequestLog{{
		RequestID:        requestID,
		OwnerUserID:      &user.ID,
		TokenOwnerUserID: &user.ID,
		Endpoint:         "/v1/responses",
		StartedAt:        startedAt,
		FinishedAt:       &lateFinishedAt,
		StatusCode:       &lateStatus,
		Success:          &lateSuccess,
		TokenID:          &token.ID,
		EstimatedCostUSD: &wantCost,
	}}); err != nil {
		t.Fatalf("write late request log: %v", err)
	}

	aggregateCosts, err := h.db.TokenObservedCostsAggregateSnapshot(ctx, []store.Token{token})
	if err != nil {
		t.Fatalf("read aggregate token costs: %v", err)
	}
	if got := aggregateCosts[token.ID]; got == nil || math.Abs(*got) > 1e-9 {
		t.Fatalf("aggregate token cost = %#v, want 0 before late-finalized repair", got)
	}
	currentCosts, err := h.db.TokenObservedCostsCurrentSnapshot(ctx, []store.Token{token})
	if err != nil {
		t.Fatalf("read current token costs: %v", err)
	}
	if got := currentCosts[token.ID]; got == nil || math.Abs(*got-wantCost) > 1e-9 {
		t.Fatalf("current token cost = %#v, want %.9f", got, wantCost)
	}

	assertTokenCost := func(t *testing.T, item map[string]any) {
		t.Helper()
		got, ok := item["local_observed_cost_usd"].(float64)
		if !ok || math.Abs(got-wantCost) > 1e-9 {
			t.Fatalf("local_observed_cost_usd = %#v, want %.9f", item["local_observed_cost_usd"], wantCost)
		}
	}

	listPayload := expectStatus(t, h.request(t, http.MethodGet, "/api/tokens?limit=20", key.PlaintextKey, ""), http.StatusOK)
	listItems, _ := listPayload["items"].([]any)
	var listed map[string]any
	for _, raw := range listItems {
		item, _ := raw.(map[string]any)
		if int64(item["id"].(float64)) == token.ID {
			listed = item
			break
		}
	}
	if listed == nil {
		t.Fatalf("token %d not returned by key list: %#v", token.ID, listItems)
	}
	assertTokenCost(t, listed)

	costPayload := expectStatus(t, h.request(t, http.MethodGet, "/admin/tokens/costs?ids="+strconv.FormatInt(token.ID, 10), "service-test-key", ""), http.StatusOK)
	costItems, _ := costPayload["items"].([]any)
	if len(costItems) != 1 {
		t.Fatalf("token cost items = %#v, want one item", costItems)
	}
	assertTokenCost(t, costItems[0].(map[string]any))
}
