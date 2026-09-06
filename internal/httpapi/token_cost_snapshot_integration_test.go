package httpapi

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

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
