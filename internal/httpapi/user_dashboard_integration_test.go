package httpapi

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func TestDashboardCalendarAndCustomBoundariesWithDatabase(t *testing.T) {
	h := newMultiUserHarness(t)
	user, key := h.createUser(t, "dashboard-boundaries")
	other, _ := h.createUser(t, "dashboard-other")
	ctx := context.Background()
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	date := func(value string) time.Time {
		t.Helper()
		parsed, err := time.ParseInLocation("2006-01-02 15:04", value, location)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	for _, fixture := range []struct {
		when  string
		owner int64
	}{
		{"2024-12-29 23:00", user.ID},
		{"2024-12-30 00:00", user.ID},
		{"2025-01-01 00:00", user.ID},
		{"2025-01-02 00:00", user.ID},
		{"2025-01-02 23:00", user.ID},
		{"2025-01-03 00:00", user.ID},
		{"2025-01-02 00:00", other.ID},
	} {
		_, err := h.db.Pool().Exec(ctx, `insert into gateway_request_hourly_stats
		(owner_user_id, bucket_start, model_name, request_count, total_tokens, input_tokens, cached_input_tokens, estimated_cost_usd)
		values ($1, $2, 'boundary-model', 1, 120, 100, 50, 1.25)`, fixture.owner, date(fixture.when))
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		name    string
		opts    store.UserDashboardOptions
		count   int64
		buckets int
	}{
		{"today", store.UserDashboardOptions{Range: store.UserDashboardToday}, 1, 2},
		{"week", store.UserDashboardOptions{Range: store.UserDashboardWeek}, 3, 4},
		{"month", store.UserDashboardOptions{Range: store.UserDashboardMonth}, 2, 2},
		{"year", store.UserDashboardOptions{Range: store.UserDashboardYear}, 2, 1},
		{"custom", store.UserDashboardOptions{Range: store.UserDashboardCustom, CustomFrom: date("2024-12-30 00:00"), CustomTo: date("2025-01-02 00:00")}, 4, 4},
		{"single-day", store.UserDashboardOptions{Range: store.UserDashboardCustom, CustomFrom: date("2025-01-02 00:00"), CustomTo: date("2025-01-02 00:00")}, 2, 24},
		{"empty", store.UserDashboardOptions{Range: store.UserDashboardCustom, CustomFrom: date("2024-12-31 00:00"), CustomTo: date("2024-12-31 00:00")}, 0, 24},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.opts.Location = location
			test.opts.Now = date("2025-01-02 01:30")
			if test.opts.Range == store.UserDashboardCustom {
				test.opts.Now = date("2025-01-04 00:30")
			}
			payload, err := h.db.UserDashboardScoped(ctx, store.OwnerResources(user.ID), test.opts)
			if err != nil {
				t.Fatal(err)
			}
			period := payload.Periods[string(payload.Range)]
			if period.RequestCount != test.count || period.TotalTokens != test.count*120 || math.Abs(period.EstimatedCostUSD-float64(test.count)*1.25) > 1e-9 {
				t.Fatalf("summary = %+v, want %d requests", period, test.count)
			}
			if len(payload.Trend) != test.buckets {
				t.Fatalf("trend buckets = %d, want %d", len(payload.Trend), test.buckets)
			}
			var modelCount, trendCount int64
			for _, model := range payload.Models {
				modelCount += model.RequestCount
			}
			for _, point := range payload.Trend {
				trendCount += point.RequestCount
			}
			if modelCount != test.count || trendCount != test.count {
				t.Fatalf("summary/trend/model mismatch: %d/%d/%d", period.RequestCount, trendCount, modelCount)
			}
		})
	}
	_, err = h.db.UserDashboardScoped(ctx, store.OwnerResources(user.ID), store.UserDashboardOptions{
		Range: store.UserDashboardCustom, Location: location, Now: date("2025-01-04 00:30"),
		CustomFrom: date("2025-01-03 00:00"), CustomTo: date("2025-01-02 00:00"),
	})
	if !errors.Is(err, store.ErrInvalidDashboardDates) {
		t.Fatalf("reversed dates accepted: %v", err)
	}
	for _, query := range []string{
		"range=custom", "range=custom&from=bad&to=2025-01-02", "range=custom&from=2025-01-03&to=2025-01-02",
		"range=custom&from=2025-02-30&to=2025-03-01", "range=invalid",
	} {
		expectStatus(t, h.request(t, http.MethodGet, "/api/me/dashboard?"+query, key.PlaintextKey, ""), http.StatusBadRequest)
	}
	response := h.request(t, http.MethodGet, "/api/me/dashboard?range=custom&from=2024-12-30&to=2025-01-02&timezone=Asia%2FShanghai", key.PlaintextKey, "")
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("dashboard must not be cached")
	}
	payload := expectStatus(t, response, http.StatusOK)["dashboard"].(map[string]any)
	if payload["custom_from"] != "2024-12-30" || payload["custom_to"] != "2025-01-02" {
		t.Fatalf("custom dates = %v", payload)
	}
}
