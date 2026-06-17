package httpapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func TestParseCodexQuotaPayloadExtracts5HAnd7DWindows(t *testing.T) {
	now := time.Date(2026, 6, 17, 4, 0, 0, 0, time.UTC)
	payload := map[string]any{
		"plan_type": "chatgpt_pro",
		"rate_limit": map[string]any{
			"allowed": false,
			"primary_window": map[string]any{
				"limit_window_seconds": quotaWindow5HSeconds,
				"used_percent":         87.5,
				"reset_after_seconds":  float64(600),
			},
			"secondary_window": map[string]any{
				"limitWindowSeconds": quotaWindow7DSeconds,
				"usedPercent":        20,
				"resetAt":            "2026-06-18T04:00:00Z",
			},
		},
	}
	snapshot, err := parseCodexQuotaPayload(payload, now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PlanType == nil || *snapshot.PlanType != "pro" {
		t.Fatalf("plan type = %v", snapshot.PlanType)
	}
	if len(snapshot.Windows) != 2 {
		t.Fatalf("windows = %+v", snapshot.Windows)
	}
	if snapshot.Windows[0].Label != "5h" || snapshot.Windows[0].UsedPercent == nil || *snapshot.Windows[0].UsedPercent != 87.5 {
		t.Fatalf("5h window = %+v", snapshot.Windows[0])
	}
	if snapshot.Windows[0].RemainingPercent == nil || *snapshot.Windows[0].RemainingPercent != 12.5 {
		t.Fatalf("5h remaining = %+v", snapshot.Windows[0].RemainingPercent)
	}
	if snapshot.Windows[0].ResetAt == nil || !snapshot.Windows[0].ResetAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("5h reset = %v", snapshot.Windows[0].ResetAt)
	}
	if snapshot.Windows[1].Label != "7d" || snapshot.Windows[1].LimitWindowSeconds == nil || *snapshot.Windows[1].LimitWindowSeconds != quotaWindow7DSeconds {
		t.Fatalf("7d window = %+v", snapshot.Windows[1])
	}
}

func TestParseCodexQuotaPayloadDeducesExhaustedWindow(t *testing.T) {
	now := time.Date(2026, 6, 17, 4, 0, 0, 0, time.UTC)
	snapshot, err := parseCodexQuotaPayload(map[string]any{
		"rateLimit": map[string]any{
			"limitReached": true,
			"primaryWindow": map[string]any{
				"limitWindowSeconds": quotaWindow5HSeconds,
				"resetAfterSeconds":  120,
			},
		},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Windows) != 1 {
		t.Fatalf("windows = %+v", snapshot.Windows)
	}
	window := snapshot.Windows[0]
	if window.UsedPercent == nil || *window.UsedPercent != 100 {
		t.Fatalf("used_percent = %v", window.UsedPercent)
	}
	if !window.Exhausted {
		t.Fatalf("expected exhausted window: %+v", window)
	}
}

func TestQuotaStatusShouldRefresh(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		if !quotaStatusShouldRefresh(status) {
			t.Fatalf("status %d should refresh", status)
		}
	}
	for _, status := range []int{http.StatusOK, http.StatusTooManyRequests, http.StatusInternalServerError} {
		if quotaStatusShouldRefresh(status) {
			t.Fatalf("status %d should not refresh", status)
		}
	}
}

func TestQuotaResponseShouldDisableOnlyForDeactivatedWorkspace(t *testing.T) {
	if !quotaResponseShouldDisable(http.StatusPaymentRequired, []byte(`{"error":{"code":"deactivated_workspace"}}`)) {
		t.Fatal("expected deactivated workspace to disable token")
	}
	if !quotaResponseShouldDisable(http.StatusPaymentRequired, []byte(`{"detail":"deactivated_workspace"}`)) {
		t.Fatal("expected deactivated workspace detail to disable token")
	}
	if quotaResponseShouldDisable(http.StatusPaymentRequired, []byte(`{"error":{"code":"usage_limit_reached"}}`)) {
		t.Fatal("usage limit should not disable token")
	}
	if quotaResponseShouldDisable(http.StatusTooManyRequests, []byte(`{"error":{"code":"deactivated_workspace"}}`)) {
		t.Fatal("non-402 should not disable token")
	}
}

func TestQuotaErrorSnapshotMarksDeactivatedWorkspace(t *testing.T) {
	snapshot := quotaErrorSnapshot(time.Now().UTC(), "HTTP 402: deactivated_workspace")
	if !snapshot.Disabled {
		t.Fatal("expected snapshot to be disabled")
	}
}

func TestAdminTokenStatusUsesServerTime(t *testing.T) {
	now := time.Date(2026, 6, 17, 17, 30, 0, 0, time.UTC)
	pastCooldown := now.Add(-time.Second)
	futureCooldown := now.Add(time.Second)
	disabledAt := now.Add(-time.Minute)

	tests := []struct {
		name  string
		token store.Token
		want  string
	}{
		{
			name: "active when cooldown has expired",
			token: store.Token{
				IsActive:      true,
				CooldownUntil: &pastCooldown,
			},
			want: "active",
		},
		{
			name: "cooling when cooldown is in the future",
			token: store.Token{
				IsActive:      true,
				CooldownUntil: &futureCooldown,
			},
			want: "cooling",
		},
		{
			name: "disabled when inactive",
			token: store.Token{
				IsActive: false,
			},
			want: "disabled",
		},
		{
			name: "disabled when disabled_at is set",
			token: store.Token{
				IsActive:   true,
				DisabledAt: &disabledAt,
			},
			want: "disabled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := adminTokenStatus(test.token, now); got != test.want {
				t.Fatalf("status = %s, want %s", got, test.want)
			}
		})
	}
}
