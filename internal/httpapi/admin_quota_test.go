package httpapi

import (
	"testing"
	"time"
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
