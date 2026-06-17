package store

import (
	"encoding/json"
	"testing"
)

func TestBuildTokenSelectionSettingsPreservesPythonPayload(t *testing.T) {
	raw := json.RawMessage(`{
		"strategy": "fill_first",
		"token_order": [12, "13", 12, -1, "bad"],
		"plan_order_enabled": "on",
		"plan_order": ["chatgpt_pro", "plus"],
		"active_stream_cap": 10,
		"unknown": "preserved"
	}`)
	settings := buildTokenSelectionSettings(raw, nil, 4)
	if settings.Strategy != TokenSelectionStrategyFillFirst {
		t.Fatalf("Strategy = %q, want %q", settings.Strategy, TokenSelectionStrategyFillFirst)
	}
	if settings.ActiveStreamCap != 10 {
		t.Fatalf("ActiveStreamCap = %d, want 10", settings.ActiveStreamCap)
	}
	if got := settings.TokenOrder; len(got) != 2 || got[0] != 12 || got[1] != 13 {
		t.Fatalf("TokenOrder = %#v, want [12 13]", got)
	}
	if !settings.PlanOrderEnabled {
		t.Fatal("PlanOrderEnabled = false, want true")
	}
	wantPlanOrder := []string{"pro", "plus", "free", "team"}
	if len(settings.PlanOrder) != len(wantPlanOrder) {
		t.Fatalf("PlanOrder = %#v, want %#v", settings.PlanOrder, wantPlanOrder)
	}
	for i := range wantPlanOrder {
		if settings.PlanOrder[i] != wantPlanOrder[i] {
			t.Fatalf("PlanOrder = %#v, want %#v", settings.PlanOrder, wantPlanOrder)
		}
	}

	payload := normalizeTokenSelectionPayload(raw, 4)
	if payload["unknown"] != "preserved" {
		t.Fatalf("unknown field = %#v, want preserved", payload["unknown"])
	}
	if payload["active_stream_cap"] != int64(10) {
		t.Fatalf("active_stream_cap = %#v, want int64(10)", payload["active_stream_cap"])
	}
}

func TestParseTokenActiveStreamCapRange(t *testing.T) {
	if _, err := ParseTokenActiveStreamCap(0); err == nil {
		t.Fatal("ParseTokenActiveStreamCap(0) returned nil error")
	}
	if _, err := ParseTokenActiveStreamCap(11); err == nil {
		t.Fatal("ParseTokenActiveStreamCap(11) returned nil error")
	}
	if got, err := ParseTokenActiveStreamCap(10); err != nil || got != 10 {
		t.Fatalf("ParseTokenActiveStreamCap(10) = %d, %v; want 10, nil", got, err)
	}
}
