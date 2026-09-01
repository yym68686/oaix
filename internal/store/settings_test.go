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
	if _, err := ParseTokenActiveStreamCap(51); err == nil {
		t.Fatal("ParseTokenActiveStreamCap(51) returned nil error")
	}
	if got, err := ParseTokenActiveStreamCap(50); err != nil || got != 50 {
		t.Fatalf("ParseTokenActiveStreamCap(50) = %d, %v; want 50, nil", got, err)
	}
}

func TestParseTokenConcurrencyPayloadNormalizesPlans(t *testing.T) {
	settings, err := ParseTokenConcurrencyPayload([]byte(`{
		"plan_concurrency": {
			"chatgpt_pro": 8,
			" PLUS ": 4,
			"k12": 2
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(settings) != 3 || settings["pro"] != 8 || settings["plus"] != 4 || settings["k12"] != 2 {
		t.Fatalf("settings = %#v", settings)
	}
}

func TestParseTokenConcurrencyPayloadRejectsInvalidValues(t *testing.T) {
	tests := []string{
		`{"plan_concurrency":{"pro":0}}`,
		`{"plan_concurrency":{"pro":51}}`,
		`{"plan_concurrency":{"pro":1.5}}`,
		`{"plan_concurrency":{"all":5}}`,
		`{"plan_concurrency":{"pro":2,"chatgpt_pro":3}}`,
		`{"plan_concurrency":{"":5}}`,
		`{"plan_concurrency":{},"unknown":true}`,
		`{"plan_concurrency":{}} {"plan_concurrency":{}}`,
	}
	for _, raw := range tests {
		if _, err := ParseTokenConcurrencyPayload([]byte(raw)); err == nil {
			t.Fatalf("ParseTokenConcurrencyPayload(%s) returned nil error", raw)
		}
	}
}

func TestBuildUserTokenConcurrencySettingsIgnoresCorruptEntries(t *testing.T) {
	settings := buildUserTokenConcurrencySettings(json.RawMessage(`{
		"plan_concurrency": {"pro": 8, "plus": 0, "team": 51, "free": "bad"}
	}`))
	if len(settings.PlanConcurrency) != 1 || settings.PlanConcurrency["pro"] != 8 {
		t.Fatalf("settings = %#v", settings.PlanConcurrency)
	}
	pro := "chatgpt_pro"
	if cap, ok := settings.ActiveStreamCapForPlan(&pro); !ok || cap != 8 {
		t.Fatalf("pro cap = %d, %v", cap, ok)
	}
}
