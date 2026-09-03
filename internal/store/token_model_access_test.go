package store

import (
	"encoding/json"
	"testing"
)

func TestParseTokenModelAccessPayloadNormalizesPlans(t *testing.T) {
	settings, err := ParseTokenModelAccessPayload([]byte(`{"plan_models":{"CHATGPT_PRO":[" GPT-5.5 ","gpt-5.5"],"plus":[]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(settings) != 2 || len(settings["pro"]) != 1 || settings["pro"][0] != "gpt-5.5" {
		t.Fatalf("settings=%#v", settings)
	}
	if settings["plus"] == nil {
		t.Fatal("empty explicit plan list must remain present")
	}
}

func TestParseTokenModelAccessPayloadRejectsUnknownFieldsAndPlans(t *testing.T) {
	for _, raw := range []string{
		`{"plan_models":{"all":["gpt-5.5"]}}`,
		`{"plan_models":{" ":["gpt-5.5"]}}`,
		`{"plan_models":{"pro":["bad model"]}}`,
		`{"plan_models":{},"extra":true}`,
	} {
		if _, err := ParseTokenModelAccessPayload([]byte(raw)); err == nil {
			t.Fatalf("payload %s was accepted", raw)
		}
	}
}

func TestBuildTokenModelAccessSettingsIgnoresCorruptEntries(t *testing.T) {
	settings := BuildTokenModelAccessSettings(json.RawMessage(`{"plan_models":{"pro":["gpt-5.5"],"free":["bad model"]}}`))
	if len(settings.PlanModels) != 1 || settings.PlanModels["pro"][0] != "gpt-5.5" {
		t.Fatalf("settings=%#v", settings.PlanModels)
	}
}
