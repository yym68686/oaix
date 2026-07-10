package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yym68686/oaix/internal/config"
)

func TestModelsReturnsOpenAIList(t *testing.T) {
	handler := modelsTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer service-key")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q", contentType)
	}
	var payload openAIModelsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Object != "list" {
		t.Fatalf("object = %q", payload.Object)
	}
	wantIDs := []string{
		"gpt-5.4-mini",
		"gpt-5.4",
		"gpt-5.5",
		"gpt-5.6-sol",
		"gpt-5.6-terra",
		"gpt-5.6-luna",
		"gpt-image-2",
	}
	if len(payload.Data) != len(wantIDs) {
		t.Fatalf("models = %d, want %d: %#v", len(payload.Data), len(wantIDs), payload.Data)
	}
	for index, item := range payload.Data {
		if item.ID != wantIDs[index] || item.Object != "model" || item.Created <= 0 || item.OwnedBy != "oaix" {
			t.Fatalf("model[%d] = %#v", index, item)
		}
	}
}

func TestModelsReturnsCodexCatalogForClientVersion(t *testing.T) {
	handler := modelsTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.144.0", nil)
	request.Header.Set("Authorization", "Bearer service-key")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["data"]; ok {
		t.Fatalf("Codex catalog unexpectedly includes data: %s", response.Body.String())
	}
	var payload codexModelsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{
		"gpt-5.4-mini",
		"gpt-5.4",
		"gpt-5.5",
		"gpt-5.6-sol",
		"gpt-5.6-terra",
		"gpt-5.6-luna",
	}
	if len(payload.Models) != len(wantIDs) {
		t.Fatalf("models = %d, want %d: %#v", len(payload.Models), len(wantIDs), payload.Models)
	}
	for index, item := range payload.Models {
		if item.Slug != wantIDs[index] {
			t.Fatalf("model[%d].slug = %q, want %q", index, item.Slug, wantIDs[index])
		}
		if strings.Contains(item.Slug, "image") {
			t.Fatalf("Codex catalog includes image-only model %q", item.Slug)
		}
		if item.DisplayName == "" || item.ShellType != "shell_command" || item.Visibility != "list" || !item.SupportedInAPI {
			t.Fatalf("model[%d] metadata incomplete: %#v", index, item)
		}
		if item.BaseInstructions == "" || item.ApplyPatchToolType == nil || *item.ApplyPatchToolType != "freeform" {
			t.Fatalf("model[%d] runtime metadata incomplete: %#v", index, item)
		}
		if item.TruncationPolicy.Mode != "tokens" || item.TruncationPolicy.Limit <= 0 {
			t.Fatalf("model[%d] truncation policy = %#v", index, item.TruncationPolicy)
		}
		if !item.SupportsReasoningSummaries || len(item.SupportedReasoningLevels) == 0 || len(item.InputModalities) != 2 {
			t.Fatalf("model[%d] capabilities incomplete: %#v", index, item)
		}
	}
	wantReasoning := map[string][]string{
		"gpt-5.4-mini":  {"low", "medium", "high", "xhigh"},
		"gpt-5.4":       {"low", "medium", "high", "xhigh"},
		"gpt-5.5":       {"low", "medium", "high", "xhigh"},
		"gpt-5.6-sol":   {"low", "medium", "high", "xhigh", "max", "ultra"},
		"gpt-5.6-terra": {"low", "medium", "high", "xhigh", "max", "ultra"},
		"gpt-5.6-luna":  {"low", "medium", "high", "xhigh", "max"},
	}
	for _, model := range payload.Models {
		if got := reasoningEfforts(model.SupportedReasoningLevels); !equalStrings(got, wantReasoning[model.Slug]) {
			t.Fatalf("%s reasoning levels = %#v, want %#v", model.Slug, got, wantReasoning[model.Slug])
		}
		if model.DefaultReasoningLevel == nil {
			t.Fatalf("%s default reasoning level is nil", model.Slug)
		}
		wantDefault := "medium"
		if model.Slug == "gpt-5.6-sol" {
			wantDefault = "low"
		}
		if *model.DefaultReasoningLevel != wantDefault {
			t.Fatalf("%s default reasoning level = %q, want %q", model.Slug, *model.DefaultReasoningLevel, wantDefault)
		}
	}
	sol := payload.Models[3]
	if got := reasoningDescription(sol.SupportedReasoningLevels, "max"); got != "Maximum reasoning depth for the hardest problems" {
		t.Fatalf("max description = %q", got)
	}
	if got := reasoningDescription(sol.SupportedReasoningLevels, "ultra"); got != "Maximum reasoning with automatic task delegation" {
		t.Fatalf("ultra description = %q", got)
	}
}

func TestModelsTreatsBlankClientVersionAsOpenAIList(t *testing.T) {
	handler := modelsTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/models?client_version=%20", nil)
	request.Header.Set("Authorization", "Bearer service-key")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["data"]; !ok {
		t.Fatalf("blank client_version should return OpenAI list: %s", response.Body.String())
	}
}

func TestModelsRequiresValidAPIKey(t *testing.T) {
	handler := modelsTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer invalid")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
	}
}

func modelsTestHandler() http.Handler {
	app := NewApp(config.Config{
		Auth: config.AuthConfig{ServiceAPIKeys: []string{"service-key"}},
	}, nil, nil, nil, nil, nil)
	return app.Handler()
}

func reasoningEfforts(levels []codexReasoningPreset) []string {
	efforts := make([]string, 0, len(levels))
	for _, level := range levels {
		efforts = append(efforts, level.Effort)
	}
	return efforts
}

func reasoningDescription(levels []codexReasoningPreset, effort string) string {
	for _, level := range levels {
		if level.Effort == effort {
			return level.Description
		}
	}
	return ""
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}
