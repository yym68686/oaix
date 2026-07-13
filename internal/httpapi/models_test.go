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
	type modelCapabilities struct {
		displayName       string
		defaultReasoning  string
		defaultVerbosity  string
		contextWindow     int64
		maxContextWindow  int64
		priority          int
		compHash          string
		speedTiers        int
		serviceTiers      int
		includeSkills     bool
		useResponsesLite  bool
		toolMode          string
		multiAgentVersion string
	}
	wantCapabilities := map[string]modelCapabilities{
		"gpt-5.4-mini": {
			displayName: "GPT-5.4-Mini", defaultReasoning: "medium", defaultVerbosity: "medium",
			contextWindow: 272000, maxContextWindow: 272000, priority: 23, compHash: "2911",
		},
		"gpt-5.4": {
			displayName: "GPT-5.4", defaultReasoning: "medium", defaultVerbosity: "low",
			contextWindow: 272000, maxContextWindow: 1000000, priority: 16, compHash: "2911",
		},
		"gpt-5.5": {
			displayName: "GPT-5.5", defaultReasoning: "medium", defaultVerbosity: "low",
			contextWindow: 272000, maxContextWindow: 272000, priority: 7, compHash: "2911",
			includeSkills: true,
		},
		"gpt-5.6-sol": {
			displayName: "GPT-5.6-Sol", defaultReasoning: "low", defaultVerbosity: "low",
			contextWindow: 372000, maxContextWindow: 372000, priority: 1, compHash: "3000",
			useResponsesLite: true, toolMode: "code_mode_only", multiAgentVersion: "v2",
		},
		"gpt-5.6-terra": {
			displayName: "GPT-5.6-Terra", defaultReasoning: "medium", defaultVerbosity: "low",
			contextWindow: 372000, maxContextWindow: 372000, priority: 2, compHash: "3000",
			useResponsesLite: true, toolMode: "code_mode_only", multiAgentVersion: "v2",
		},
		"gpt-5.6-luna": {
			displayName: "GPT-5.6-Luna", defaultReasoning: "medium", defaultVerbosity: "low",
			contextWindow: 372000, maxContextWindow: 372000, priority: 3, compHash: "3000",
			useResponsesLite: true, toolMode: "code_mode_only", multiAgentVersion: "v1",
		},
	}
	for _, model := range payload.Models {
		want := wantCapabilities[model.Slug]
		if got := reasoningEfforts(model.SupportedReasoningLevels); !equalStrings(got, wantReasoning[model.Slug]) {
			t.Fatalf("%s reasoning levels = %#v, want %#v", model.Slug, got, wantReasoning[model.Slug])
		}
		if model.DefaultReasoningLevel == nil {
			t.Fatalf("%s default reasoning level is nil", model.Slug)
		}
		if *model.DefaultReasoningLevel != want.defaultReasoning {
			t.Fatalf("%s default reasoning level = %q, want %q", model.Slug, *model.DefaultReasoningLevel, want.defaultReasoning)
		}
		if model.DefaultVerbosity == nil || *model.DefaultVerbosity != want.defaultVerbosity {
			t.Fatalf("%s default verbosity = %#v, want %q", model.Slug, model.DefaultVerbosity, want.defaultVerbosity)
		}
		if model.DisplayName != want.displayName ||
			model.ContextWindow != want.contextWindow ||
			model.MaxContextWindow != want.maxContextWindow ||
			model.Priority != want.priority {
			t.Fatalf("%s model profile = %#v, want %#v", model.Slug, model, want)
		}
		if model.CompHash == nil || *model.CompHash != want.compHash {
			t.Fatalf("%s comp hash = %#v, want %q", model.Slug, model.CompHash, want.compHash)
		}
		if model.DefaultReasoningSummary != "none" ||
			!model.SupportVerbosity ||
			model.WebSearchToolType != "text_and_image" ||
			!model.SupportsSearchTool {
			t.Fatalf("%s shared capabilities incomplete: %#v", model.Slug, model)
		}
		if len(model.AdditionalSpeedTiers) != want.speedTiers ||
			len(model.ServiceTiers) != want.serviceTiers ||
			model.IncludeSkillsInstructions != want.includeSkills ||
			model.UseResponsesLite != want.useResponsesLite {
			t.Fatalf("%s tier/runtime capabilities = %#v, want %#v", model.Slug, model, want)
		}
		if got := optionalString(model.ToolMode); got != want.toolMode {
			t.Fatalf("%s tool mode = %q, want %q", model.Slug, got, want.toolMode)
		}
		if got := optionalString(model.MultiAgentVersion); got != want.multiAgentVersion {
			t.Fatalf("%s multi-agent version = %q, want %q", model.Slug, got, want.multiAgentVersion)
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

func TestStaticCatalogOnlyAdvertisesCachedFastModels(t *testing.T) {
	payload := codexModelsCatalog([]string{"gpt-5.4", "gpt-5.5", "gpt-5.6-sol"}, map[string]struct{}{
		"gpt-5.5": {},
	})
	if len(payload.Models) != 3 {
		t.Fatalf("models = %d", len(payload.Models))
	}
	for _, model := range payload.Models {
		wantFast := model.Slug == "gpt-5.5"
		gotFast := len(model.ServiceTiers) > 0 && len(model.AdditionalSpeedTiers) > 0
		if gotFast != wantFast {
			t.Fatalf("%s Fast = %t, want %t", model.Slug, gotFast, wantFast)
		}
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

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
