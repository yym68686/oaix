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
	if !hasReasoningEffort(payload.Models[0].SupportedReasoningLevels, "xhigh") {
		t.Fatalf("reasoning levels = %#v", payload.Models[0].SupportedReasoningLevels)
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

func hasReasoningEffort(levels []codexReasoningPreset, effort string) bool {
	for _, level := range levels {
		if level.Effort == effort {
			return true
		}
	}
	return false
}
