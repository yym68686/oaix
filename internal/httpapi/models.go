package httpapi

import (
	"net/http"
	"strings"
	"time"
)

const (
	codexDefaultContextWindow int64 = 272000
	codexBaseInstructions           = "You are Codex, a coding agent. You and the user share one workspace, and your job is to collaborate with them until their goal is genuinely handled.\n\n" +
		"Read the codebase before making changes, prefer existing project patterns, and keep edits focused on the requested behavior. Use fast search tools such as rg when available. Preserve unrelated user changes and never run destructive git commands unless the user explicitly asks for them.\n\n" +
		"When editing files, use patch-oriented changes, keep comments sparse and useful, and avoid broad refactors unless they are required for the fix. Validate your work with the narrowest relevant tests first, then broader tests when risk warrants it. If a command fails, inspect the failure and continue from the concrete cause.\n\n" +
		"Communicate concise progress while working and finish with the important outcome, changed files, verification performed, and any remaining risk."
)

var (
	codexReasoningLow    = "low"
	codexReasoningMedium = "medium"
	codexApplyPatchTool  = "freeform"
)

type openAIModelsResponse struct {
	Object string            `json:"object"`
	Data   []openAIModelInfo `json:"data"`
}

type openAIModelInfo struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

type codexModelsResponse struct {
	Models []codexModelInfo `json:"models"`
}

type codexModelInfo struct {
	Slug                        string                 `json:"slug"`
	DisplayName                 string                 `json:"display_name"`
	Description                 string                 `json:"description,omitempty"`
	DefaultReasoningLevel       *string                `json:"default_reasoning_level,omitempty"`
	SupportedReasoningLevels    []codexReasoningPreset `json:"supported_reasoning_levels"`
	ShellType                   string                 `json:"shell_type"`
	Visibility                  string                 `json:"visibility"`
	SupportedInAPI              bool                   `json:"supported_in_api"`
	Priority                    int                    `json:"priority"`
	AdditionalSpeedTiers        []string               `json:"additional_speed_tiers"`
	ServiceTiers                []codexServiceTier     `json:"service_tiers"`
	DefaultServiceTier          *string                `json:"default_service_tier,omitempty"`
	AvailabilityNux             any                    `json:"availability_nux"`
	Upgrade                     any                    `json:"upgrade"`
	BaseInstructions            string                 `json:"base_instructions"`
	ModelMessages               any                    `json:"model_messages,omitempty"`
	SupportsReasoningSummaries  bool                   `json:"supports_reasoning_summaries"`
	DefaultReasoningSummary     string                 `json:"default_reasoning_summary"`
	SupportVerbosity            bool                   `json:"support_verbosity"`
	DefaultVerbosity            *string                `json:"default_verbosity"`
	ApplyPatchToolType          *string                `json:"apply_patch_tool_type"`
	WebSearchToolType           string                 `json:"web_search_tool_type"`
	TruncationPolicy            codexTruncationPolicy  `json:"truncation_policy"`
	SupportsParallelToolCalls   bool                   `json:"supports_parallel_tool_calls"`
	SupportsImageDetailOriginal bool                   `json:"supports_image_detail_original"`
	ContextWindow               int64                  `json:"context_window"`
	MaxContextWindow            int64                  `json:"max_context_window"`
	AutoCompactTokenLimit       *int64                 `json:"auto_compact_token_limit"`
	CompHash                    *string                `json:"comp_hash,omitempty"`
	EffectiveContextWindowPct   int64                  `json:"effective_context_window_percent"`
	ExperimentalSupportedTools  []string               `json:"experimental_supported_tools"`
	InputModalities             []string               `json:"input_modalities"`
	SupportsSearchTool          bool                   `json:"supports_search_tool"`
	UseResponsesLite            bool                   `json:"use_responses_lite"`
	AutoReviewModelOverride     *string                `json:"auto_review_model_override,omitempty"`
	ToolMode                    *string                `json:"tool_mode,omitempty"`
	MultiAgentVersion           *string                `json:"multi_agent_version,omitempty"`
}

type codexReasoningPreset struct {
	Effort      string `json:"effort"`
	Description string `json:"description"`
}

type codexServiceTier struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type codexTruncationPolicy struct {
	Mode  string `json:"mode"`
	Limit int64  `json:"limit"`
}

func (a *App) models(w http.ResponseWriter, r *http.Request) {
	modelIDs := advertisedModelIDs()
	if strings.TrimSpace(r.URL.Query().Get("client_version")) != "" {
		writeJSON(w, http.StatusOK, codexModelsCatalog(modelIDs))
		return
	}

	created := time.Now().UnixMilli()
	data := make([]openAIModelInfo, 0, len(modelIDs))
	for _, id := range modelIDs {
		data = append(data, openAIModelInfo{
			ID:      id,
			Object:  "model",
			Created: created,
			OwnedBy: "oaix",
		})
	}
	writeJSON(w, http.StatusOK, openAIModelsResponse{Object: "list", Data: data})
}

func advertisedModelIDs() []string {
	modelIDs := make([]string, 0, len(supportedTokenProbeModels)+1)
	modelIDs = append(modelIDs, supportedTokenProbeModels...)
	return append(modelIDs, "gpt-image-2")
}

func codexModelsCatalog(modelIDs []string) codexModelsResponse {
	models := make([]codexModelInfo, 0, len(modelIDs))
	for _, id := range modelIDs {
		id = strings.TrimSpace(id)
		if !isCodexCatalogModelID(id) {
			continue
		}
		models = append(models, codexModelInfoForID(id, len(models)))
	}
	return codexModelsResponse{Models: models}
}

func codexModelInfoForID(id string, priority int) codexModelInfo {
	reasoningLevels := []codexReasoningPreset{}
	defaultReasoningLevel := (*string)(nil)
	supportsReasoningSummaries := false
	if codexModelSupportsReasoning(id) {
		defaultReasoningLevel = codexDefaultReasoningLevel(id)
		reasoningLevels = codexReasoningLevels(id)
		supportsReasoningSummaries = true
	}

	inputModalities := []string{"text"}
	supportsImageDetailOriginal := false
	if codexModelSupportsImages(id) {
		inputModalities = []string{"text", "image"}
		supportsImageDetailOriginal = true
	}

	return codexModelInfo{
		Slug:                       id,
		DisplayName:                id,
		Description:                "Available through OAIX.",
		DefaultReasoningLevel:      defaultReasoningLevel,
		SupportedReasoningLevels:   reasoningLevels,
		ShellType:                  "shell_command",
		Visibility:                 "list",
		SupportedInAPI:             true,
		Priority:                   priority,
		AdditionalSpeedTiers:       []string{"fast"},
		ServiceTiers:               []codexServiceTier{{ID: "priority", Name: "Fast", Description: "1.5x speed, increased usage"}},
		AvailabilityNux:            nil,
		Upgrade:                    nil,
		BaseInstructions:           codexBaseInstructions,
		SupportsReasoningSummaries: supportsReasoningSummaries,
		DefaultReasoningSummary:    "auto",
		SupportVerbosity:           false,
		DefaultVerbosity:           nil,
		ApplyPatchToolType:         &codexApplyPatchTool,
		WebSearchToolType:          "text",
		TruncationPolicy: codexTruncationPolicy{
			Mode:  "tokens",
			Limit: 10000,
		},
		SupportsParallelToolCalls:   true,
		SupportsImageDetailOriginal: supportsImageDetailOriginal,
		ContextWindow:               codexDefaultContextWindow,
		MaxContextWindow:            codexDefaultContextWindow,
		AutoCompactTokenLimit:       nil,
		EffectiveContextWindowPct:   95,
		ExperimentalSupportedTools:  []string{},
		InputModalities:             inputModalities,
		SupportsSearchTool:          false,
		UseResponsesLite:            false,
		MultiAgentVersion:           codexMultiAgentVersion(id),
	}
}

func codexDefaultReasoningLevel(id string) *string {
	if codexModelMatchesFamily(id, "gpt-5.6-sol") {
		return &codexReasoningLow
	}
	return &codexReasoningMedium
}

func codexReasoningLevels(id string) []codexReasoningPreset {
	levels := []codexReasoningPreset{
		{Effort: "low", Description: "Fast responses with lighter reasoning"},
		{Effort: "medium", Description: "Balances speed and reasoning depth for everyday tasks"},
		{Effort: "high", Description: "Greater reasoning depth for complex problems"},
		{Effort: "xhigh", Description: "Extra high reasoning depth for complex problems"},
	}
	if codexModelMatchesFamily(id, "gpt-5.6-sol") ||
		codexModelMatchesFamily(id, "gpt-5.6-terra") ||
		codexModelMatchesFamily(id, "gpt-5.6-luna") {
		levels = append(levels, codexReasoningPreset{
			Effort:      "max",
			Description: "Maximum reasoning depth for the hardest problems",
		})
	}
	if codexModelMatchesFamily(id, "gpt-5.6-sol") ||
		codexModelMatchesFamily(id, "gpt-5.6-terra") {
		levels = append(levels, codexReasoningPreset{
			Effort:      "ultra",
			Description: "Maximum reasoning with automatic task delegation",
		})
	}
	return levels
}

func codexModelMatchesFamily(id, family string) bool {
	lower := strings.ToLower(strings.TrimSpace(id))
	return lower == family || strings.HasPrefix(lower, family+"-")
}

func codexMultiAgentVersion(id string) *string {
	version := ""
	switch {
	case codexModelMatchesFamily(id, "gpt-5.6-sol"),
		codexModelMatchesFamily(id, "gpt-5.6-terra"):
		version = "v2"
	case codexModelMatchesFamily(id, "gpt-5.6-luna"):
		version = "v1"
	default:
		return nil
	}
	return &version
}

func isCodexCatalogModelID(id string) bool {
	if id == "" {
		return false
	}
	lower := strings.ToLower(id)
	for _, token := range []string{
		"audio",
		"dall-e",
		"embedding",
		"image",
		"moderation",
		"rerank",
		"seedance",
		"sora",
		"speech",
		"tts",
		"video",
		"whisper",
	} {
		if strings.Contains(lower, token) {
			return false
		}
	}
	return true
}

func codexModelSupportsReasoning(id string) bool {
	lower := strings.ToLower(id)
	return strings.Contains(lower, "codex") ||
		strings.HasPrefix(lower, "gpt-5") ||
		strings.HasPrefix(lower, "o1") ||
		strings.HasPrefix(lower, "o3") ||
		strings.HasPrefix(lower, "o4")
}

func codexModelSupportsImages(id string) bool {
	return !strings.Contains(strings.ToLower(id), "deepseek")
}
