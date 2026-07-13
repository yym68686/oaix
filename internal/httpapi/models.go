package httpapi

import (
	"net/http"
	"strings"
	"time"
)

const (
	codexFallbackContextWindow int64 = 272000
	codexBaseInstructions            = "You are Codex, a coding agent. You and the user share one workspace, and your job is to collaborate with them until their goal is genuinely handled.\n\n" +
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
	IncludeSkillsInstructions   bool                   `json:"include_skills_usage_instructions"`
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

type codexModelProfile struct {
	DisplayName               string
	Description               string
	DefaultReasoningLevel     string
	Priority                  int
	AdditionalSpeedTiers      []string
	ServiceTiers              []codexServiceTier
	IncludeSkillsInstructions bool
	DefaultReasoningSummary   string
	SupportVerbosity          bool
	DefaultVerbosity          string
	WebSearchToolType         string
	ContextWindow             int64
	MaxContextWindow          int64
	CompHash                  string
	SupportsSearchTool        bool
	UseResponsesLite          bool
	ToolMode                  string
	MultiAgentVersion         string
}

func (a *App) models(w http.ResponseWriter, r *http.Request) {
	modelIDs := advertisedModelIDs()
	if clientVersion := strings.TrimSpace(r.URL.Query().Get("client_version")); clientVersion != "" {
		if wrote, err := a.writeOfficialModelsCatalog(w, r, clientVersion); wrote {
			return
		} else if err != nil {
			w.Header().Set("X-OAIX-Models-Source", "official-unavailable")
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"detail": "Model capability scan is incomplete; retry shortly",
			})
			return
		}
		w.Header().Set("X-OAIX-Models-Source", "static-fallback")
		writeJSON(w, http.StatusOK, codexModelsCatalog(modelIDs, a.cachedFastModelsForRequest(r, clientVersion)))
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

func (a *App) cachedFastModelsForRequest(r *http.Request, clientVersion string) map[string]struct{} {
	if a == nil || a.tokens == nil || r == nil {
		return nil
	}
	auth := authFromContext(r.Context())
	ownerUserID, err := auth.proxyOwner(r.Context(), a.store)
	if err != nil {
		return nil
	}
	return a.tokens.FastModelsForOwner(ownerUserID, clientVersion, time.Now().UTC())
}

func codexModelsCatalog(modelIDs []string, fastModels map[string]struct{}) codexModelsResponse {
	models := make([]codexModelInfo, 0, len(modelIDs))
	for _, id := range modelIDs {
		id = strings.TrimSpace(id)
		if !isCodexCatalogModelID(id) {
			continue
		}
		model := codexModelInfoForID(id, len(models))
		if !catalogFastModelsMatch(fastModels, id) {
			model.AdditionalSpeedTiers = []string{}
			model.ServiceTiers = []codexServiceTier{}
			model.DefaultServiceTier = nil
		}
		models = append(models, model)
	}
	return codexModelsResponse{Models: models}
}

func catalogFastModelsMatch(fastModels map[string]struct{}, model string) bool {
	for fastModel := range fastModels {
		if codexModelMatchesFamily(model, fastModel) {
			return true
		}
	}
	return false
}

func codexModelInfoForID(id string, priority int) codexModelInfo {
	profile := codexModelProfileForID(id, priority)
	reasoningLevels := []codexReasoningPreset{}
	defaultReasoningLevel := (*string)(nil)
	supportsReasoningSummaries := false
	if codexModelSupportsReasoning(id) {
		defaultReasoningLevel = stringPointer(profile.DefaultReasoningLevel)
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
		DisplayName:                profile.DisplayName,
		Description:                profile.Description,
		DefaultReasoningLevel:      defaultReasoningLevel,
		SupportedReasoningLevels:   reasoningLevels,
		ShellType:                  "shell_command",
		Visibility:                 "list",
		SupportedInAPI:             true,
		Priority:                   profile.Priority,
		AdditionalSpeedTiers:       profile.AdditionalSpeedTiers,
		ServiceTiers:               profile.ServiceTiers,
		AvailabilityNux:            nil,
		Upgrade:                    nil,
		BaseInstructions:           codexBaseInstructions,
		IncludeSkillsInstructions:  profile.IncludeSkillsInstructions,
		SupportsReasoningSummaries: supportsReasoningSummaries,
		DefaultReasoningSummary:    profile.DefaultReasoningSummary,
		SupportVerbosity:           profile.SupportVerbosity,
		DefaultVerbosity:           stringPointer(profile.DefaultVerbosity),
		ApplyPatchToolType:         &codexApplyPatchTool,
		WebSearchToolType:          profile.WebSearchToolType,
		TruncationPolicy: codexTruncationPolicy{
			Mode:  "tokens",
			Limit: 10000,
		},
		SupportsParallelToolCalls:   true,
		SupportsImageDetailOriginal: supportsImageDetailOriginal,
		ContextWindow:               profile.ContextWindow,
		MaxContextWindow:            profile.MaxContextWindow,
		AutoCompactTokenLimit:       nil,
		CompHash:                    stringPointer(profile.CompHash),
		EffectiveContextWindowPct:   95,
		ExperimentalSupportedTools:  []string{},
		InputModalities:             inputModalities,
		SupportsSearchTool:          profile.SupportsSearchTool,
		UseResponsesLite:            profile.UseResponsesLite,
		ToolMode:                    stringPointer(profile.ToolMode),
		MultiAgentVersion:           stringPointer(profile.MultiAgentVersion),
	}
}

func codexModelProfileForID(id string, priority int) codexModelProfile {
	fastTiers := []string{"fast"}
	priorityService := []codexServiceTier{{
		ID:          "priority",
		Name:        "Fast",
		Description: "1.5x speed, increased usage",
	}}
	profile := codexModelProfile{
		DisplayName:             id,
		Description:             "Available through OAIX.",
		DefaultReasoningLevel:   codexReasoningMedium,
		Priority:                priority,
		AdditionalSpeedTiers:    fastTiers,
		ServiceTiers:            priorityService,
		DefaultReasoningSummary: "auto",
		WebSearchToolType:       "text",
		ContextWindow:           codexFallbackContextWindow,
		MaxContextWindow:        codexFallbackContextWindow,
	}
	switch {
	case codexModelMatchesFamily(id, "gpt-5.6-sol"):
		profile.DisplayName = "GPT-5.6-Sol"
		profile.Description = "Latest frontier agentic coding model."
		profile.DefaultReasoningLevel = codexReasoningLow
		profile.Priority = 1
		profile.DefaultVerbosity = "low"
		profile.ContextWindow = 372000
		profile.MaxContextWindow = 372000
		profile.CompHash = "3000"
		profile.UseResponsesLite = true
		profile.ToolMode = "code_mode_only"
		profile.MultiAgentVersion = "v2"
	case codexModelMatchesFamily(id, "gpt-5.6-terra"):
		profile.DisplayName = "GPT-5.6-Terra"
		profile.Description = "Balanced agentic coding model for everyday work."
		profile.Priority = 2
		profile.DefaultVerbosity = "low"
		profile.ContextWindow = 372000
		profile.MaxContextWindow = 372000
		profile.CompHash = "3000"
		profile.UseResponsesLite = true
		profile.ToolMode = "code_mode_only"
		profile.MultiAgentVersion = "v2"
	case codexModelMatchesFamily(id, "gpt-5.6-luna"):
		profile.DisplayName = "GPT-5.6-Luna"
		profile.Description = "Fast and affordable agentic coding model."
		profile.Priority = 3
		profile.DefaultVerbosity = "low"
		profile.ContextWindow = 372000
		profile.MaxContextWindow = 372000
		profile.CompHash = "3000"
		profile.UseResponsesLite = true
		profile.ToolMode = "code_mode_only"
		profile.MultiAgentVersion = "v1"
	case codexModelMatchesFamily(id, "gpt-5.5"):
		profile.DisplayName = "GPT-5.5"
		profile.Description = "Frontier model for complex coding, research, and real-world work."
		profile.Priority = 7
		profile.DefaultVerbosity = "low"
		profile.CompHash = "2911"
		profile.IncludeSkillsInstructions = true
	case codexModelMatchesFamily(id, "gpt-5.4-mini"):
		profile.DisplayName = "GPT-5.4-Mini"
		profile.Description = "Small, fast, and cost-efficient model for simpler coding tasks."
		profile.Priority = 23
		profile.AdditionalSpeedTiers = []string{}
		profile.ServiceTiers = []codexServiceTier{}
		profile.DefaultVerbosity = "medium"
		profile.CompHash = "2911"
	case codexModelMatchesFamily(id, "gpt-5.4"):
		profile.DisplayName = "GPT-5.4"
		profile.Description = "Strong model for everyday coding."
		profile.Priority = 16
		profile.DefaultVerbosity = "low"
		profile.MaxContextWindow = 1000000
		profile.CompHash = "2911"
	}
	if profile.CompHash != "" {
		profile.DefaultReasoningSummary = "none"
		profile.SupportVerbosity = true
		profile.WebSearchToolType = "text_and_image"
		profile.SupportsSearchTool = true
	}
	return profile
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

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
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
