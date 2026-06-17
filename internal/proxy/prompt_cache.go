package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/yym68686/oaix/internal/config"
)

type PromptCacheContext struct {
	Endpoint                      string
	Model                         string
	Compact                       bool
	PreviousResponseID            string
	PromptCacheKey                string
	PromptCacheKeyHash            string
	AffinityKey                   string
	ClientScope                   string
	Source                        string
	SessionID                     string
	SessionIDSource               string
	RequestPayloadHash            string
	UpstreamPayloadHash           string
	PromptTemplateHash            string
	PromptDynamicHash             string
	PromptCacheRetentionRequested string
	PromptCacheRetentionSent      string
	PreviousResponseIDHash        string
	PromptCacheTrace              map[string]any
}

func buildPromptCacheContext(headers http.Header, intent RequestIntent, body []byte, cfg config.PromptCacheConfig) (*PromptCacheContext, []byte) {
	if !cfg.AffinityEnabled {
		return nil, body
	}
	if intent.Endpoint != "/v1/responses" && intent.Endpoint != "/v1/responses/compact" && intent.Endpoint != "/v1/chat/completions" {
		return nil, body
	}
	var raw map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(body), &raw); err != nil || raw == nil {
		return nil, body
	}
	model := intent.Model
	if model == "" {
		model = text(raw["model"])
	}
	previousResponseID := text(raw["previous_response_id"])
	explicitKey := text(raw["prompt_cache_key"])
	promptCacheKey := explicitKey
	source := "explicit"
	if promptCacheKey == "" {
		source = "none"
		if cfg.AutoKeyEnabled {
			switch intent.Endpoint {
			case "/v1/chat/completions":
				promptCacheKey = deriveChatPromptCacheKey(raw)
				if promptCacheKey != "" {
					source = "derived_chat"
				}
			case "/v1/responses", "/v1/responses/compact":
				promptCacheKey = deriveResponsesPromptCacheKey(raw)
				if promptCacheKey != "" {
					source = "derived_responses"
				}
			}
		}
	}
	if previousResponseID == "" && promptCacheKey == "" {
		return nil, body
	}

	upstream := cloneMap(raw)
	retentionRequested := text(raw["prompt_cache_retention"])
	delete(upstream, "prompt_cache_retention")
	if promptCacheKey != "" {
		upstream["prompt_cache_key"] = promptCacheKey
	}
	upstreamBody, err := encodeSeedJSON(upstream)
	if err != nil {
		return nil, body
	}

	keyHash := ""
	if promptCacheKey != "" {
		keyHash = shortHash(promptCacheKey, 32)
	}
	clientScope := clientScopeHash(headers)
	affinityKey := ""
	if keyHash != "" {
		affinityEndpoint, affinityCompact := promptCacheAffinityFamily(intent.Endpoint, intent.Compact)
		seed := map[string]any{
			"endpoint": affinityEndpoint,
			"model":    firstNonEmpty(model, text(raw["model"])),
			"compact":  affinityCompact,
			"client":   clientScope,
			"prompt":   keyHash,
		}
		affinityKey = shortHash(mustSeedJSON(seed), 32)
	}
	sessionID, sessionSource := promptCacheSession(headers, clientScope, promptCacheKey, affinityKey, cfg.SessionPreferHeader)
	previousHash := ""
	if previousResponseID != "" {
		previousHash = shortHash(previousResponseID, 64)
	}
	promptTemplateHash, promptDynamicHash := promptHashes(upstream)
	ctx := &PromptCacheContext{
		Endpoint:                      intent.Endpoint,
		Model:                         firstNonEmpty(model, text(raw["model"])),
		Compact:                       intent.Compact,
		PreviousResponseID:            previousResponseID,
		PromptCacheKey:                promptCacheKey,
		PromptCacheKeyHash:            keyHash,
		AffinityKey:                   affinityKey,
		ClientScope:                   clientScope,
		Source:                        source,
		SessionID:                     sessionID,
		SessionIDSource:               sessionSource,
		RequestPayloadHash:            hashPayload(raw),
		UpstreamPayloadHash:           hashPayload(upstream),
		PromptTemplateHash:            promptTemplateHash,
		PromptDynamicHash:             promptDynamicHash,
		PromptCacheRetentionRequested: retentionRequested,
		PromptCacheRetentionSent:      text(upstream["prompt_cache_retention"]),
		PreviousResponseIDHash:        previousHash,
	}
	ctx.PromptCacheTrace = buildPromptCacheTrace(ctx)
	return ctx, []byte(upstreamBody)
}

func deriveResponsesPromptCacheKey(payload map[string]any) string {
	model := text(payload["model"])
	if model == "" {
		return ""
	}
	seed := map[string]any{
		"kind":             "responses",
		"model":            model,
		"reasoning_effort": reasoningEffort(payload),
		"instructions":     payload["instructions"],
		"system":           responsesSystemMessages(payload["input"]),
		"first_user":       responsesFirstUser(payload["input"]),
		"tools":            payload["tools"],
		"tool_choice":      payload["tool_choice"],
		"text":             payload["text"],
	}
	return fmt.Sprintf("oaix:resp:%s:%s", model, shortHash(mustSeedJSON(seed), 32))
}

func deriveChatPromptCacheKey(payload map[string]any) string {
	model := text(payload["model"])
	if model == "" {
		return ""
	}
	messages := listValue(payload["messages"])
	seed := map[string]any{
		"kind":             "chat_completions",
		"model":            model,
		"reasoning_effort": reasoningEffort(payload),
		"system":           systemMessages(messages),
		"first_user":       firstUserMessage(messages),
		"tools":            payload["tools"],
		"functions":        payload["functions"],
		"tool_choice":      payload["tool_choice"],
		"function_call":    payload["function_call"],
		"response_format":  payload["response_format"],
	}
	return fmt.Sprintf("oaix:chat:%s:%s", model, shortHash(mustSeedJSON(seed), 32))
}

func promptCacheSession(headers http.Header, clientScope, promptCacheKey, affinityKey string, preferHeader bool) (string, string) {
	explicit := strings.TrimSpace(headers.Get("Session_id"))
	seed := promptCacheKey
	if seed == "" {
		seed = affinityKey
	}
	promptSessionID := ""
	if seed != "" {
		promptSessionID = deterministicUUID(clientScope + ":" + seed)
	}
	if explicit != "" && (promptSessionID == "" || preferHeader) {
		return explicit, "header"
	}
	if promptSessionID != "" {
		return promptSessionID, "prompt_cache"
	}
	return deterministicUUID(fmt.Sprintf("%s:%s", clientScope, hashPayload(headers))), "generated"
}

func promptCacheAffinityFamily(endpoint string, compact bool) (string, bool) {
	if endpoint == "/v1/responses" || endpoint == "/v1/responses/compact" {
		return "/v1/responses", false
	}
	return endpoint, compact
}

func clientScopeHash(headers http.Header) string {
	if headers == nil {
		return "anonymous"
	}
	authorization := strings.TrimSpace(headers.Get("Authorization"))
	if authorization != "" {
		return shortHash(authorization, 16)
	}
	return "anonymous"
}

func promptHashes(payload map[string]any) (string, string) {
	static, dynamic := promptComponents(payload)
	template := stageHash("template", static)
	dynamicHash := ""
	if len(dynamic) > 0 {
		dynamicHash = stageHash("dynamic", dynamic)
	}
	return template, dynamicHash
}

func promptComponents(payload map[string]any) ([]map[string]any, []map[string]any) {
	if _, ok := payload["messages"].([]any); ok {
		messages := listValue(payload["messages"])
		static := []map[string]any{
			fingerprint("model", payload["model"]),
			fingerprint("reasoning_effort", reasoningEffort(payload)),
			fingerprint("system_messages", systemMessages(messages)),
			fingerprint("tools", payload["tools"]),
			fingerprint("functions", payload["functions"]),
			fingerprint("tool_choice", payload["tool_choice"]),
			fingerprint("function_call", payload["function_call"]),
			fingerprint("response_format", payload["response_format"]),
		}
		dynamic := []map[string]any{
			fingerprint("messages", messages),
			fingerprint("first_user", firstUserMessage(messages)),
		}
		return static, dynamic
	}
	if _, ok := payload["input"]; ok {
		input := payload["input"]
		inputMessages := responsesInputMessages(input)
		static := []map[string]any{
			fingerprint("model", payload["model"]),
			fingerprint("reasoning_effort", reasoningEffort(payload)),
			fingerprint("instructions", payload["instructions"]),
			fingerprint("system_messages", responsesSystemMessages(input)),
			fingerprint("tools", payload["tools"]),
			fingerprint("tool_choice", payload["tool_choice"]),
			fingerprint("text", payload["text"]),
		}
		dynamic := []map[string]any{
			fingerprint("input", inputMessages),
			fingerprint("first_user", firstUserMessage(inputMessages)),
		}
		return static, dynamic
	}
	return []map[string]any{fingerprint("payload", payload)}, nil
}

func fingerprint(name string, value any) map[string]any {
	normalized := mustSeedJSON(contentDigestible(value))
	return map[string]any{
		"name":    name,
		"hash":    shortHash(normalized, 64),
		"bytes":   len([]byte(normalized)),
		"count":   valueCount(value),
		"present": value != nil,
	}
}

func stageHash(name string, components []map[string]any) string {
	return shortHash(mustSeedJSON(map[string]any{"name": name, "components": components}), 64)
}

func valueCount(value any) int {
	switch v := value.(type) {
	case map[string]any:
		return len(v)
	case []any:
		return len(v)
	default:
		if value == nil {
			return 0
		}
		return 1
	}
}

func reasoningEffort(payload map[string]any) any {
	if reasoning, ok := payload["reasoning"].(map[string]any); ok {
		return reasoning["effort"]
	}
	return payload["reasoning_effort"]
}

func firstUserMessage(messages []any) any {
	for _, message := range messages {
		if messageRole(message) == "user" {
			return contentDigestible(mappingGet(message, "content"))
		}
	}
	return nil
}

func systemMessages(messages []any) []any {
	result := make([]any, 0)
	for _, message := range messages {
		role := messageRole(message)
		if role == "system" || role == "developer" {
			result = append(result, contentDigestible(mappingGet(message, "content")))
		}
	}
	return result
}

func responsesInputMessages(input any) []any {
	switch value := input.(type) {
	case []any:
		return append([]any(nil), value...)
	case string:
		return []any{map[string]any{"role": "user", "content": value}}
	default:
		return []any{}
	}
}

func responsesFirstUser(input any) any {
	if value, ok := input.(string); ok {
		return value
	}
	return firstUserMessage(responsesInputMessages(input))
}

func responsesSystemMessages(input any) []any {
	return systemMessages(responsesInputMessages(input))
}

func messageRole(message any) string {
	return strings.ToLower(text(mappingGet(message, "role")))
}

func contentDigestible(value any) any {
	switch typed := value.(type) {
	case []any:
		normalized := make([]any, 0, len(typed))
		for _, item := range typed {
			if mapping, ok := item.(map[string]any); ok {
				itemType := text(mapping["type"])
				switch itemType {
				case "input_text", "output_text", "text":
					normalized = append(normalized, map[string]any{"type": itemType, "text": mapping["text"]})
					continue
				case "image_url", "input_image":
					normalized = append(normalized, map[string]any{
						"type":      itemType,
						"image_url": mapping["image_url"],
						"detail":    mapping["detail"],
					})
					continue
				}
			}
			normalized = append(normalized, item)
		}
		return normalized
	default:
		return value
	}
}

func mappingGet(value any, key string) any {
	if mapping, ok := value.(map[string]any); ok {
		return mapping[key]
	}
	return nil
}

func listValue(value any) []any {
	if list, ok := value.([]any); ok {
		return list
	}
	return []any{}
}

func text(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func shortHash(value string, length int) string {
	sum := sha256.Sum256([]byte(value))
	hexed := hex.EncodeToString(sum[:])
	if length < 8 {
		length = 8
	}
	if length > len(hexed) {
		length = len(hexed)
	}
	return hexed[:length]
}

func hashPayload(value any) string {
	if value == nil {
		return ""
	}
	return shortHash(mustSeedJSON(value), 64)
}

func mustSeedJSON(value any) string {
	data, err := encodeSeedJSON(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return data
}

func encodeSeedJSON(value any) (string, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

func deterministicUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	raw := append([]byte(nil), sum[:16]...)
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
}

func buildPromptCacheTrace(ctx *PromptCacheContext) map[string]any {
	if ctx == nil {
		return nil
	}
	return map[string]any{
		"trace_version":                    1,
		"endpoint":                         ctx.Endpoint,
		"model":                            ctx.Model,
		"compact":                          ctx.Compact,
		"request_payload_hash":             ctx.RequestPayloadHash,
		"upstream_payload_hash":            ctx.UpstreamPayloadHash,
		"prompt_cache_key_hash":            ctx.PromptCacheKeyHash,
		"prompt_cache_key_source":          ctx.Source,
		"prompt_cache_retention_requested": nullableStringValue(ctx.PromptCacheRetentionRequested),
		"prompt_cache_retention_sent":      nullableStringValue(ctx.PromptCacheRetentionSent),
		"previous_response_id_hash":        nullableStringValue(ctx.PreviousResponseIDHash),
		"session_id_hash":                  shortHash(ctx.SessionID, 64),
		"session_id_source":                ctx.SessionIDSource,
		"prompt": map[string]any{
			"template_hash": ctx.PromptTemplateHash,
			"dynamic_hash":  nullableStringValue(ctx.PromptDynamicHash),
		},
		"route": map[string]any{},
		"usage": map[string]any{},
	}
}

func nullableStringValue(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
