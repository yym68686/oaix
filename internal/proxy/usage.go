package proxy

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

type UsageMetrics struct {
	InputTokens       int
	CachedInputTokens int
	OutputTokens      int
	TotalTokens       int
	CacheHitRatio     *float64
	EstimatedCostUSD  *float64
}

type usageObserver struct {
	modelName    string
	pending      []byte
	dataLines    []string
	usage        *UsageMetrics
	responseID   string
	firstTokenAt *time.Time
}

func newUsageObserver(modelName string) *usageObserver {
	return &usageObserver{modelName: modelName}
}

func (o *usageObserver) Observe(chunk []byte) {
	if o == nil || len(chunk) == 0 {
		return
	}
	o.pending = append(o.pending, chunk...)
	for {
		index := bytes.IndexByte(o.pending, '\n')
		if index < 0 {
			if len(o.pending) > 1<<20 {
				o.pending = o.pending[:0]
			}
			return
		}
		line := string(o.pending[:index])
		o.pending = o.pending[index+1:]
		o.observeLine(strings.TrimRight(line, "\r"))
	}
}

func (o *usageObserver) observeLine(line string) {
	if strings.TrimSpace(line) == "" {
		o.flushEvent()
		return
	}
	if strings.HasPrefix(line, "data:") {
		o.dataLines = append(o.dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
	}
}

func (o *usageObserver) flushEvent() {
	if len(o.dataLines) == 0 {
		return
	}
	data := strings.TrimSpace(strings.Join(o.dataLines, "\n"))
	o.dataLines = o.dataLines[:0]
	if data == "" || data == "[DONE]" {
		return
	}
	var payload any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return
	}
	if o.firstTokenAt == nil {
		now := time.Now().UTC()
		o.firstTokenAt = &now
	}
	if usage := extractUsageMetrics(payload, o.modelName); usage != nil {
		o.usage = usage
	}
	if responseID := extractResponseID(payload); responseID != "" {
		o.responseID = responseID
	}
}

func extractResponseMetrics(body []byte, modelName string) (*UsageMetrics, string) {
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, ""
	}
	return extractUsageMetrics(payload, modelName), extractResponseID(payload)
}

func extractUsageMetrics(payload any, modelName string) *UsageMetrics {
	usageObj := usageObject(payload)
	if usageObj == nil {
		return nil
	}
	inputTokens, inputOK := normalizeInt(usageObj["input_tokens"])
	if !inputOK {
		inputTokens, inputOK = normalizeInt(usageObj["prompt_tokens"])
	}
	outputTokens, outputOK := normalizeInt(usageObj["output_tokens"])
	if !outputOK {
		outputTokens, outputOK = normalizeInt(usageObj["completion_tokens"])
	}
	totalTokens, totalOK := normalizeInt(usageObj["total_tokens"])
	if !totalOK && (inputOK || outputOK) {
		totalTokens = inputTokens + outputTokens
		totalOK = true
	}
	cachedTokens, _ := normalizeInt(firstPresent(
		usageObj["input_cached_tokens"],
		nestedGet(usageObj, "input_tokens_details", "cached_tokens"),
		nestedGet(usageObj, "prompt_tokens_details", "cached_tokens"),
	))
	if !inputOK && !outputOK && !totalOK {
		return nil
	}
	metrics := &UsageMetrics{
		InputTokens:       inputTokens,
		CachedInputTokens: cachedTokens,
		OutputTokens:      outputTokens,
		TotalTokens:       totalTokens,
	}
	if inputTokens > 0 {
		ratio := math.Max(0, math.Min(1, float64(cachedTokens)/float64(inputTokens)))
		metrics.CacheHitRatio = &ratio
	}
	if cost := estimateUsageCost(modelName, inputTokens, outputTokens, cachedTokens); cost != nil {
		metrics.EstimatedCostUSD = cost
	}
	return metrics
}

func nestedGet(mapping any, keys ...string) any {
	current := mapping
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = next[key]
	}
	return current
}

func usageObject(payload any) map[string]any {
	mapping, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	if usage, ok := mapping["usage"].(map[string]any); ok {
		return usage
	}
	if response, ok := mapping["response"].(map[string]any); ok {
		if usage, ok := response["usage"].(map[string]any); ok {
			return usage
		}
	}
	return nil
}

func extractResponseID(payload any) string {
	mapping, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	if id := text(mapping["id"]); id != "" {
		return id
	}
	if response, ok := mapping["response"].(map[string]any); ok {
		return text(response["id"])
	}
	return ""
}

func normalizeInt(value any) (int, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case int:
		if typed < 0 {
			return 0, true
		}
		return typed, true
	case int64:
		if typed < 0 {
			return 0, true
		}
		return int(typed), true
	case float64:
		if typed < 0 {
			return 0, true
		}
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		if parsed < 0 {
			return 0, true
		}
		return int(parsed), true
	case string:
		if strings.TrimSpace(typed) == "" {
			return 0, false
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		if parsed < 0 {
			return 0, true
		}
		return parsed, true
	default:
		return 0, false
	}
}

func firstPresent(values ...any) any {
	for _, value := range values {
		if value != nil && value != "" {
			return value
		}
	}
	return nil
}

func estimateUsageCost(modelName string, inputTokens, outputTokens, cachedInputTokens int) *float64 {
	pricing, ok := pricingForModel(modelName)
	if !ok {
		return nil
	}
	nonCached := inputTokens - cachedInputTokens
	if nonCached < 0 {
		nonCached = 0
	}
	cost := (float64(nonCached)/1_000_000.0)*pricing.input +
		(float64(outputTokens)/1_000_000.0)*pricing.output
	if pricing.cached != nil && cachedInputTokens > 0 {
		cost += (float64(cachedInputTokens) / 1_000_000.0) * *pricing.cached
	}
	rounded := math.Round(cost*100_000_000) / 100_000_000
	return &rounded
}

type modelPricing struct {
	input  float64
	output float64
	cached *float64
}

func pricingForModel(modelName string) (modelPricing, bool) {
	normalized := strings.ToLower(strings.TrimSpace(modelName))
	cached := func(value float64) *float64 { return &value }
	table := map[string]modelPricing{
		"gpt-5.5":      {input: 5.0, cached: cached(0.5), output: 30.0},
		"gpt-5":        {input: 1.25, cached: cached(0.125), output: 10.0},
		"gpt-5-mini":   {input: 0.25, cached: cached(0.025), output: 2.0},
		"gpt-5-nano":   {input: 0.05, cached: cached(0.005), output: 0.4},
		"gpt-5.4":      {input: 2.5, cached: cached(0.25), output: 15.0},
		"gpt-5.4-mini": {input: 0.75, cached: cached(0.075), output: 4.5},
		"gpt-5.4-nano": {input: 0.2, cached: cached(0.02), output: 1.25},
	}
	if pricing, ok := table[normalized]; ok {
		return pricing, true
	}
	for _, family := range []string{"gpt-5.5", "gpt-5.4", "gpt-5"} {
		if strings.HasPrefix(normalized, family) {
			return table[family], true
		}
	}
	if strings.Contains(normalized, "-mini") {
		if strings.HasPrefix(normalized, "gpt-5.4") {
			return table["gpt-5.4-mini"], true
		}
		return table["gpt-5-mini"], true
	}
	if strings.Contains(normalized, "-nano") {
		if strings.HasPrefix(normalized, "gpt-5.4") {
			return table["gpt-5.4-nano"], true
		}
		return table["gpt-5-nano"], true
	}
	return modelPricing{}, false
}
