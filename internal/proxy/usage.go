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
	InputTokens                   int
	CacheWriteInputTokens         int
	CachedInputTokens             int
	OutputTokens                  int
	TotalTokens                   int
	NonCachedInputTokens          int
	InputTokensSource             string
	CacheWriteTokensSource        string
	CachedInputTokensSource       string
	OutputTokensSource            string
	TotalTokensSource             string
	PricingModel                  string
	BillingMode                   string
	InputPricePerMillionUSD       float64
	CacheWritePricePerMillionUSD  *float64
	CachedInputPricePerMillionUSD *float64
	OutputPricePerMillionUSD      float64
	CacheHitRatio                 *float64
	EstimatedCostUSD              *float64
	Anomalies                     []string
}

const (
	usageBillingModeOpenAICachedInput = "openai_cached_input"
	usageBillingModeOpenAIPromptCache = "openai_prompt_cache"
)

type usageField struct {
	Value   int
	Present bool
	Valid   bool
	Source  string
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
	usageObj, usagePrefix := usageObject(payload)
	if usageObj == nil {
		return nil
	}
	input := resolveUsageField(usageObj, usagePrefix, []string{"input_tokens"}, []string{"prompt_tokens"})
	output := resolveUsageField(usageObj, usagePrefix, []string{"output_tokens"}, []string{"completion_tokens"})
	total := resolveUsageField(usageObj, usagePrefix, []string{"total_tokens"})
	cacheWrite := resolveUsageField(
		usageObj,
		usagePrefix,
		[]string{"input_tokens_details", "cache_write_tokens"},
		[]string{"prompt_tokens_details", "cache_write_tokens"},
		[]string{"cache_write_tokens"},
		[]string{"cache_creation_input_tokens"},
	)
	cached := resolveUsageField(
		usageObj,
		usagePrefix,
		[]string{"input_cached_tokens"},
		[]string{"input_tokens_details", "cached_tokens"},
		[]string{"prompt_tokens_details", "cached_tokens"},
		[]string{"cache_read_input_tokens"},
	)
	if !total.Valid && (input.Valid || output.Valid) {
		total = usageField{
			Value:   input.Value + output.Value,
			Present: true,
			Valid:   true,
			Source:  "computed.input_plus_output",
		}
	}
	if !input.Valid && !output.Valid && !total.Valid {
		return nil
	}
	metrics := &UsageMetrics{
		InputTokens:             input.Value,
		CacheWriteInputTokens:   cacheWrite.Value,
		CachedInputTokens:       cached.Value,
		OutputTokens:            output.Value,
		TotalTokens:             total.Value,
		InputTokensSource:       input.Source,
		CacheWriteTokensSource:  cacheWrite.Source,
		CachedInputTokensSource: cached.Source,
		OutputTokensSource:      output.Source,
		TotalTokensSource:       total.Source,
	}
	if input.Value > 0 {
		ratio := math.Max(0, math.Min(1, float64(cached.Value)/float64(input.Value)))
		metrics.CacheHitRatio = &ratio
	}
	if !cacheWrite.Valid && cacheWrite.Present {
		metrics.Anomalies = append(metrics.Anomalies, "invalid_cache_write_tokens")
	}
	if !cached.Valid && cached.Present {
		metrics.Anomalies = append(metrics.Anomalies, "invalid_cached_tokens")
	}
	if pricing, ok := pricingForModel(modelName); ok {
		applyUsagePricing(metrics, pricing)
		if pricing.billingMode == usageBillingModeOpenAIPromptCache && !cacheWrite.Present {
			metrics.Anomalies = append(metrics.Anomalies, "gpt56_cache_write_tokens_missing")
		}
	}
	cacheComponentTokens := metrics.CachedInputTokens
	if metrics.BillingMode == usageBillingModeOpenAIPromptCache {
		cacheComponentTokens += metrics.CacheWriteInputTokens
	}
	if cacheComponentTokens > metrics.InputTokens {
		metrics.Anomalies = append(metrics.Anomalies, "cache_components_exceed_input_tokens")
	}
	return metrics
}

func resolveUsageField(mapping map[string]any, prefix string, paths ...[]string) usageField {
	var firstInvalid *usageField
	for _, path := range paths {
		raw, present := lookupPath(mapping, path...)
		if !present {
			continue
		}
		source := strings.Trim(strings.TrimSpace(prefix)+"."+strings.Join(path, "."), ".")
		value, valid := normalizeInt(raw)
		field := usageField{Value: value, Present: true, Valid: valid, Source: source}
		if valid {
			return field
		}
		if firstInvalid == nil {
			copy := field
			firstInvalid = &copy
		}
	}
	if firstInvalid != nil {
		return *firstInvalid
	}
	return usageField{}
}

func lookupPath(mapping map[string]any, keys ...string) (any, bool) {
	var current any = mapping
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, present := next[key]
		if !present {
			return nil, false
		}
		current = value
	}
	return current, true
}

func usageObject(payload any) (map[string]any, string) {
	mapping, ok := payload.(map[string]any)
	if !ok {
		return nil, ""
	}
	if usage, ok := mapping["usage"].(map[string]any); ok {
		return usage, "usage"
	}
	if response, ok := mapping["response"].(map[string]any); ok {
		if usage, ok := response["usage"].(map[string]any); ok {
			return usage, "response.usage"
		}
	}
	return nil, ""
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

func estimateUsageCost(modelName string, inputTokens, outputTokens, cacheWriteInputTokens, cachedInputTokens int) *float64 {
	pricing, ok := pricingForModel(modelName)
	if !ok {
		return nil
	}
	metrics := &UsageMetrics{
		InputTokens:           inputTokens,
		CacheWriteInputTokens: cacheWriteInputTokens,
		CachedInputTokens:     cachedInputTokens,
		OutputTokens:          outputTokens,
	}
	applyUsagePricing(metrics, pricing)
	return metrics.EstimatedCostUSD
}

func applyUsagePricing(metrics *UsageMetrics, pricing modelPricing) {
	if metrics == nil {
		return
	}
	nonCached := metrics.InputTokens - metrics.CachedInputTokens
	if pricing.cacheWrite != nil {
		nonCached -= metrics.CacheWriteInputTokens
	}
	if nonCached < 0 {
		nonCached = 0
	}
	cost := (float64(nonCached)/1_000_000.0)*pricing.input +
		(float64(metrics.OutputTokens)/1_000_000.0)*pricing.output
	if pricing.cacheWrite != nil && metrics.CacheWriteInputTokens > 0 {
		cost += (float64(metrics.CacheWriteInputTokens) / 1_000_000.0) * *pricing.cacheWrite
	}
	if pricing.cached != nil && metrics.CachedInputTokens > 0 {
		cost += (float64(metrics.CachedInputTokens) / 1_000_000.0) * *pricing.cached
	}
	rounded := math.Round(cost*100_000_000) / 100_000_000
	metrics.NonCachedInputTokens = nonCached
	metrics.PricingModel = pricing.name
	metrics.BillingMode = pricing.billingMode
	metrics.InputPricePerMillionUSD = pricing.input
	metrics.CacheWritePricePerMillionUSD = pricing.cacheWrite
	metrics.CachedInputPricePerMillionUSD = pricing.cached
	metrics.OutputPricePerMillionUSD = pricing.output
	metrics.EstimatedCostUSD = &rounded
}

type modelPricing struct {
	name        string
	billingMode string
	input       float64
	output      float64
	cached      *float64
	cacheWrite  *float64
}

func pricingForModel(modelName string) (modelPricing, bool) {
	normalized := strings.ToLower(strings.TrimSpace(modelName))
	price := func(value float64) *float64 { return &value }
	table := map[string]modelPricing{
		"gpt-5.6-sol": {
			name: "gpt-5.6-sol", billingMode: usageBillingModeOpenAIPromptCache,
			input: 5.0, cacheWrite: price(6.25), cached: price(0.5), output: 30.0,
		},
		"gpt-5.6-terra": {
			name: "gpt-5.6-terra", billingMode: usageBillingModeOpenAIPromptCache,
			input: 2.5, cacheWrite: price(3.125), cached: price(0.25), output: 15.0,
		},
		"gpt-5.6-luna": {
			name: "gpt-5.6-luna", billingMode: usageBillingModeOpenAIPromptCache,
			input: 1.0, cacheWrite: price(1.25), cached: price(0.1), output: 6.0,
		},
		"gpt-5.5": {
			name: "gpt-5.5", billingMode: usageBillingModeOpenAICachedInput,
			input: 5.0, cached: price(0.5), output: 30.0,
		},
		"gpt-5": {
			name: "gpt-5", billingMode: usageBillingModeOpenAICachedInput,
			input: 1.25, cached: price(0.125), output: 10.0,
		},
		"gpt-5-mini": {
			name: "gpt-5-mini", billingMode: usageBillingModeOpenAICachedInput,
			input: 0.25, cached: price(0.025), output: 2.0,
		},
		"gpt-5-nano": {
			name: "gpt-5-nano", billingMode: usageBillingModeOpenAICachedInput,
			input: 0.05, cached: price(0.005), output: 0.4,
		},
		"gpt-5.4": {
			name: "gpt-5.4", billingMode: usageBillingModeOpenAICachedInput,
			input: 2.5, cached: price(0.25), output: 15.0,
		},
		"gpt-5.4-mini": {
			name: "gpt-5.4-mini", billingMode: usageBillingModeOpenAICachedInput,
			input: 0.75, cached: price(0.075), output: 4.5,
		},
		"gpt-5.4-nano": {
			name: "gpt-5.4-nano", billingMode: usageBillingModeOpenAICachedInput,
			input: 0.2, cached: price(0.02), output: 1.25,
		},
	}
	if pricing, ok := table[normalized]; ok {
		return pricing, true
	}
	for _, family := range []string{"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"} {
		if strings.HasPrefix(normalized, family+"-") {
			return table[family], true
		}
	}
	if strings.HasPrefix(normalized, "gpt-5.6") {
		return modelPricing{}, false
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

func (u *UsageMetrics) Trace() map[string]any {
	if u == nil {
		return nil
	}
	raw := map[string]any{
		"input_tokens":              u.InputTokens,
		"cache_write_tokens":        u.CacheWriteInputTokens,
		"cached_tokens":             u.CachedInputTokens,
		"output_tokens":             u.OutputTokens,
		"total_tokens":              u.TotalTokens,
		"input_tokens_source":       u.InputTokensSource,
		"cache_write_tokens_source": u.CacheWriteTokensSource,
		"cached_tokens_source":      u.CachedInputTokensSource,
		"output_tokens_source":      u.OutputTokensSource,
		"total_tokens_source":       u.TotalTokensSource,
		"cache_write_reported":      u.CacheWriteTokensSource != "",
	}
	trace := map[string]any{"raw": raw}
	if u.PricingModel != "" {
		billing := map[string]any{
			"pricing_model":            u.PricingModel,
			"mode":                     u.BillingMode,
			"non_cached_input_tokens":  u.NonCachedInputTokens,
			"cache_write_input_tokens": u.CacheWriteInputTokens,
			"cached_input_tokens":      u.CachedInputTokens,
			"input_per_million_usd":    u.InputPricePerMillionUSD,
			"output_per_million_usd":   u.OutputPricePerMillionUSD,
		}
		if u.CacheWritePricePerMillionUSD != nil {
			billing["cache_write_per_million_usd"] = *u.CacheWritePricePerMillionUSD
		}
		if u.CachedInputPricePerMillionUSD != nil {
			billing["cached_input_per_million_usd"] = *u.CachedInputPricePerMillionUSD
		}
		if u.EstimatedCostUSD != nil {
			billing["estimated_cost_usd"] = *u.EstimatedCostUSD
		}
		trace["billing"] = billing
	}
	if len(u.Anomalies) > 0 {
		trace["anomalies"] = append([]string(nil), u.Anomalies...)
	}
	return trace
}
