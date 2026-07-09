package proxy

import (
	"time"

	"github.com/yym68686/oaix/internal/tokens"
)

func promptString(ctx *PromptCacheContext, getter func(*PromptCacheContext) string) *string {
	if ctx == nil || getter == nil {
		return nil
	}
	return nullable(getter(ctx))
}

func promptTrace(ctx *PromptCacheContext) map[string]any {
	if ctx == nil || ctx.PromptCacheTrace == nil {
		return nil
	}
	return cloneTraceMap(ctx.PromptCacheTrace)
}

func updatePromptTrace(ctx *PromptCacheContext, affinityResult tokens.PromptAffinityResult, usage *UsageMetrics, upstreamResponseID string, status int) map[string]any {
	trace := promptTrace(ctx)
	if trace == nil {
		if usage == nil {
			return nil
		}
		trace = map[string]any{}
	}
	route := ensureTraceMap(trace, "route")
	if affinityResult.Result != "" {
		route["cache_affinity_result"] = affinityResult.Result
	}
	if affinityResult.LaneIndex != nil {
		route["cache_affinity_lane_index"] = *affinityResult.LaneIndex
	}
	if affinityResult.LaneCount > 0 {
		route["cache_affinity_lane_count"] = affinityResult.LaneCount
	}
	usageMap := ensureTraceMap(trace, "usage")
	if usage != nil {
		usageMap["input_tokens"] = usage.InputTokens
		usageMap["cache_write_input_tokens"] = usage.CacheWriteInputTokens
		usageMap["cached_input_tokens"] = usage.CachedInputTokens
		usageMap["output_tokens"] = usage.OutputTokens
		usageMap["total_tokens"] = usage.TotalTokens
		usageMap["non_cached_input_tokens"] = usage.NonCachedInputTokens
		usageMap["cache_write_tokens_source"] = usage.CacheWriteTokensSource
		usageMap["cached_input_tokens_source"] = usage.CachedInputTokensSource
		usageMap["pricing_model"] = usage.PricingModel
		usageMap["billing_mode"] = usage.BillingMode
		if usage.CacheHitRatio != nil {
			usageMap["cache_hit_ratio"] = *usage.CacheHitRatio
		}
		for key, value := range usage.Trace() {
			usageMap[key] = value
		}
	}
	response := ensureTraceMap(trace, "response")
	if upstreamResponseID != "" {
		response["response_id"] = upstreamResponseID
	}
	if status > 0 {
		response["status_code"] = status
	}
	return trace
}

func ensureTraceMap(trace map[string]any, key string) map[string]any {
	if existing, ok := trace[key].(map[string]any); ok {
		return existing
	}
	next := map[string]any{}
	trace[key] = next
	return next
}

func cloneTraceMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		if nested, ok := value.(map[string]any); ok {
			output[key] = cloneTraceMap(nested)
			continue
		}
		output[key] = value
	}
	return output
}

func usageInt(usage *UsageMetrics, getter func(*UsageMetrics) int) *int {
	if usage == nil || getter == nil {
		return nil
	}
	value := getter(usage)
	return &value
}

func usageString(usage *UsageMetrics, getter func(*UsageMetrics) string) *string {
	if usage == nil || getter == nil {
		return nil
	}
	return nullable(getter(usage))
}

func usageFloat(usage *UsageMetrics, getter func(*UsageMetrics) *float64) *float64 {
	if usage == nil || getter == nil {
		return nil
	}
	return getter(usage)
}

func usageFloatValue(usage *UsageMetrics, getter func(*UsageMetrics) *float64) *float64 {
	return usageFloat(usage, getter)
}

func ttftMillis(started time.Time, firstTokenAt *time.Time) *int {
	if firstTokenAt == nil || firstTokenAt.IsZero() {
		return nil
	}
	value := int(firstTokenAt.Sub(started).Milliseconds())
	if value < 0 {
		value = 0
	}
	return &value
}

func claimReason(claim *tokens.Claim) string {
	if claim == nil || claim.Reason == "" {
		return "snapshot_round_robin"
	}
	return claim.Reason
}
