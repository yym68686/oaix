package cooldown

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

type UsageLimit struct {
	Matched       bool
	ExplicitKind  bool
	ResetAt       *time.Time
	ExplicitReset bool
}

func ParseUsageLimit(status int, raw []byte, now time.Time) UsageLimit {
	if status != 429 {
		return UsageLimit{}
	}
	errObj := errorObject(raw)
	if errObj == nil {
		return UsageLimit{}
	}
	errorType := strings.TrimSpace(strings.ToLower(text(errObj["type"])))
	errorCode := strings.TrimSpace(strings.ToLower(text(errObj["code"])))
	message := strings.TrimSpace(strings.ToLower(text(errObj["message"])))
	explicitKind := errorType == "usage_limit_reached" || errorCode == "usage_limit_reached"
	if !explicitKind && !strings.Contains(message, "usage limit") {
		return UsageLimit{}
	}
	result := UsageLimit{Matched: true, ExplicitKind: explicitKind}
	if seconds, ok := floatValue(errObj["resets_in_seconds"]); ok {
		result.ResetAt = ptr(now.UTC().Add(secondsDuration(seconds)))
		result.ExplicitReset = true
		return result
	}
	if seconds, ok := floatValue(errObj["resetsInSeconds"]); ok {
		result.ResetAt = ptr(now.UTC().Add(secondsDuration(seconds)))
		result.ExplicitReset = true
		return result
	}
	if resetAt := epochTime(errObj["resets_at"]); resetAt != nil {
		result.ResetAt = resetAt
		result.ExplicitReset = true
		return result
	}
	if resetAt := epochTime(errObj["resetsAt"]); resetAt != nil {
		result.ResetAt = resetAt
		result.ExplicitReset = true
	}
	return result
}

func UsageLimitUntil(status int, raw []byte, now time.Time, fallback time.Duration) *time.Time {
	parsed := ParseUsageLimit(status, raw, now)
	if !parsed.Matched {
		return nil
	}
	if parsed.ResetAt != nil {
		return parsed.ResetAt
	}
	if fallback <= 0 {
		fallback = 300 * time.Second
	}
	return ptr(now.UTC().Add(fallback))
}

func errorObject(raw []byte) map[string]any {
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return nestedErrorObject(payload)
}

func nestedErrorObject(value any) map[string]any {
	if raw, ok := value.(string); ok {
		var decoded any
		if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
			return nestedErrorObject(decoded)
		}
		return nil
	}
	mapping, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if errorValue, ok := mapping["error"].(map[string]any); ok {
		return errorValue
	}
	if response, ok := mapping["response"].(map[string]any); ok {
		if errorValue, ok := response["error"].(map[string]any); ok {
			return errorValue
		}
	}
	if detail, ok := mapping["detail"]; ok {
		if errorValue := nestedErrorObject(detail); errorValue != nil {
			return errorValue
		}
	}
	if text(mapping["type"]) != "" || text(mapping["message"]) != "" || text(mapping["code"]) != "" {
		return mapping
	}
	return nil
}

func text(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func floatValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		next, err := typed.Float64()
		return next, err == nil
	case string:
		next, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return next, err == nil
	default:
		return 0, false
	}
}

func epochTime(value any) *time.Time {
	if raw, ok := value.(string); ok {
		trimmed := strings.TrimSpace(raw)
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if parsed, err := time.Parse(layout, trimmed); err == nil {
				next := parsed.UTC()
				return &next
			}
		}
	}
	seconds, ok := floatValue(value)
	if !ok {
		return nil
	}
	nanos := int64(math.Round(seconds * float64(time.Second)))
	next := time.Unix(0, nanos).UTC()
	return &next
}

func secondsDuration(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(seconds * float64(time.Second)))
}

func ptr(value time.Time) *time.Time {
	return &value
}
