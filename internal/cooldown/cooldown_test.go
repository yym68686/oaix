package cooldown

import (
	"strconv"
	"testing"
	"time"
)

func TestUsageLimitUntilUsesResetsInSeconds(t *testing.T) {
	now := time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC)
	got := UsageLimitUntil(429, []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":123}}`), now, 300*time.Second)
	if got == nil {
		t.Fatal("expected cooldown")
	}
	if want := now.Add(123 * time.Second); !got.Equal(want) {
		t.Fatalf("cooldown = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestUsageLimitUntilUsesResetsAt(t *testing.T) {
	now := time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC)
	reset := now.Add(2 * time.Hour).Unix()
	got := UsageLimitUntil(429, []byte(`{"error":{"code":"usage_limit_reached","resets_at":`+strconv.FormatInt(reset, 10)+`}}`), now, 300*time.Second)
	if got == nil {
		t.Fatal("expected cooldown")
	}
	if want := time.Unix(reset, 0).UTC(); !got.Equal(want) {
		t.Fatalf("cooldown = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestUsageLimitUntilReadsNestedResponseFailedDetail(t *testing.T) {
	now := time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC)
	raw := []byte(`{"type":"response.failed","response":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resetsInSeconds":"42"}}}`)
	got := UsageLimitUntil(429, raw, now, 300*time.Second)
	if got == nil {
		t.Fatal("expected cooldown")
	}
	if want := now.Add(42 * time.Second); !got.Equal(want) {
		t.Fatalf("cooldown = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestUsageLimitUntilIgnoresOtherRateLimits(t *testing.T) {
	now := time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC)
	got := UsageLimitUntil(429, []byte(`{"error":{"code":"rate_limit_exceeded","message":"Concurrency limit exceeded"}}`), now, 300*time.Second)
	if got != nil {
		t.Fatalf("unexpected cooldown: %s", got.Format(time.RFC3339))
	}
}
