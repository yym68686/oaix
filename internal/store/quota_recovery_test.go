package store

import (
	"testing"
	"time"
)

func TestQuotaRecoveryUsageLimitCooldownNeverWritesPastTime(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Second)
	got := quotaRecoveryUsageLimitCooldown(expired, nil, now, 7*time.Minute)
	if want := now.Add(7 * time.Minute); !got.Equal(want) {
		t.Fatalf("cooldown = %s, want %s", got, want)
	}
}

func TestQuotaRecoveryUsageLimitCooldownPreservesFutureCooldownWithoutReset(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	previous := now.Add(7 * 24 * time.Hour)
	got := quotaRecoveryUsageLimitCooldown(previous, nil, now, 5*time.Minute)
	if !got.Equal(previous) {
		t.Fatalf("cooldown = %s, want preserved %s", got, previous)
	}
}

func TestQuotaRecoveryUsageLimitCooldownUsesExplicitFutureReset(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	previous := now.Add(7 * 24 * time.Hour)
	reset := now.Add(2 * time.Hour)
	got := quotaRecoveryUsageLimitCooldown(previous, &reset, now, 5*time.Minute)
	if !got.Equal(reset) {
		t.Fatalf("cooldown = %s, want explicit reset %s", got, reset)
	}
}
