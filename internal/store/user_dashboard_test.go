package store

import (
	"testing"
	"time"
)

func TestBuildUserDashboardWindowUsesLocalCalendarBoundaries(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 14, 6, 35, 0, 0, time.UTC)
	window := buildUserDashboardWindow(now, location, UserDashboardMonth)

	assertDashboardTime(t, window.periodStarts["today"], "2026-08-14T00:00:00+08:00")
	assertDashboardTime(t, window.periodStarts["week"], "2026-08-08T00:00:00+08:00")
	assertDashboardTime(t, window.periodStarts["month"], "2026-07-16T00:00:00+08:00")
	assertDashboardTime(t, window.periodStarts["year"], "2026-01-01T00:00:00+08:00")
	if window.bucket != "day" || !window.selectedFrom.Equal(window.periodStarts["month"]) {
		t.Fatalf("unexpected selected window: %+v", window)
	}
}

func TestFillUserDashboardTrendPreservesEmptyBucketsWithoutInventingCacheRate(t *testing.T) {
	location := time.UTC
	from := time.Date(2026, time.August, 10, 0, 0, 0, 0, location)
	now := time.Date(2026, time.August, 12, 23, 0, 0, 0, location)
	ratio := 0.5
	items := []UserDashboardTrendPoint{{
		BucketStart:       from.AddDate(0, 0, 1),
		RequestCount:      2,
		InputTokens:       100,
		CachedInputTokens: 50,
		CacheHitRatio:     &ratio,
	}}
	filled := fillUserDashboardTrend(items, from, now, location, "day")
	if len(filled) != 3 {
		t.Fatalf("filled trend length = %d, want 3", len(filled))
	}
	if filled[0].CacheHitRatio != nil || filled[2].CacheHitRatio != nil {
		t.Fatalf("empty buckets must not report a zero cache rate: %+v", filled)
	}
	if filled[1].CacheHitRatio == nil || *filled[1].CacheHitRatio != ratio {
		t.Fatalf("observed bucket cache rate = %v, want %v", filled[1].CacheHitRatio, ratio)
	}
}

func assertDashboardTime(t *testing.T, got time.Time, want string) {
	t.Helper()
	if got.Format(time.RFC3339) != want {
		t.Fatalf("dashboard boundary = %s, want %s", got.Format(time.RFC3339), want)
	}
}
