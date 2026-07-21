package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestSafeSQLShapeRedactsLiteralsAndKeepsRelations(t *testing.T) {
	shape := safeSQLShape("select * from gateway_request_logs where request_id = 'secret-value' and status = 'it''s-bad'", 240)
	if strings.Contains(shape, "secret-value") || strings.Contains(shape, "it''s-bad") {
		t.Fatalf("literal leaked in SQL shape: %s", shape)
	}
	if !strings.Contains(shape, "gateway_request_logs") || strings.Count(shape, "'?'") != 2 {
		t.Fatalf("unexpected SQL shape: %s", shape)
	}
}

func TestSQLFingerprintIsStableAndBounded(t *testing.T) {
	first := sqlFingerprint("select * from example where id = $1")
	second := sqlFingerprint("select * from example where id = $1")
	if first != second || len(first) != 12 {
		t.Fatalf("fingerprints = %q %q", first, second)
	}
}

func TestSafeSQLShapeTruncates(t *testing.T) {
	shape := safeSQLShape(strings.Repeat("select column from relation ", 20), 64)
	if len(shape) > 67 || !strings.HasSuffix(shape, "…") {
		t.Fatalf("shape was not bounded: %q", shape)
	}
}

func TestDBQueryStatsAreRequestScopedAndRanked(t *testing.T) {
	ctx, stats := ContextWithDBQueryStats(context.Background())
	fromContext, _ := ctx.Value(dbQueryStatsKey{}).(*DBQueryStats)
	if fromContext != stats {
		t.Fatal("query stats were not attached to the request context")
	}
	stats.record(slowQueryTrace{fingerprint: "quick", operation: "select", shape: "select 1"}, 5*time.Millisecond, nil)
	stats.record(slowQueryTrace{fingerprint: "slow", operation: "select", shape: "select 2"}, 30*time.Millisecond, nil)
	stats.record(slowQueryTrace{fingerprint: "slow", operation: "select", shape: "select 2"}, 20*time.Millisecond, context.Canceled)

	snapshot := stats.Snapshot(1)
	if snapshot.Count != 3 || snapshot.DurationMS != 55 || snapshot.MaxMS != 30 || snapshot.Errors != 1 {
		t.Fatalf("unexpected query stats: %+v", snapshot)
	}
	if len(snapshot.Top) != 1 || snapshot.Top[0].Fingerprint != "slow" || snapshot.Top[0].Count != 2 || snapshot.Top[0].DurationMS != 50 || snapshot.Top[0].Errors != 1 {
		t.Fatalf("unexpected top query stats: %+v", snapshot.Top)
	}
}
