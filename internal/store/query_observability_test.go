package store

import (
	"strings"
	"testing"
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
