package observability

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func TestConnectionIDGeneratorConnContext(t *testing.T) {
	generator := NewConnectionIDGenerator()

	firstContext := generator.ConnContext(context.Background(), nil)
	secondContext := generator.ConnContext(context.Background(), nil)
	firstID := ConnectionIDFromContext(firstContext)
	secondID := ConnectionIDFromContext(secondContext)

	if firstID == "" || secondID == "" {
		t.Fatalf("connection IDs must be non-empty: first=%q second=%q", firstID, secondID)
	}
	if firstID == secondID {
		t.Fatalf("connection IDs must be unique: %q", firstID)
	}

	firstPrefix, firstCounter := splitConnectionID(t, firstID)
	secondPrefix, secondCounter := splitConnectionID(t, secondID)
	if firstPrefix != secondPrefix {
		t.Fatalf("connection IDs from one generator must share a process boot ID: %q != %q", firstPrefix, secondPrefix)
	}
	if secondCounter != firstCounter+1 {
		t.Fatalf("connection counter must increase monotonically: first=%d second=%d", firstCounter, secondCounter)
	}
}

func TestConnectionIDContextRoundTripAndOverride(t *testing.T) {
	ctx := ContextWithConnectionID(context.Background(), "oaixc-test-1")
	if got := ConnectionIDFromContext(ctx); got != "oaixc-test-1" {
		t.Fatalf("unexpected connection ID: %q", got)
	}

	overridden := ContextWithConnectionID(ctx, "oaixc-test-2")
	if got := ConnectionIDFromContext(overridden); got != "oaixc-test-2" {
		t.Fatalf("connection ID was not overridden: %q", got)
	}

	unchanged := ContextWithConnectionID(overridden, "")
	if unchanged != overridden {
		t.Fatal("an empty connection ID should leave the context unchanged")
	}
	if got := ConnectionIDFromContext(unchanged); got != "oaixc-test-2" {
		t.Fatalf("an empty connection ID must not erase an existing ID: %q", got)
	}
	if got := ConnectionIDFromContext(nil); got != "" {
		t.Fatalf("a nil context should not contain a connection ID: %q", got)
	}
}

func splitConnectionID(t *testing.T, connectionID string) (string, uint64) {
	t.Helper()

	separator := strings.LastIndexByte(connectionID, '-')
	if separator <= len("oaixc-") || separator == len(connectionID)-1 {
		t.Fatalf("unexpected connection ID format: %q", connectionID)
	}
	counter, err := strconv.ParseUint(connectionID[separator+1:], 16, 64)
	if err != nil {
		t.Fatalf("parse connection counter from %q: %v", connectionID, err)
	}
	return connectionID[:separator], counter
}
