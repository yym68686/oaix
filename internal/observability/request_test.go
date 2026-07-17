package observability

import (
	"context"
	"testing"
)

func TestRequestIDContextAndNormalization(t *testing.T) {
	ctx := ContextWithRequestID(context.Background(), " request-123 ")
	if got := RequestIDFromContext(ctx); got != "request-123" {
		t.Fatalf("request id = %q", got)
	}
	if got := NormalizeRequestID("bad header value"); got != "" {
		t.Fatalf("invalid request id = %q", got)
	}
	if got := NewRequestID(); len(got) != 32 {
		t.Fatalf("generated request id = %q", got)
	}
}
