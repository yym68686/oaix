package observability

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type requestIDContextKey struct{}

func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	requestID = NormalizeRequestID(requestID)
	if requestID == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDContextKey{}, requestID)
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	requestID, _ := ctx.Value(requestIDContextKey{}).(string)
	return requestID
}

func NewRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	digest := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	return hex.EncodeToString(digest[:16])
}

func NormalizeRequestID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 128 {
		return ""
	}
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("-_.:", char) {
			continue
		}
		return ""
	}
	return value
}
