package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

type connectionIDContextKey struct{}

// ConnectionIDGenerator assigns one opaque ID to every connection accepted by
// net/http. The random process boot ID prevents counter collisions across
// gateway restarts and replicas.
type ConnectionIDGenerator struct {
	processBootID string
	next          atomic.Uint64
}

func NewConnectionIDGenerator() *ConnectionIDGenerator {
	return &ConnectionIDGenerator{processBootID: newProcessBootID()}
}

// ConnContext is intended to be installed directly as http.Server.ConnContext.
func (g *ConnectionIDGenerator) ConnContext(ctx context.Context, _ net.Conn) context.Context {
	if g == nil {
		return ctx
	}
	id := fmt.Sprintf("oaixc-%s-%x", g.processBootID, g.next.Add(1))
	return ContextWithConnectionID(ctx, id)
}

func ContextWithConnectionID(ctx context.Context, connectionID string) context.Context {
	if connectionID == "" {
		return ctx
	}
	return context.WithValue(ctx, connectionIDContextKey{}, connectionID)
}

func ConnectionIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	connectionID, _ := ctx.Value(connectionIDContextKey{}).(string)
	return connectionID
}

func newProcessBootID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	// crypto/rand failure is exceptionally rare. Keep the fallback opaque and
	// process-local without using a client address or other personal data.
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), os.Getpid())
}
