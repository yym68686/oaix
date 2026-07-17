package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/observability"
)

type slowQueryTraceKey struct{}

type slowQueryTrace struct {
	started     time.Time
	fingerprint string
	shape       string
	operation   string
}

type slowQueryTracer struct {
	logger    *slog.Logger
	threshold time.Duration
}

func newSlowQueryTracer(logger *slog.Logger, threshold time.Duration) *slowQueryTracer {
	if threshold <= 0 {
		threshold = 250 * time.Millisecond
	}
	return &slowQueryTracer{logger: logger, threshold: threshold}
}

func (t *slowQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	shape := safeSQLShape(data.SQL, 240)
	return context.WithValue(ctx, slowQueryTraceKey{}, slowQueryTrace{
		started:     time.Now(),
		fingerprint: sqlFingerprint(shape),
		shape:       shape,
		operation:   sqlOperation(shape),
	})
}

func (t *slowQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if t == nil || t.logger == nil {
		return
	}
	trace, _ := ctx.Value(slowQueryTraceKey{}).(slowQueryTrace)
	if trace.started.IsZero() {
		return
	}
	duration := time.Since(trace.started)
	if data.Err == nil && duration < t.threshold {
		return
	}
	message := "database query slow"
	if data.Err != nil {
		message = "database query failed"
	}
	t.logger.Warn(message,
		"request_id", observability.RequestIDFromContext(ctx),
		"sql_fingerprint", trace.fingerprint,
		"sql_operation", trace.operation,
		"sql_shape", trace.shape,
		"duration_ms", duration.Milliseconds(),
		"command_tag", data.CommandTag.String(),
		"error", data.Err,
	)
}

func safeSQLShape(sql string, limit int) string {
	if limit <= 0 {
		limit = 240
	}
	var builder strings.Builder
	inLiteral := false
	for index := 0; index < len(sql); index++ {
		char := sql[index]
		if inLiteral {
			if char != '\'' {
				continue
			}
			if index+1 < len(sql) && sql[index+1] == '\'' {
				index++
				continue
			}
			inLiteral = false
			builder.WriteString("'?'")
			continue
		}
		if char == '\'' {
			inLiteral = true
			continue
		}
		builder.WriteByte(char)
	}
	if inLiteral {
		builder.WriteString("'?'")
	}
	shape := strings.Join(strings.Fields(builder.String()), " ")
	if len(shape) > limit {
		shape = strings.TrimSpace(shape[:limit]) + "…"
	}
	return shape
}

func sqlFingerprint(shape string) string {
	digest := sha256.Sum256([]byte(shape))
	return fmt.Sprintf("%x", digest[:6])
}

func sqlOperation(shape string) string {
	fields := strings.Fields(shape)
	if len(fields) == 0 {
		return "unknown"
	}
	return strings.ToLower(fields[0])
}
