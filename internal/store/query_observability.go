package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/observability"
)

type slowQueryTraceKey struct{}

type dbQueryStatsKey struct{}

type DBQueryFingerprintStats struct {
	Fingerprint string `json:"fingerprint"`
	Operation   string `json:"operation"`
	Shape       string `json:"shape"`
	Count       int64  `json:"count"`
	DurationMS  int64  `json:"duration_ms"`
	MaxMS       int64  `json:"max_ms"`
	Errors      int64  `json:"errors"`
}

type DBQueryStatsSnapshot struct {
	Count      int64                     `json:"count"`
	DurationMS int64                     `json:"duration_ms"`
	MaxMS      int64                     `json:"max_ms"`
	Errors     int64                     `json:"errors"`
	Top        []DBQueryFingerprintStats `json:"top"`
}

type DBQueryStats struct {
	mu          sync.Mutex
	count       int64
	duration    time.Duration
	maxDuration time.Duration
	errors      int64
	byQuery     map[string]*dbQueryFingerprintAccumulator
}

type dbQueryFingerprintAccumulator struct {
	DBQueryFingerprintStats
	duration    time.Duration
	maxDuration time.Duration
}

func ContextWithDBQueryStats(ctx context.Context) (context.Context, *DBQueryStats) {
	stats := &DBQueryStats{byQuery: make(map[string]*dbQueryFingerprintAccumulator)}
	return context.WithValue(ctx, dbQueryStatsKey{}, stats), stats
}

func (s *DBQueryStats) record(trace slowQueryTrace, duration time.Duration, err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	s.duration += duration
	if duration > s.maxDuration {
		s.maxDuration = duration
	}
	if err != nil {
		s.errors++
	}
	item := s.byQuery[trace.fingerprint]
	if item == nil {
		item = &dbQueryFingerprintAccumulator{DBQueryFingerprintStats: DBQueryFingerprintStats{
			Fingerprint: trace.fingerprint,
			Operation:   trace.operation,
			Shape:       trace.shape,
		}}
		s.byQuery[trace.fingerprint] = item
	}
	item.Count++
	item.duration += duration
	if duration > item.maxDuration {
		item.maxDuration = duration
	}
	if err != nil {
		item.Errors++
	}
}

func (s *DBQueryStats) Snapshot(limit int) DBQueryStatsSnapshot {
	if s == nil {
		return DBQueryStatsSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 5
	}
	items := make([]DBQueryFingerprintStats, 0, len(s.byQuery))
	for _, accumulated := range s.byQuery {
		item := accumulated.DBQueryFingerprintStats
		item.DurationMS = accumulated.duration.Milliseconds()
		item.MaxMS = accumulated.maxDuration.Milliseconds()
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].DurationMS == items[j].DurationMS {
			return items[i].Fingerprint < items[j].Fingerprint
		}
		return items[i].DurationMS > items[j].DurationMS
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return DBQueryStatsSnapshot{
		Count:      s.count,
		DurationMS: s.duration.Milliseconds(),
		MaxMS:      s.maxDuration.Milliseconds(),
		Errors:     s.errors,
		Top:        items,
	}
}

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
	if t == nil {
		return
	}
	trace, _ := ctx.Value(slowQueryTraceKey{}).(slowQueryTrace)
	if trace.started.IsZero() {
		return
	}
	duration := time.Since(trace.started)
	if stats, _ := ctx.Value(dbQueryStatsKey{}).(*DBQueryStats); stats != nil {
		stats.record(trace, duration, data.Err)
	}
	if t.logger == nil {
		return
	}
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
