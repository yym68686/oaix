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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yym68686/oaix/internal/admindiag"
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
	observation *admindiag.Handle
	started     time.Time
	fingerprint string
	shape       string
	operation   string
}

type slowQueryTracer struct {
	admin       *admindiag.Recorder
	connections sync.Map
	logger      *slog.Logger
	threshold   time.Duration
}

func newSlowQueryTracer(logger *slog.Logger, threshold time.Duration) *slowQueryTracer {
	if threshold <= 0 {
		threshold = 250 * time.Millisecond
	}
	return &slowQueryTracer{logger: logger, threshold: threshold}
}

func (t *slowQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	fullShape := safeSQLShape(data.SQL, len(data.SQL)+1)
	shape := fullShape
	if len(shape) > 240 {
		shape = strings.TrimSpace(shape[:240]) + "…"
	}
	var pid uint32
	connection := ""
	if conn != nil {
		pid = conn.PgConn().PID()
		if id, ok := t.connections.Load(conn); ok {
			connection = id.(string)
		}
	}
	fingerprint := sqlFingerprint(fullShape)
	h := admindiag.BeginEvent(ctx, "query", fingerprint, pid, connection, data.SQL)
	t.admin.Track(ctx, h)
	return context.WithValue(ctx, slowQueryTraceKey{}, slowQueryTrace{
		started:     time.Now(),
		fingerprint: fingerprint,
		observation: h,
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
	trace.observation.End(ctx, data.Err, data.CommandTag.RowsAffected())
	t.admin.Untrack(trace.observation)
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

// The fingerprint uses the whole normalized template. Display truncation is
// separate. SQL arguments are never supplied to this function.
func safeSQLShape(sql string, limit int) string {
	if limit <= 0 {
		limit = 240
	}
	var b strings.Builder
	for i := 0; i < len(sql); {
		c := sql[i]
		if i+1 < len(sql) && sql[i:i+2] == "--" {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
			continue
		}
		if i+1 < len(sql) && sql[i:i+2] == "/*" {
			depth := 1
			i += 2
			for i < len(sql) && depth > 0 {
				if i+1 < len(sql) && sql[i:i+2] == "/*" {
					depth++
					i += 2
				} else if i+1 < len(sql) && sql[i:i+2] == "*/" {
					depth--
					i += 2
				} else {
					i++
				}
			}
			b.WriteByte(' ')
			continue
		}
		if c == '\'' {
			i++
			for i < len(sql) {
				if sql[i] == '\\' && i+1 < len(sql) {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					i++
					if i < len(sql) && sql[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			b.WriteString("'?'")
			continue
		}
		if c == '$' {
			j := i + 1
			for j < len(sql) && ((sql[j] >= 'a' && sql[j] <= 'z') || (sql[j] >= 'A' && sql[j] <= 'Z') || sql[j] == '_' || (j > i+1 && sql[j] >= '0' && sql[j] <= '9')) {
				j++
			}
			if j < len(sql) && sql[j] == '$' {
				tag := sql[i : j+1]
				end := strings.Index(sql[j+1:], tag)
				if end < 0 {
					i = len(sql)
				} else {
					i = j + 1 + end + len(tag)
				}
				b.WriteString("'?'")
				continue
			}
			if j == i+1 {
				b.WriteByte(c)
				i++
				for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
					b.WriteByte(sql[i])
					i++
				}
				continue
			}
		}
		if c >= '0' && c <= '9' && (i == 0 || !((sql[i-1] >= 'a' && sql[i-1] <= 'z') || (sql[i-1] >= 'A' && sql[i-1] <= 'Z') || sql[i-1] == '_')) {
			i++
			for i < len(sql) && ((sql[i] >= '0' && sql[i] <= '9') || sql[i] == '.') {
				i++
			}
			b.WriteByte('?')
			continue
		}
		b.WriteByte(c)
		i++
	}
	shape := strings.Join(strings.Fields(b.String()), " ")
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

// These hooks measure this acquire call; global pool deltas include siblings.
type adminAcquireKey struct{}

func (t *slowQueryTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	h := admindiag.BeginEvent(ctx, "acquire", "", 0, "", "")
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, adminAcquireKey{}, h)
}
func (t *slowQueryTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, d pgxpool.TraceAcquireEndData) {
	h, _ := ctx.Value(adminAcquireKey{}).(*admindiag.Handle)
	h.End(ctx, d.Err, 0)
}
