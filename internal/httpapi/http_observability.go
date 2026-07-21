package httpapi

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/observability"
	"github.com/yym68686/oaix/internal/store"
)

const slowOwnedAPIDuration = time.Second

type observedResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	started     time.Time
	firstByteAt time.Time
}

func (w *observedResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.firstByteAt = time.Now()
	w.ResponseWriter.WriteHeader(status)
}

func (w *observedResponseWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(value)
	w.bytes += int64(n)
	return n, err
}

func (w *observedResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *observedResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *observedResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *observedResponseWriter) firstByteDuration() time.Duration {
	if w.firstByteAt.IsZero() {
		return 0
	}
	return w.firstByteAt.Sub(w.started)
}

type httpRouteMetric struct {
	Method         string
	Route          string
	Requests       uint64
	Errors         uint64
	Slow           uint64
	DurationMs     uint64
	FirstByteMs    uint64
	FirstByteCount uint64
	ResponseBytes  uint64
	MaxDurationMs  uint64
}

type httpRouteMetrics struct {
	mu     sync.Mutex
	routes map[string]*httpRouteMetric
}

func newHTTPRouteMetrics() *httpRouteMetrics {
	return &httpRouteMetrics{routes: map[string]*httpRouteMetric{}}
}

func (m *httpRouteMetrics) observe(method string, route string, status int, duration time.Duration, firstByte time.Duration, responseBytes int64) {
	if m == nil {
		return
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	route = normalizeRoutePattern(method, route)
	if route == "" {
		route = "unmatched"
	}
	key := method + " " + route
	m.mu.Lock()
	defer m.mu.Unlock()
	item := m.routes[key]
	if item == nil {
		item = &httpRouteMetric{Method: method, Route: route}
		m.routes[key] = item
	}
	durationMillis := duration.Milliseconds()
	if durationMillis < 0 {
		durationMillis = 0
	}
	durationMs := uint64(durationMillis)
	item.Requests++
	item.DurationMs += durationMs
	if status >= http.StatusInternalServerError {
		item.Errors++
	}
	if duration >= slowOwnedAPIDuration {
		item.Slow++
	}
	if durationMs > item.MaxDurationMs {
		item.MaxDurationMs = durationMs
	}
	if firstByte > 0 {
		firstByteMillis := firstByte.Milliseconds()
		if firstByteMillis < 0 {
			firstByteMillis = 0
		}
		item.FirstByteMs += uint64(firstByteMillis)
		item.FirstByteCount++
	}
	if responseBytes > 0 {
		item.ResponseBytes += uint64(responseBytes)
	}
}

func normalizeRoutePattern(method string, route string) string {
	route = strings.TrimSpace(route)
	prefix := strings.ToUpper(strings.TrimSpace(method)) + " "
	if strings.HasPrefix(route, prefix) {
		route = strings.TrimSpace(strings.TrimPrefix(route, prefix))
	}
	return route
}

func (m *httpRouteMetrics) snapshot() []httpRouteMetric {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]httpRouteMetric, 0, len(m.routes))
	for _, item := range m.routes {
		out = append(out, *item)
	}
	sort.Slice(out, func(i int, j int) bool {
		if out[i].Route == out[j].Route {
			return out[i].Method < out[j].Method
		}
		return out[i].Route < out[j].Route
	})
	return out
}

func (a *App) observeHTTP(next http.Handler) http.Handler {
	if a.httpRoutes == nil {
		a.httpRoutes = newHTTPRouteMetrics()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		requestID := observability.NormalizeRequestID(r.Header.Get("X-Request-ID"))
		if requestID == "" {
			requestID = observability.NewRequestID()
		}
		ctx := observability.ContextWithRequestID(r.Context(), requestID)
		ctx, dbQueryStats := store.ContextWithDBQueryStats(ctx)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", requestID)
		before := a.poolStats()
		observed := &observedResponseWriter{ResponseWriter: w, started: started}
		next.ServeHTTP(observed, r)
		duration := time.Since(started)
		status := observed.statusCode()
		route := normalizeRoutePattern(r.Method, r.Pattern)
		if route == "" {
			route = "unmatched"
		}
		a.httpRoutes.observe(r.Method, route, status, duration, observed.firstByteDuration(), observed.bytes)

		ownedAPI := strings.HasPrefix(route, "/admin/") || strings.HasPrefix(route, "/api/") || route == "/healthz"
		if a.logger == nil || status < http.StatusInternalServerError && (!ownedAPI || duration < slowOwnedAPIDuration) {
			return
		}
		after := a.poolStats()
		requestDB := dbQueryStats.Snapshot(5)
		message := "http request slow"
		if status >= http.StatusInternalServerError {
			message = "http request failed"
		}
		a.logger.Warn(message,
			"request_id", requestID,
			"method", r.Method,
			"route", route,
			"status", status,
			"duration_ms", duration.Milliseconds(),
			"first_byte_ms", observed.firstByteDuration().Milliseconds(),
			"response_bytes", observed.bytes,
			"db_query_count", requestDB.Count,
			"db_query_duration_ms", requestDB.DurationMS,
			"db_query_max_duration_ms", requestDB.MaxMS,
			"db_query_error_count", requestDB.Errors,
			"db_query_top", requestDB.Top,
			"db_acquire_count_delta", after.AcquireCount-before.AcquireCount,
			"db_acquire_duration_ms_delta", after.AcquireDurationMs-before.AcquireDurationMs,
			"db_empty_acquire_count_delta", after.EmptyAcquireCount-before.EmptyAcquireCount,
			"db_canceled_acquire_count_delta", after.CanceledAcquireCount-before.CanceledAcquireCount,
			"db_acquired_conns", after.AcquiredConns,
		)
	})
}

func (a *App) poolStats() store.PoolStats {
	if a == nil || a.store == nil {
		return store.PoolStats{}
	}
	return a.store.PoolStats()
}

func (a *App) writeHTTPRouteMetrics(w io.Writer) {
	if a == nil || a.httpRoutes == nil {
		return
	}
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_requests_total Completed HTTP requests by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_requests_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_errors_total HTTP 5xx responses by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_errors_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_slow_total Requests taking at least one second by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_slow_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_duration_milliseconds_total Cumulative request duration by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_duration_milliseconds_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_max_duration_milliseconds Maximum observed request duration by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_max_duration_milliseconds gauge")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_first_byte_milliseconds_total Cumulative time to first response byte by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_first_byte_milliseconds_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_request_first_byte_observations_total Responses with an observed first byte by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_request_first_byte_observations_total counter")
	_, _ = fmt.Fprintln(w, "# HELP oaix_http_response_bytes_total Cumulative response bytes by route.")
	_, _ = fmt.Fprintln(w, "# TYPE oaix_http_response_bytes_total counter")
	for _, item := range a.httpRoutes.snapshot() {
		labels := fmt.Sprintf("method=\"%s\",route=\"%s\"", prometheusLabel(item.Method), prometheusLabel(item.Route))
		_, _ = fmt.Fprintf(w, "oaix_http_requests_total{%s} %d\n", labels, item.Requests)
		_, _ = fmt.Fprintf(w, "oaix_http_request_errors_total{%s} %d\n", labels, item.Errors)
		_, _ = fmt.Fprintf(w, "oaix_http_request_slow_total{%s} %d\n", labels, item.Slow)
		_, _ = fmt.Fprintf(w, "oaix_http_request_duration_milliseconds_total{%s} %d\n", labels, item.DurationMs)
		_, _ = fmt.Fprintf(w, "oaix_http_request_max_duration_milliseconds{%s} %d\n", labels, item.MaxDurationMs)
		_, _ = fmt.Fprintf(w, "oaix_http_request_first_byte_milliseconds_total{%s} %d\n", labels, item.FirstByteMs)
		_, _ = fmt.Fprintf(w, "oaix_http_request_first_byte_observations_total{%s} %d\n", labels, item.FirstByteCount)
		_, _ = fmt.Fprintf(w, "oaix_http_response_bytes_total{%s} %d\n", labels, item.ResponseBytes)
	}
}
