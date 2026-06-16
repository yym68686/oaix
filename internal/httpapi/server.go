package httpapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
)

type App struct {
	cfg      config.Config
	logger   *slog.Logger
	store    *store.Store
	tokens   *tokens.Manager
	logs     *logs.Writer
	proxy    *proxy.Pipeline
	webDir   string
	started  time.Time
	authKeys []string
}

func NewApp(cfg config.Config, logger *slog.Logger, store *store.Store, tokenManager *tokens.Manager, logWriter *logs.Writer, pipeline *proxy.Pipeline) *App {
	return &App{
		cfg:      cfg,
		logger:   logger,
		store:    store,
		tokens:   tokenManager,
		logs:     logWriter,
		proxy:    pipeline,
		webDir:   filepath.Join("oaix_gateway", "web"),
		started:  time.Now().UTC(),
		authKeys: cfg.Auth.ServiceAPIKeys,
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.index)
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir(a.webDir))))
	mux.HandleFunc("GET /livez", a.livez)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /admin/token-selection", a.requireAuth(a.tokenSelection))
	mux.HandleFunc("POST /admin/token-selection", a.requireAuth(a.updateTokenSelection))
	mux.HandleFunc("GET /admin/tokens", a.requireAuth(a.listTokens))
	mux.HandleFunc("POST /admin/tokens/batch", a.requireAuth(a.batchTokens))
	mux.HandleFunc("GET /admin/tokens/{token_id}", a.requireAuth(a.getToken))
	mux.HandleFunc("POST /admin/tokens/import", a.requireAuth(a.importTokens))
	mux.HandleFunc("GET /admin/tokens/import-batches", a.requireAuth(a.listImportBatches))
	mux.HandleFunc("GET /admin/tokens/import-jobs/{job_id}", a.requireAuth(a.getImportJob))
	mux.HandleFunc("POST /admin/tokens/import-jobs/{job_id}/cancel", a.requireAuth(a.cancelImportJob))
	mux.HandleFunc("POST /admin/tokens/{token_id}/activation", a.requireAuth(a.updateTokenActivation))
	mux.HandleFunc("POST /admin/tokens/{token_id}/remark", a.requireAuth(a.updateTokenRemark))
	mux.HandleFunc("DELETE /admin/tokens/{token_id}", a.requireAuth(a.deleteToken))
	mux.HandleFunc("GET /admin/requests", a.requireAuth(a.listRequests))
	mux.HandleFunc("GET /admin/analytics", a.requireAuth(a.analytics))
	mux.HandleFunc("GET /admin/runtime", a.requireAuth(a.runtime))
	mux.HandleFunc("GET /admin/settings", a.requireAuth(a.listSettings))
	mux.HandleFunc("POST /admin/settings/{key}", a.requireAuth(a.updateSetting))
	mux.HandleFunc("POST /v1/responses", a.requireAuth(a.responses))
	mux.HandleFunc("GET /v1/responses/{response_id}", a.requireAuth(a.getResponse))
	mux.HandleFunc("DELETE /v1/responses/{response_id}", a.requireAuth(a.deleteResponse))
	mux.HandleFunc("POST /v1/responses/{response_id}/cancel", a.requireAuth(a.cancelResponse))
	mux.HandleFunc("POST /v1/responses/compact", a.requireAuth(a.responsesCompact))
	mux.HandleFunc("POST /v1/chat/completions", a.requireAuth(a.chatCompletions))
	mux.HandleFunc("POST /v1/images/generations", a.requireAuth(a.imagesGenerations))
	mux.HandleFunc("POST /v1/images/edits", a.requireAuth(a.imagesEdits))
	return a.recoverer(a.cors(mux))
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(a.webDir, "index.html")
	data, err := os.ReadFile(path)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, "<!doctype html><title>oaix</title><h1>oaix gateway</h1>")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
	_, _ = w.Write(data)
}

func (a *App) livez(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"uptime_seconds": int(time.Since(a.started).Seconds()),
		"token_pool":     a.tokens.Stats(),
		"request_log":    a.logs.Stats(),
	})
}

func (a *App) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	counts, err := a.store.TokenCounts(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":       false,
			"degraded": true,
			"error":    err.Error(),
		})
		return
	}
	ok := counts.Available > 0
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]any{
		"ok":                    ok,
		"degraded":              !ok,
		"counts":                counts,
		"upstream":              a.cfg.Upstream.ResponsesURL,
		"service_key_protected": len(a.authKeys) > 0,
		"token_pool":            a.tokens.Stats(),
	})
}

func (a *App) metrics(w http.ResponseWriter, r *http.Request) {
	tokenStats := a.tokens.Stats()
	logStats := a.logs.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "# HELP oaix_token_ready_tokens Ready tokens in the current snapshot.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_token_ready_tokens gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_token_ready_tokens %d\n", tokenStats.ReadyTokens)
	_, _ = fmt.Fprintf(w, "# HELP oaix_token_active_streams Active streams tracked by the gateway.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_token_active_streams gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_token_active_streams %d\n", tokenStats.ActiveStreams)
	_, _ = fmt.Fprintf(w, "# HELP oaix_token_snapshot_age_ms Age of the token snapshot in milliseconds.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_token_snapshot_age_ms gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_token_snapshot_age_ms %d\n", tokenStats.AgeMs)
	_, _ = fmt.Fprintf(w, "# HELP oaix_token_selector_misses_total Token selector misses.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_token_selector_misses_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_token_selector_misses_total %d\n", a.tokens.SelectorMisses())
	_, _ = fmt.Fprintf(w, "# HELP oaix_token_state_commit_failures_total Token runtime state commit failures after retry.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_token_state_commit_failures_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_token_state_commit_failures_total %d\n", a.proxy.StateCommitFailures())
	_, _ = fmt.Fprintf(w, "# HELP oaix_request_log_queue_depth Request log writer queue depth.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_request_log_queue_depth gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_request_log_queue_depth %d\n", logStats.Queued)
	_, _ = fmt.Fprintf(w, "# HELP oaix_request_log_written_total Request log rows written by the async writer.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_request_log_written_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_request_log_written_total %d\n", logStats.Written)
	_, _ = fmt.Fprintf(w, "# HELP oaix_request_log_dropped_total Request log rows dropped after fallback failed.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_request_log_dropped_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_request_log_dropped_total %d\n", logStats.Dropped)
	_, _ = fmt.Fprintf(w, "# HELP oaix_request_log_outbox_writes_total Request log rows written to outbox fallback.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_request_log_outbox_writes_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_request_log_outbox_writes_total %d\n", logStats.OutboxWrites)
	upstreamStats := a.proxy.TransportStats()
	_, _ = fmt.Fprintf(w, "# HELP oaix_upstream_requests_total Upstream requests issued.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_upstream_requests_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_upstream_requests_total %d\n", upstreamStats.RequestsTotal)
	_, _ = fmt.Fprintf(w, "# HELP oaix_upstream_errors_total Upstream transport errors.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_upstream_errors_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_upstream_errors_total %d\n", upstreamStats.ErrorsTotal)
	_, _ = fmt.Fprintf(w, "# HELP oaix_upstream_active_requests Active upstream requests.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_upstream_active_requests gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_upstream_active_requests %d\n", upstreamStats.Active)
	dbStats := a.store.PoolStats()
	_, _ = fmt.Fprintf(w, "# HELP oaix_db_pool_acquired_conns Acquired database connections.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_db_pool_acquired_conns gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_db_pool_acquired_conns %d\n", dbStats.AcquiredConns)
	_, _ = fmt.Fprintf(w, "# HELP oaix_db_pool_idle_conns Idle database connections.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_db_pool_idle_conns gauge\n")
	_, _ = fmt.Fprintf(w, "oaix_db_pool_idle_conns %d\n", dbStats.IdleConns)
	_, _ = fmt.Fprintf(w, "# HELP oaix_db_pool_acquire_count_total Database pool acquire count.\n")
	_, _ = fmt.Fprintf(w, "# TYPE oaix_db_pool_acquire_count_total counter\n")
	_, _ = fmt.Fprintf(w, "oaix_db_pool_acquire_count_total %d\n", dbStats.AcquireCount)
}

func (a *App) tokenSelection(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"strategy":          "snapshot_round_robin",
		"active_stream_cap": a.cfg.TokenPool.ActiveStreamCap,
		"snapshot":          a.tokens.Stats(),
	})
}

func (a *App) updateTokenSelection(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"strategy":          "snapshot_round_robin",
		"active_stream_cap": a.cfg.TokenPool.ActiveStreamCap,
	})
}

func (a *App) listTokens(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 100)
	offset := queryInt(r, "offset", 0)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, total, err := a.store.ListTokens(ctx, store.TokenListOptions{
		Limit:  limit,
		Offset: offset,
		Query:  r.URL.Query().Get("q"),
		Status: r.URL.Query().Get("status"),
		Sort:   r.URL.Query().Get("sort"),
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	counts, _ := a.store.TokenCounts(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"counts":          counts,
		"filtered_counts": counts,
		"pagination": map[string]any{
			"limit":        limit,
			"offset":       offset,
			"returned":     len(items),
			"total":        total,
			"has_previous": offset > 0,
			"has_next":     offset+len(items) < total,
		},
		"items": items,
	})
}

func (a *App) importTokens(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var body any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	payloads, tokensToImport, queuePosition, err := parseImportPayload(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := a.store.UpsertAccessTokens(ctx, tokensToImport, "admin_api")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	job, err := a.store.CreateCompletedImportJob(ctx, payloads, queuePosition, result)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
		a.logger.Warn("token snapshot refresh after import failed", "error", err)
	}
	_ = a.store.WriteAuditLog(ctx, "token_import", "api", "import_job", strconv.FormatInt(job.ID, 10), map[string]any{
		"created": result.Created,
		"updated": result.Updated,
		"skipped": result.Skipped,
		"failed":  result.Failed,
	})
	writeJSON(w, http.StatusAccepted, map[string]any{
		"job":    job,
		"result": result,
	})
}

func (a *App) getToken(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid token id"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetToken(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, token)
}

func (a *App) listImportBatches(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListImportJobs(ctx, queryInt(r, "limit", 30))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) getImportJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("job_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid job id"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	job, err := a.store.GetImportJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("import job not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (a *App) cancelImportJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("job_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid job id"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	job, err := a.store.CancelImportJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("import job not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (a *App) batchTokens(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Action        string  `json:"action"`
		TokenIDs      []int64 `json:"token_ids"`
		Active        *bool   `json:"active"`
		ClearCooldown bool    `json:"clear_cooldown"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(payload.TokenIDs) == 0 || len(payload.TokenIDs) > 500 {
		writeError(w, http.StatusBadRequest, errors.New("token_ids must contain 1..500 ids"))
		return
	}
	action := strings.TrimSpace(strings.ToLower(payload.Action))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var updated, deleted, failed int
	for _, id := range payload.TokenIDs {
		if id <= 0 {
			failed++
			continue
		}
		switch action {
		case "activate", "deactivate", "set_active":
			active := action == "activate"
			if payload.Active != nil {
				active = *payload.Active
			}
			if _, err := a.store.SetTokenActive(ctx, id, active, payload.ClearCooldown); err != nil {
				failed++
			} else {
				updated++
			}
		case "delete":
			if err := a.store.DeleteToken(ctx, id); err != nil {
				failed++
			} else {
				deleted++
			}
		default:
			writeError(w, http.StatusBadRequest, errors.New("action must be activate, deactivate, set_active, or delete"))
			return
		}
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_batch_"+action, "api", "token", "", payload)
	writeJSON(w, http.StatusOK, map[string]any{
		"updated": updated,
		"deleted": deleted,
		"failed":  failed,
	})
}

func (a *App) updateTokenActivation(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid token id"))
		return
	}
	var payload struct {
		Active        bool `json:"active"`
		ClearCooldown bool `json:"clear_cooldown"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.SetTokenActive(ctx, id, payload.Active, payload.ClearCooldown)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_activation", "api", "token", strconv.FormatInt(id, 10), payload)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) updateTokenRemark(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid token id"))
		return
	}
	var payload struct {
		Remark string `json:"remark"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.UpdateTokenRemark(ctx, id, payload.Remark)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_remark", "api", "token", strconv.FormatInt(id, 10), payload)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) deleteToken(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid token id"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteToken(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_delete", "api", "token", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "deleted": true})
}

func (a *App) listRequests(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	summary, err := a.store.RequestLogSummary(ctx, 24)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	items, err := a.store.ListRequestLogs(ctx, queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "items": items})
}

func (a *App) analytics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	payload, err := a.store.RequestAnalytics(ctx, queryInt(r, "hours", 24))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (a *App) runtime(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"token_pool":  a.tokens.Stats(),
		"request_log": a.logs.Stats(),
		"upstream":    a.proxy.TransportStats(),
		"state_commit": map[string]any{
			"failures": a.proxy.StateCommitFailures(),
		},
		"db_pool": a.store.PoolStats(),
		"config":  a.cfg.SanitizedSummary(),
	})
}

func (a *App) listSettings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListSettings(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) updateSetting(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.PathValue("key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, errors.New("setting key is required"))
		return
	}
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if !json.Valid(body) {
		writeError(w, http.StatusBadRequest, errors.New("setting value must be valid JSON"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.UpsertSetting(ctx, key, body)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "setting_update", "api", "setting", key, json.RawMessage(body))
	writeJSON(w, http.StatusOK, item)
}

func (a *App) responses(w http.ResponseWriter, r *http.Request) {
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/responses", Stream: requestStream(r)})
}

func (a *App) getResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}", Method: http.MethodGet, UpstreamSuffix: responseID})
}

func (a *App) deleteResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}", Method: http.MethodDelete, UpstreamSuffix: responseID})
}

func (a *App) cancelResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}/cancel", Method: http.MethodPost, UpstreamSuffix: responseID + "/cancel"})
}

func (a *App) responsesCompact(w http.ResponseWriter, r *http.Request) {
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/compact", Stream: requestStream(r), Compact: true})
}

func (a *App) chatCompletions(w http.ResponseWriter, r *http.Request) {
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/chat/completions", Stream: requestStream(r)})
}

func (a *App) imagesGenerations(w http.ResponseWriter, r *http.Request) {
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/images/generations", Stream: requestStream(r), Model: "gpt-image-2"})
}

func (a *App) imagesEdits(w http.ResponseWriter, r *http.Request) {
	a.proxy.Proxy(w, r, proxy.RequestIntent{Endpoint: "/v1/images/edits", Stream: requestStream(r), Model: "gpt-image-2"})
}

func (a *App) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(a.authKeys) == 0 {
			next(w, r)
			return
		}
		provided := bearerToken(r)
		if provided == "" {
			provided = strings.TrimSpace(r.Header.Get("X-API-Key"))
		}
		for _, key := range a.authKeys {
			if subtle.ConstantTimeCompare([]byte(provided), []byte(key)) == 1 {
				next(w, r)
				return
			}
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid or missing service API key"})
	}
}

func (a *App) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if a.logger != nil {
					a.logger.Error("panic recovered", "panic", recovered, "path", r.URL.Path)
				}
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (a *App) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-API-Key,X-Request-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseImportPayload(body any) ([]map[string]any, []string, string, error) {
	queuePosition := "front"
	var values []any
	switch typed := body.(type) {
	case map[string]any:
		if qp, ok := typed["import_queue_position"].(string); ok && strings.TrimSpace(qp) != "" {
			queuePosition = strings.TrimSpace(qp)
		}
		if list, ok := typed["tokens"].([]any); ok {
			values = list
		} else {
			values = []any{typed}
		}
	case []any:
		values = typed
	default:
		return nil, nil, queuePosition, errors.New("body must be a token object, an array, or {tokens: [...]}")
	}
	payloads := make([]map[string]any, 0, len(values))
	accessTokens := make([]string, 0, len(values))
	for _, value := range values {
		switch item := value.(type) {
		case string:
			text := strings.TrimSpace(item)
			if text != "" {
				payloads = append(payloads, map[string]any{"access_token": text})
				accessTokens = append(accessTokens, text)
			}
		case map[string]any:
			payloads = append(payloads, item)
			for _, key := range []string{"access_token", "accessToken", "token"} {
				if text, ok := item[key].(string); ok && strings.TrimSpace(text) != "" {
					accessTokens = append(accessTokens, strings.TrimSpace(text))
					break
				}
			}
		default:
			return nil, nil, queuePosition, fmt.Errorf("unsupported token payload %T", value)
		}
	}
	return payloads, accessTokens, queuePosition, nil
}

func requestStream(r *http.Request) bool {
	if strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	return strings.Contains(strings.ToLower(r.URL.RawQuery), "stream=true")
}

func queryInt(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func bearerToken(r *http.Request) string {
	value := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"detail": err.Error()})
}
