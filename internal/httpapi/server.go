package httpapi

import (
	"context"
	"crypto/sha256"
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
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
)

type App struct {
	cfg       config.Config
	logger    *slog.Logger
	store     *store.Store
	tokens    *tokens.Manager
	logs      *logs.Writer
	proxy     *proxy.Pipeline
	quota     *adminQuotaService
	quotaJobs *quotaRefreshRegistry
	oauth     *openAIOAuthSessionStore
	webDir    string
	started   time.Time
	authKeys  []string
}

type adminImportItem struct {
	ID                 int64      `json:"id"`
	JobID              int64      `json:"job_id"`
	ItemIndex          int        `json:"item_index"`
	Status             string     `json:"status"`
	TokenID            *int64     `json:"token_id,omitempty"`
	Action             *string    `json:"action,omitempty"`
	ErrorMessage       *string    `json:"error_message,omitempty"`
	ValidationMS       *int       `json:"validation_ms,omitempty"`
	PublishMS          *int       `json:"publish_ms,omitempty"`
	ValidationStarted  *time.Time `json:"validation_started_at,omitempty"`
	ValidationFinished *time.Time `json:"validation_finished_at,omitempty"`
	PublishedAt        *time.Time `json:"published_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func NewApp(cfg config.Config, logger *slog.Logger, store *store.Store, tokenManager *tokens.Manager, logWriter *logs.Writer, pipeline *proxy.Pipeline) *App {
	return &App{
		cfg:       cfg,
		logger:    logger,
		store:     store,
		tokens:    tokenManager,
		logs:      logWriter,
		proxy:     pipeline,
		quota:     newAdminQuotaService(cfg, store),
		quotaJobs: newQuotaRefreshRegistry(),
		oauth:     newOpenAIOAuthSessionStore(),
		webDir:    filepath.Join("oaix_gateway", "web"),
		started:   time.Now().UTC(),
		authKeys:  cfg.Auth.ServiceAPIKeys,
	}
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", a.index)
	mux.HandleFunc("GET /auth/callback", a.openAIOAuthCallback)
	mux.Handle("GET /assets/", a.assets())
	mux.HandleFunc("GET /livez", a.livez)
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /metrics", a.metrics)
	a.registerUserAPIRoutes(mux)
	a.registerPlatformAdminAPIRoutes(mux)
	mux.HandleFunc("GET /admin/token-selection", a.requireAuth(a.tokenSelection))
	mux.HandleFunc("POST /admin/token-selection", a.requireAuth(a.updateTokenSelection))
	mux.HandleFunc("GET /admin/tokens", a.requireAuth(a.listTokens))
	mux.HandleFunc("POST /admin/tokens/batch", a.requireAuth(a.batchTokens))
	mux.HandleFunc("GET /admin/tokens/costs", a.requireAuth(a.listTokenCosts))
	mux.HandleFunc("GET /admin/tokens/{token_id}", a.requireAuth(a.getToken))
	mux.HandleFunc("GET /admin/tokens/quota", a.requireAuth(a.listTokenQuota))
	mux.HandleFunc("POST /admin/tokens/import", a.requireAuth(a.importTokens))
	mux.HandleFunc("POST /admin/oauth/openai/start", a.requireAuth(a.startOpenAIOAuth))
	mux.HandleFunc("POST /admin/oauth/openai/exchange", a.requireAuth(a.exchangeOpenAIOAuth))
	mux.HandleFunc("GET /admin/tokens/import-batches", a.requireAuth(a.listImportBatches))
	mux.HandleFunc("GET /admin/tokens/import-jobs/{job_id}", a.requireAuth(a.getImportJob))
	mux.HandleFunc("POST /admin/tokens/import-jobs/{job_id}/cancel", a.requireAuth(a.cancelImportJob))
	mux.HandleFunc("DELETE /admin/tokens/import-jobs/{job_id}", a.requireAuth(a.deleteImportJob))
	mux.HandleFunc("POST /admin/tokens/{token_id}/activation", a.requireAuth(a.updateTokenActivation))
	mux.HandleFunc("POST /admin/tokens/{token_id}/remark", a.requireAuth(a.updateTokenRemark))
	mux.HandleFunc("POST /admin/tokens/{token_id}/probe", a.requireAuth(a.probeToken))
	mux.HandleFunc("DELETE /admin/tokens/{token_id}", a.requireAuth(a.deleteToken))
	mux.HandleFunc("GET /admin/requests", a.requireAuth(a.listRequests))
	mux.HandleFunc("GET /admin/analytics", a.requireAuth(a.analytics))
	mux.HandleFunc("GET /admin/runtime", a.requireAuth(a.runtime))
	mux.HandleFunc("GET /admin/settings", a.requireAuth(a.listSettings))
	mux.HandleFunc("POST /admin/settings/{key}", a.requireAuth(a.updateSetting))
	a.registerAdminAPIRoutes(mux)
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

func (a *App) assets() http.Handler {
	files := http.StripPrefix("/assets/", http.FileServer(http.Dir(a.webDir)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}

func (a *App) index(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(a.webDir, "index.html")
	data, err := os.ReadFile(path)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, "<!doctype html><title>oaix</title><h1>oaix gateway</h1>")
		return
	}
	html := string(data)
	html = strings.ReplaceAll(html, "/assets/styles.css", "/assets/styles.css?v="+a.webAssetVersion("styles.css"))
	html = strings.ReplaceAll(html, "/assets/src/main.js", "/assets/src/main.js?v="+a.webAssetVersion(filepath.Join("src", "main.js")))
	hash, versionTime := a.webBundleVersion()
	html = strings.ReplaceAll(html, "__OAIX_WEB_VERSION_HASH__", hash)
	html = strings.ReplaceAll(html, "__OAIX_WEB_VERSION_TIME__", versionTime)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
	_, _ = io.WriteString(w, html)
}

func (a *App) webAssetVersion(rel string) string {
	data, err := os.ReadFile(filepath.Join(a.webDir, rel))
	if err != nil {
		return "missing"
	}
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:6])
}

func (a *App) webBundleVersion() (string, string) {
	digest := sha256.New()
	var newest time.Time
	_ = filepath.WalkDir(a.webDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".html" && ext != ".css" && ext != ".js" {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr == nil {
			_, _ = digest.Write(data)
		}
		if info, statErr := entry.Info(); statErr == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		return nil
	})
	hash := fmt.Sprintf("%x", digest.Sum(nil)[:6])
	if newest.IsZero() {
		return hash, "-"
	}
	return hash, newest.UTC().In(time.FixedZone("UTC+8", 8*60*60)).Format("2006-01-02 15:04:05 UTC+8")
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
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	settings, err := a.store.GetTokenSelectionSettings(ctx, a.cfg.TokenPool.ActiveStreamCap)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	activeCap := a.tokens.SetActiveStreamCap(settings.ActiveStreamCap)
	writeJSON(w, http.StatusOK, map[string]any{
		"strategy":          "snapshot_round_robin",
		"active_stream_cap": activeCap,
		"settings":          settings,
		"snapshot":          a.tokens.Stats(),
	})
}

func (a *App) updateTokenSelection(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ActiveStreamCap *int64 `json:"active_stream_cap"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if payload.ActiveStreamCap == nil {
		writeError(w, http.StatusBadRequest, errors.New("active_stream_cap is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	settings, err := a.store.UpdateTokenActiveStreamCapSettings(ctx, *payload.ActiveStreamCap, a.cfg.TokenPool.ActiveStreamCap)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	activeCap := a.tokens.SetActiveStreamCap(settings.ActiveStreamCap)
	_ = a.store.WriteAuditLog(ctx, "token_selection_concurrency_updated", "admin", "setting", store.TokenSelectionSettingKey, map[string]any{
		"active_stream_cap": activeCap,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"strategy":          "snapshot_round_robin",
		"active_stream_cap": activeCap,
		"settings":          settings,
		"snapshot":          a.tokens.Stats(),
	})
}

func (a *App) listTokens(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 100)
	offset := queryInt(r, "offset", 0)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tokenOpts := store.TokenListOptions{
		Limit:  limit,
		Offset: offset,
	}
	tokenOpts = tokenListOptionsFromRequest(r, tokenOpts)
	items, total, err := a.store.ListTokens(ctx, tokenOpts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	adminItems, pendingIDs := a.adminTokenItems(r.Context(), items, queryBool(r, "include_quota", false))
	counts, _ := a.store.TokenCounts(ctx)
	planCtx, planCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer planCancel()
	planCounts, _ := a.store.TokenPlanCounts(planCtx, tokenOpts)
	writeJSON(w, http.StatusOK, map[string]any{
		"counts":          counts,
		"filtered_counts": counts,
		"plan_counts":     planCounts,
		"pagination": map[string]any{
			"limit":        limit,
			"offset":       offset,
			"returned":     len(items),
			"total":        total,
			"has_previous": offset > 0,
			"has_next":     offset+len(items) < total,
		},
		"items":                     adminItems,
		"quota_refresh_pending_ids": pendingIDs,
	})
}

func (a *App) listTokenCosts(w http.ResponseWriter, r *http.Request) {
	ids, err := parseAdminTokenIDs(r.URL.Query().Get("ids"), 100)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"items": []store.TokenObservedCost{}})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tokens, err := a.store.ListTokensByIDs(ctx, ids)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	costs, err := a.store.TokenObservedCosts(ctx, tokens)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	items := make([]store.TokenObservedCost, 0, len(tokens))
	for _, token := range tokens {
		items = append(items, store.TokenObservedCost{
			ID:              token.ID,
			ObservedCostUSD: costs[token.ID],
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) importTokens(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var body any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	payloads, queuePosition, err := parseImportPayload(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	job, result, err := a.completeTokenImport(ctx, payloads, queuePosition, "admin_api", "api")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"job":    job,
		"result": result,
	})
}

func (a *App) completeTokenImport(ctx context.Context, payloads []map[string]any, queuePosition string, source string, auditActor string) (store.ImportJob, store.ImportResult, error) {
	ownerID, err := a.store.BootstrapUserID(ctx)
	if err != nil {
		return store.ImportJob{}, store.ImportResult{}, err
	}
	return a.completeTokenImportForOwner(ctx, ownerID, payloads, queuePosition, source, auditActor)
}

func (a *App) completeTokenImportForOwner(ctx context.Context, ownerUserID int64, payloads []map[string]any, queuePosition string, source string, auditActor string) (store.ImportJob, store.ImportResult, error) {
	originalPayloads := payloads
	upsertPayloads, preflightResult := a.prepareImportPayloads(ctx, originalPayloads)
	result, err := a.store.UpsertTokenPayloadsForOwner(ctx, ownerUserID, upsertPayloads, source)
	if err != nil {
		return store.ImportJob{}, store.ImportResult{}, err
	}
	result = mergeImportResults(preflightResult, result)
	job, err := a.store.CreateCompletedImportJobForOwner(ctx, ownerUserID, originalPayloads, queuePosition, result)
	if err != nil {
		return store.ImportJob{}, store.ImportResult{}, err
	}
	if a.tokens != nil {
		if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
			a.logger.Warn("token snapshot refresh after import failed", "error", err)
		}
	}
	if a.store != nil {
		_ = a.store.WriteAuditLog(ctx, "token_import", auditActor, "import_job", strconv.FormatInt(job.ID, 10), map[string]any{
			"created": result.Created,
			"updated": result.Updated,
			"skipped": result.Skipped,
			"failed":  result.Failed,
			"source":  source,
		})
	}
	return job, result, nil
}

func (a *App) prepareImportPayloads(ctx context.Context, payloads []map[string]any) ([]map[string]any, store.ImportResult) {
	result := store.ImportResult{}
	out := make([]map[string]any, 0, len(payloads))
	client := oauth.NewHTTPClient(a.cfg.Upstream.OAuthTokenURL)
	client.ClientID = a.cfg.Upstream.OAuthClientID
	client.Scope = a.cfg.Upstream.OAuthScope
	for index, payload := range payloads {
		normalized := clonePayload(payload)
		refreshToken := stringFromImportPayload(normalized, "refresh_token", "refreshToken")
		accessToken := stringFromImportPayload(normalized, "access_token", "accessToken")
		if accessToken == "" {
			if token := stringFromImportPayload(normalized, "token"); looksLikeAccessTokenText(token) {
				accessToken = token
			}
		}
		if refreshToken != "" && accessToken == "" {
			refreshed, err := client.Refresh(ctx, refreshToken)
			if err != nil {
				result.Failed++
				result.Items = append(result.Items, store.ImportResultItem{
					Index:        index,
					Action:       "failed",
					Status:       "failed",
					ErrorMessage: sanitizeImportError(err),
				})
				continue
			}
			normalized["_previous_refresh_token"] = refreshToken
			normalized["access_token"] = refreshed.AccessToken
			if refreshed.RefreshToken != "" {
				normalized["refresh_token"] = refreshed.RefreshToken
			}
			if refreshed.IDToken != "" {
				normalized["id_token"] = refreshed.IDToken
			}
			if refreshed.AccountID != "" {
				normalized["account_id"] = refreshed.AccountID
				normalized["chatgpt_account_id"] = refreshed.AccountID
			}
			if refreshed.Email != "" {
				normalized["email"] = refreshed.Email
			}
			if refreshed.PlanType != "" {
				normalized["plan_type"] = refreshed.PlanType
			}
			normalized["last_refresh"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		normalized["_import_index"] = index
		out = append(out, normalized)
	}
	return out, result
}

func mergeImportResults(left store.ImportResult, right store.ImportResult) store.ImportResult {
	if left.Failed == 0 && len(left.Items) == 0 {
		return right
	}
	right.Created += left.Created
	right.Updated += left.Updated
	right.Skipped += left.Skipped
	right.Failed += left.Failed
	if len(left.Tokens) > 0 {
		right.Tokens = append(left.Tokens, right.Tokens...)
	}
	if len(left.Items) > 0 {
		right.Items = append(left.Items, right.Items...)
	}
	return right
}

func clonePayload(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func stringFromImportPayload(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func sanitizeImportError(err error) string {
	value := strings.TrimSpace(err.Error())
	if value == "" {
		return "token refresh failed"
	}
	if len(value) > 500 {
		value = value[:500]
	}
	return value
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
	items, _ := a.adminTokenItems(r.Context(), []store.Token{*token}, queryBool(r, "include_quota", false))
	if len(items) > 0 {
		writeJSON(w, http.StatusOK, items[0])
		return
	}
	writeJSON(w, http.StatusOK, token)
}

func (a *App) listImportBatches(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListImportJobSummaries(ctx, queryInt(r, "limit", 30), true)
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
	job, err := a.store.GetImportJobSummary(ctx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("import job not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	importItems, err := a.store.ListImportJobItems(ctx, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	tokensByID, err := a.store.ListTokensByIDs(ctx, job.TokenIDs)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	adminItems, pendingIDs := a.adminTokenItems(r.Context(), orderTokensByID(tokensByID, job.TokenIDs), queryBool(r, "include_quota", false))
	writeJSON(w, http.StatusOK, map[string]any{
		"job":                       job,
		"items":                     sanitizeImportItems(importItems),
		"tokens":                    adminItems,
		"quota_refresh_pending_ids": pendingIDs,
	})
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

func (a *App) deleteImportJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("job_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid job id"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if queryBool(r, "dry_run", false) {
		result, err := a.store.DeleteImportJobDryRun(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("import job not found"))
			return
		}
		if err != nil {
			if strings.Contains(err.Error(), "cannot delete") {
				writeError(w, http.StatusConflict, err)
				return
			}
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"dry_run":        true,
			"job":            result.ImportJob,
			"token_ids":      result.TokenIDs,
			"deleted_tokens": result.DeletedTokens,
		})
		return
	}
	result, err := a.store.DeleteImportJobWithTokens(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("import job not found"))
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "cannot delete") {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_import_job_delete", "api", "import_job", strconv.FormatInt(id, 10), map[string]any{
		"job_id":         id,
		"token_ids":      result.TokenIDs,
		"deleted_tokens": result.DeletedTokens,
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"job":            result.ImportJob,
		"token_ids":      result.TokenIDs,
		"deleted_tokens": result.DeletedTokens,
	})
}

func orderTokensByID(tokens []store.Token, ids []int64) []store.Token {
	if len(tokens) == 0 || len(ids) == 0 {
		return tokens
	}
	byID := make(map[int64]store.Token, len(tokens))
	for _, token := range tokens {
		byID[token.ID] = token
	}
	ordered := make([]store.Token, 0, len(tokens))
	seen := make(map[int64]struct{}, len(tokens))
	for _, id := range ids {
		token, ok := byID[id]
		if !ok {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, token)
	}
	return ordered
}

func sanitizeImportItems(items []store.ImportItem) []adminImportItem {
	out := make([]adminImportItem, 0, len(items))
	for _, item := range items {
		out = append(out, adminImportItem{
			ID:                 item.ID,
			JobID:              item.JobID,
			ItemIndex:          item.ItemIndex,
			Status:             item.Status,
			TokenID:            item.TokenID,
			Action:             item.Action,
			ErrorMessage:       item.ErrorMessage,
			ValidationMS:       item.ValidationMS,
			PublishMS:          item.PublishMS,
			ValidationStarted:  item.ValidationStarted,
			ValidationFinished: item.ValidationFinished,
			PublishedAt:        item.PublishedAt,
			CreatedAt:          item.CreatedAt,
			UpdatedAt:          item.UpdatedAt,
		})
	}
	return out
}

func (a *App) batchTokens(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Action        string  `json:"action"`
		TokenIDs      []int64 `json:"token_ids"`
		Active        *bool   `json:"active"`
		ClearCooldown bool    `json:"clear_cooldown"`
		PlanType      string  `json:"plan_type"`
		Remark        string  `json:"remark"`
		Model         string  `json:"model"`
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
	var results []any
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
		case "clear_cooldown":
			if _, err := a.store.ClearTokenCooldown(ctx, id); err != nil {
				failed++
			} else {
				updated++
			}
		case "set_plan":
			plan := payload.PlanType
			if _, err := a.store.UpdateTokenMetadata(ctx, store.TokenMetadataUpdate{TokenID: id, PlanType: &plan}); err != nil {
				failed++
			} else {
				updated++
			}
		case "set_remark":
			remark := payload.Remark
			if _, err := a.store.UpdateTokenMetadata(ctx, store.TokenMetadataUpdate{TokenID: id, Remark: &remark}); err != nil {
				failed++
			} else {
				updated++
			}
		case "probe":
			token, err := a.store.GetToken(ctx, id)
			if err != nil {
				failed++
				continue
			}
			results = append(results, a.probeTokenWithAccess(ctx, *token, payload.Model))
			updated++
		case "quota_refresh":
			token, err := a.store.GetToken(ctx, id)
			if err != nil {
				failed++
				continue
			}
			if a.quota != nil {
				a.quota.clear([]int64{id})
			}
			items, _ := a.adminTokenItems(ctx, []store.Token{*token}, true)
			if len(items) > 0 {
				results = append(results, items[0])
				updated++
			} else {
				failed++
			}
		case "export":
			token, err := a.store.GetToken(ctx, id)
			if err != nil {
				failed++
				continue
			}
			results = append(results, token)
		default:
			writeError(w, http.StatusBadRequest, errors.New("action must be activate, deactivate, set_active, delete, probe, quota_refresh, clear_cooldown, set_plan, set_remark, or export"))
			return
		}
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_batch_"+action, "api", "token", "", payload)
	writeJSON(w, http.StatusOK, map[string]any{
		"updated": updated,
		"deleted": deleted,
		"failed":  failed,
		"results": results,
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
	opts := requestLogOptionsFromRequest(r)
	items, total, err := a.store.ListRequestLogsFiltered(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"summary": summary, "items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
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
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/responses", Stream: requestStream(r)})
}

func (a *App) getResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}", Method: http.MethodGet, UpstreamSuffix: responseID})
}

func (a *App) deleteResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}", Method: http.MethodDelete, UpstreamSuffix: responseID})
}

func (a *App) cancelResponse(w http.ResponseWriter, r *http.Request) {
	responseID := strings.TrimSpace(r.PathValue("response_id"))
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/{id}/cancel", Method: http.MethodPost, UpstreamSuffix: responseID + "/cancel"})
}

func (a *App) responsesCompact(w http.ResponseWriter, r *http.Request) {
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/responses/compact", Stream: requestStream(r), Compact: true})
}

func (a *App) chatCompletions(w http.ResponseWriter, r *http.Request) {
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/chat/completions", Stream: requestStream(r)})
}

func (a *App) imagesGenerations(w http.ResponseWriter, r *http.Request) {
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/images/generations", Stream: requestStream(r), Model: "gpt-image-2"})
}

func (a *App) imagesEdits(w http.ResponseWriter, r *http.Request) {
	a.proxyRequest(w, r, proxy.RequestIntent{Endpoint: "/v1/images/edits", Stream: requestStream(r), Model: "gpt-image-2"})
}

func (a *App) proxyRequest(w http.ResponseWriter, r *http.Request, intent proxy.RequestIntent) {
	auth := authFromContext(r.Context())
	if auth == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid or missing API key"})
		return
	}
	ownerID, err := auth.proxyOwner(r.Context(), a.store)
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": err.Error()})
		return
	}
	intent.OwnerUserID = ownerID
	intent.APIKeyID = auth.APIKeyID
	a.proxy.Proxy(w, r, intent)
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
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,Content-Encoding,X-API-Key,X-Request-ID,Idempotency-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func parseImportPayload(body any) ([]map[string]any, string, error) {
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
		return nil, queuePosition, errors.New("body must be a token object, an array, or {tokens: [...]}")
	}
	payloads := make([]map[string]any, 0, len(values))
	for _, value := range values {
		switch item := value.(type) {
		case string:
			text := strings.TrimSpace(item)
			if text != "" {
				payloads = append(payloads, importPayloadFromString(text))
			}
		case map[string]any:
			payloads = append(payloads, normalizeImportPayloadMap(item))
		default:
			return nil, queuePosition, fmt.Errorf("unsupported token payload %T", value)
		}
	}
	return payloads, queuePosition, nil
}

func importPayloadFromString(value string) map[string]any {
	value = strings.TrimSpace(value)
	if looksLikeAccessTokenText(value) {
		return map[string]any{"access_token": value}
	}
	return map[string]any{"refresh_token": value}
}

func normalizeImportPayloadMap(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		out[key] = value
	}
	if _, hasAccess := out["access_token"]; !hasAccess {
		if value, ok := out["accessToken"].(string); ok && strings.TrimSpace(value) != "" {
			out["access_token"] = strings.TrimSpace(value)
		}
	}
	if _, hasRefresh := out["refresh_token"]; !hasRefresh {
		if value, ok := out["refreshToken"].(string); ok && strings.TrimSpace(value) != "" {
			out["refresh_token"] = strings.TrimSpace(value)
		}
	}
	if _, hasAccess := out["access_token"]; !hasAccess {
		if _, hasRefresh := out["refresh_token"]; !hasRefresh {
			if value, ok := out["token"].(string); ok && strings.TrimSpace(value) != "" {
				text := strings.TrimSpace(value)
				if looksLikeAccessTokenText(text) {
					out["access_token"] = text
				} else {
					out["refresh_token"] = text
				}
			}
		}
	}
	return out
}

func looksLikeAccessTokenText(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "eyJ")
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

func queryBool(r *http.Request, key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
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
	message := "request failed"
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		message = err.Error()
	}
	writeJSON(w, status, map[string]any{
		"detail": message,
		"error": map[string]any{
			"code":      httpStatusErrorCode(status),
			"message":   message,
			"retryable": status == http.StatusTooManyRequests || status >= 500,
		},
		"request_id": "",
	})
}

func httpStatusErrorCode(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "not_found"
	case http.StatusConflict:
		return "conflict"
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusServiceUnavailable:
		return "service_unavailable"
	case http.StatusBadGateway:
		return "bad_gateway"
	default:
		if status >= 500 {
			return "internal_error"
		}
		return "request_error"
	}
}
