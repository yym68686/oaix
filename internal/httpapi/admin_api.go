package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/store"
)

type quotaRefreshRegistry struct {
	mu   sync.Mutex
	jobs map[string]*quotaRefreshJob
}

type quotaRefreshJob struct {
	ID         string           `json:"id"`
	Status     string           `json:"status"`
	Total      int              `json:"total"`
	Processed  int              `json:"processed"`
	Failed     int              `json:"failed"`
	PendingIDs []int64          `json:"pending_ids,omitempty"`
	Results    []adminTokenItem `json:"results,omitempty"`
	Error      string           `json:"error,omitempty"`
	CreatedAt  time.Time        `json:"created_at"`
	FinishedAt *time.Time       `json:"finished_at,omitempty"`
}

func newQuotaRefreshRegistry() *quotaRefreshRegistry {
	return &quotaRefreshRegistry{jobs: map[string]*quotaRefreshJob{}}
}

func (r *quotaRefreshRegistry) put(job *quotaRefreshJob) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[job.ID] = job
	if len(r.jobs) > 100 {
		cutoff := time.Now().UTC().Add(-2 * time.Hour)
		for id, item := range r.jobs {
			if item.CreatedAt.Before(cutoff) {
				delete(r.jobs, id)
			}
		}
	}
}

func (r *quotaRefreshRegistry) get(id string) (*quotaRefreshJob, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok {
		return nil, false
	}
	copy := *job
	copy.PendingIDs = append([]int64(nil), job.PendingIDs...)
	copy.Results = append([]adminTokenItem(nil), job.Results...)
	return &copy, true
}

func (r *quotaRefreshRegistry) update(id string, fn func(job *quotaRefreshJob)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if job := r.jobs[id]; job != nil {
		fn(job)
	}
}

func (a *App) registerAdminAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/openapi.json", a.requireAuth(a.openapiSpec))
	mux.HandleFunc("GET /admin/options", a.requireAuth(a.adminOptions))

	mux.HandleFunc("PATCH /admin/tokens/{token_id}", a.requireAuth(a.patchToken))
	mux.HandleFunc("GET /admin/tokens/export", a.requireAuth(a.exportTokens))
	mux.HandleFunc("POST /admin/tokens/quota-refresh", a.requireAuth(a.createQuotaRefreshJob))
	mux.HandleFunc("GET /admin/tokens/quota-refresh/{job_id}", a.requireAuth(a.getQuotaRefreshJob))
	mux.HandleFunc("GET /admin/token-quota-history/{token_id}", a.requireAuth(a.listTokenQuotaHistory))
	mux.HandleFunc("POST /admin/tokens/{token_id}/cooldown", a.requireAuth(a.setTokenCooldown))
	mux.HandleFunc("DELETE /admin/token-cooldown/{token_id}", a.requireAuth(a.clearTokenCooldown))
	mux.HandleFunc("DELETE /admin/token-last-error/{token_id}", a.requireAuth(a.clearTokenLastError))
	mux.HandleFunc("POST /admin/tokens/{token_id}/secrets", a.requireAuth(a.updateTokenSecrets))
	mux.HandleFunc("POST /admin/tokens/{token_id}/refresh", a.requireAuth(a.refreshTokenSecret))
	mux.HandleFunc("GET /admin/token-refresh-history/{token_id}", a.requireAuth(a.listTokenRefreshHistory))
	mux.HandleFunc("POST /admin/tokens/{token_id}/merge", a.requireAuth(a.mergeToken))
	mux.HandleFunc("POST /admin/tokens/{token_id}/unmerge", a.requireAuth(a.unmergeToken))
	mux.HandleFunc("DELETE /admin/token-response-owners/{token_id}", a.requireAuth(a.deleteTokenResponseOwners))

	mux.HandleFunc("GET /admin/costs/by-token", a.requireAuth(a.costsByToken))
	mux.HandleFunc("GET /admin/costs/by-account", a.requireAuth(a.costsByAccount))
	mux.HandleFunc("POST /admin/request-costs/reconcile", a.requireAuth(a.reconcileRequestCosts))
	mux.HandleFunc("GET /admin/request-costs/reconcile", a.requireAuth(a.reconcileStatus))

	mux.HandleFunc("POST /admin/import/parse", a.requireAuth(a.parseImport))
	mux.HandleFunc("POST /admin/import/upload", a.requireAuth(a.uploadImport))
	mux.HandleFunc("POST /admin/import/jobs", a.requireAuth(a.createImportJob))
	mux.HandleFunc("GET /admin/import/jobs", a.requireAuth(a.listImportJobs))
	mux.HandleFunc("GET /admin/import/jobs/{job_id}", a.requireAuth(a.getImportJobV2))
	mux.HandleFunc("GET /admin/import/jobs/{job_id}/items", a.requireAuth(a.listImportJobItems))
	mux.HandleFunc("GET /admin/import/jobs/{job_id}/tokens", a.requireAuth(a.listImportJobTokens))
	mux.HandleFunc("GET /admin/import/jobs/{job_id}/failed-items", a.requireAuth(a.listImportJobFailedItems))
	mux.HandleFunc("POST /admin/import/jobs/{job_id}/retry", a.requireAuth(a.retryImportJob))
	mux.HandleFunc("POST /admin/import/items/{item_id}/retry", a.requireAuth(a.retryImportItem))
	mux.HandleFunc("POST /admin/import/items/{item_id}/skip", a.requireAuth(a.skipImportItem))
	mux.HandleFunc("POST /admin/import/jobs/{job_id}/cancel", a.requireAuth(a.cancelImportJob))
	mux.HandleFunc("DELETE /admin/import/jobs/{job_id}", a.requireAuth(a.deleteImportJob))
	mux.HandleFunc("GET /admin/import/jobs/{job_id}/events", a.requireAuth(a.importJobEvents))

	mux.HandleFunc("POST /admin/oauth/openai/sessions", a.requireAuth(a.startOpenAIOAuth))
	mux.HandleFunc("GET /admin/oauth/openai/sessions/{session_id}", a.requireAuth(a.getOpenAIOAuthSession))
	mux.HandleFunc("DELETE /admin/oauth/openai/sessions/{session_id}", a.requireAuth(a.cancelOpenAIOAuthSession))
	mux.HandleFunc("POST /admin/oauth/openai/sessions/{session_id}/exchange", a.requireAuth(a.exchangeOpenAIOAuthSession))

	mux.HandleFunc("GET /admin/requests/{request_id}", a.requireAuth(a.getRequestLog))
	mux.HandleFunc("GET /admin/requests/export", a.requireAuth(a.exportRequests))
	mux.HandleFunc("GET /admin/analytics/models", a.requireAuth(a.analytics))
	mux.HandleFunc("GET /admin/analytics/cache", a.requireAuth(a.cacheAnalytics))
	mux.HandleFunc("GET /admin/analytics/errors", a.requireAuth(a.errorAnalytics))
	mux.HandleFunc("GET /admin/analytics/latency", a.requireAuth(a.latencyAnalytics))
	mux.HandleFunc("GET /admin/inflight", a.requireAuth(a.inflight))

	mux.HandleFunc("GET /admin/workers", a.requireAuth(a.workers))
	mux.HandleFunc("GET /admin/request-log-outbox", a.requireAuth(a.requestLogOutbox))
	mux.HandleFunc("POST /admin/request-log-outbox/drain", a.requireAuth(a.drainRequestLogOutbox))
	mux.HandleFunc("POST /admin/maintenance/hourly-stats", a.requireAuth(a.aggregateHourlyStats))
	mux.HandleFunc("POST /admin/maintenance/cleanup-request-logs", a.requireAuth(a.cleanupRequestLogs))
	mux.HandleFunc("POST /admin/token-pool/refresh", a.requireAuth(a.refreshTokenPool))
	mux.HandleFunc("GET /admin/token-pool/snapshot", a.requireAuth(a.tokenPoolSnapshot))
	mux.HandleFunc("POST /admin/transport/reset-idle", a.requireAuth(a.resetIdleTransport))

	mux.HandleFunc("GET /admin/prompt-cache/lanes", a.requireAuth(a.listPromptCacheLanes))
	mux.HandleFunc("GET /admin/prompt-cache/lanes/{prompt_key_hash}", a.requireAuth(a.getPromptCacheLane))
	mux.HandleFunc("DELETE /admin/prompt-cache/lanes/{prompt_key_hash}", a.requireAuth(a.deletePromptCacheLane))
	mux.HandleFunc("GET /admin/response-owners", a.requireAuth(a.listResponseOwners))
	mux.HandleFunc("DELETE /admin/response-owners/{response_id_hash}", a.requireAuth(a.deleteResponseOwner))
	mux.HandleFunc("GET /admin/prompt-cache/stats", a.requireAuth(a.promptCacheStats))

	mux.HandleFunc("GET /admin/settings/schema", a.requireAuth(a.settingsSchema))
	mux.HandleFunc("GET /admin/settings/{key}", a.requireAuth(a.getSetting))
	mux.HandleFunc("DELETE /admin/settings/{key}", a.requireAuth(a.deleteSetting))
	mux.HandleFunc("POST /admin/settings/{key}/validate", a.requireAuth(a.validateSetting))
	mux.HandleFunc("GET /admin/audit-logs", a.requireAuth(a.listAuditLogs))
	mux.HandleFunc("GET /admin/audit-logs/{audit_id}", a.requireAuth(a.getAuditLog))
	mux.HandleFunc("GET /admin/api-keys", a.requireAuth(a.listAPIKeys))
	mux.HandleFunc("POST /admin/api-keys", a.requireAuth(a.createAPIKey))
	mux.HandleFunc("POST /admin/api-keys/{key_id}/rotate", a.requireAuth(a.rotateAPIKey))
	mux.HandleFunc("DELETE /admin/api-keys/{key_id}", a.requireAuth(a.revokeAPIKey))
}

func tokenListOptionsFromRequest(r *http.Request, opts store.TokenListOptions) store.TokenListOptions {
	query := r.URL.Query()
	opts.Query = query.Get("q")
	opts.Status = query.Get("status")
	opts.Plan = firstNonEmpty(query.Get("plan"), query.Get("plan_type"))
	opts.Sort = query.Get("sort")
	opts.Cursor = query.Get("cursor")
	opts.ImportJobID = queryInt64(r, "import_job_id", 0)
	if opts.ImportJobID == 0 {
		opts.ImportJobID = queryInt64(r, "import_batch_id", 0)
	}
	opts.Source = query.Get("source")
	opts.SourceFile = query.Get("source_file")
	opts.CreatedFrom = queryTime(r, "created_from")
	opts.CreatedTo = queryTime(r, "created_to")
	opts.LastUsedFrom = queryTime(r, "last_used_from")
	opts.LastUsedTo = queryTime(r, "last_used_to")
	if has := queryTriBool(r, "has_error"); has != nil {
		opts.HasError = has
	}
	opts.ErrorCode = query.Get("error_code")
	opts.CooldownReason = query.Get("cooldown_reason")
	opts.QuotaState = query.Get("quota_state")
	return opts
}

func (a *App) openapiSpec(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, adminOpenAPISpec())
}

func adminOpenAPISpec() map[string]any {
	routes := []string{
		"GET /admin/openapi.json", "GET /admin/options",
		"GET /admin/tokens", "GET /admin/tokens/{token_id}/refresh-token", "PATCH /admin/tokens/{token_id}", "GET /admin/tokens/export",
		"POST /admin/tokens/{token_id}/cooldown", "DELETE /admin/token-cooldown/{token_id}",
		"DELETE /admin/token-last-error/{token_id}", "POST /admin/tokens/{token_id}/secrets",
		"POST /admin/tokens/{token_id}/refresh", "GET /admin/token-refresh-history/{token_id}",
		"POST /admin/tokens/{token_id}/merge", "POST /admin/tokens/{token_id}/unmerge",
		"POST /admin/tokens/quota-refresh", "GET /admin/tokens/quota-refresh/{job_id}",
		"GET /admin/token-quota-history/{token_id}", "GET /admin/costs/by-token", "GET /admin/costs/by-account",
		"POST /admin/import/parse", "POST /admin/import/upload", "POST /admin/import/jobs",
		"GET /admin/import/jobs", "GET /admin/import/jobs/{job_id}", "GET /admin/import/jobs/{job_id}/items",
		"GET /admin/import/jobs/{job_id}/tokens", "GET /admin/import/jobs/{job_id}/failed-items",
		"POST /admin/import/jobs/{job_id}/retry", "POST /admin/import/items/{item_id}/retry",
		"POST /admin/import/items/{item_id}/skip", "GET /admin/import/jobs/{job_id}/events",
		"POST /admin/oauth/openai/sessions", "GET /admin/oauth/openai/sessions/{session_id}",
		"DELETE /admin/oauth/openai/sessions/{session_id}", "POST /admin/oauth/openai/sessions/{session_id}/exchange",
		"GET /admin/requests", "GET /admin/requests/{request_id}", "GET /admin/requests/export",
		"GET /admin/analytics/models", "GET /admin/analytics/cache", "GET /admin/analytics/errors",
		"GET /admin/analytics/latency", "GET /admin/inflight", "GET /admin/runtime", "GET /admin/workers",
		"GET /admin/request-log-outbox", "POST /admin/request-log-outbox/drain",
		"POST /admin/maintenance/hourly-stats", "POST /admin/maintenance/cleanup-request-logs",
		"POST /admin/token-pool/refresh", "GET /admin/token-pool/snapshot", "POST /admin/transport/reset-idle",
		"GET /admin/prompt-cache/lanes", "GET /admin/prompt-cache/lanes/{prompt_key_hash}",
		"DELETE /admin/prompt-cache/lanes/{prompt_key_hash}", "GET /admin/response-owners",
		"DELETE /admin/response-owners/{response_id_hash}", "DELETE /admin/token-response-owners/{token_id}",
		"GET /admin/prompt-cache/stats", "GET /admin/settings", "GET /admin/settings/{key}",
		"POST /admin/settings/{key}", "DELETE /admin/settings/{key}", "POST /admin/settings/{key}/validate",
		"GET /admin/settings/schema", "GET /admin/audit-logs", "GET /admin/audit-logs/{audit_id}",
		"GET /admin/api-keys", "POST /admin/api-keys", "POST /admin/api-keys/{key_id}/rotate",
		"DELETE /admin/api-keys/{key_id}",
		"POST /api/auth/register", "POST /api/auth/login", "GET /api/me", "GET /api/me/api-keys",
		"POST /api/me/api-keys", "DELETE /api/me/api-keys/{key_id}", "GET /api/me/usage",
		"GET /api/me/pool-summary", "GET /api/me/settings", "GET /api/me/settings/{key}",
		"POST /api/me/settings/{key}", "DELETE /api/me/settings/{key}",
		"GET /api/tokens", "GET /api/tokens/{token_id}", "GET /api/tokens/{token_id}/refresh-token", "PATCH /api/tokens/{token_id}",
		"DELETE /api/tokens/{token_id}", "POST /api/tokens/{token_id}/probe",
		"POST /api/import/parse", "POST /api/import/upload", "POST /api/import/jobs",
		"GET /api/import/jobs", "GET /api/import/jobs/{job_id}", "POST /api/import/jobs/{job_id}/cancel",
		"DELETE /api/import/jobs/{job_id}", "GET /api/import/jobs/{job_id}/items",
		"GET /api/import/jobs/{job_id}/tokens", "GET /api/requests",
		"GET /api/admin/users", "POST /api/admin/users", "GET /api/admin/users/{user_id}",
		"PATCH /api/admin/users/{user_id}", "GET /api/admin/users/{user_id}/api-keys",
		"POST /api/admin/users/{user_id}/api-keys", "DELETE /api/admin/users/{user_id}/api-keys/{key_id}",
		"GET /api/admin/users/{user_id}/tokens", "GET /api/admin/users/{user_id}/import/jobs",
		"GET /api/admin/users/{user_id}/requests", "GET /api/admin/users/{user_id}/usage",
		"GET /api/admin/pool-summary", "GET /api/admin/pool-summary/by-user",
		"GET /api/admin/analytics/users", "GET /api/admin/requests", "GET /api/admin/requests/export",
		"GET /api/admin/audit-logs", "GET /api/admin/audit-logs/{audit_id}",
		"GET /api/admin/sub2api/targets", "POST /api/admin/sub2api/targets",
		"GET /api/admin/sub2api/targets/{target_id}", "PATCH /api/admin/sub2api/targets/{target_id}",
		"DELETE /api/admin/sub2api/targets/{target_id}", "GET /api/admin/sub2api/targets/{target_id}/groups",
		"POST /api/admin/sub2api/targets/{target_id}/check", "POST /api/admin/sub2api/targets/{target_id}/sync",
		"GET /api/admin/sub2api/targets/{target_id}/runs", "POST /api/admin/sub2api/probe-groups",
		"GET /api/admin/sub2api/runs",
	}
	paths := map[string]any{}
	for _, route := range routes {
		method, path, _ := strings.Cut(route, " ")
		paths[path] = mergeOpenAPIPath(paths[path], strings.ToLower(method))
	}
	return map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":   "oaix Admin API",
			"version": "2026-06-19",
		},
		"paths": paths,
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"serviceApiKey": map[string]any{"type": "http", "scheme": "bearer"},
			},
		},
		"x-oaix-error-envelope": map[string]any{
			"detail":     "legacy-compatible message",
			"error":      map[string]any{"code": "string", "message": "string", "retryable": "boolean"},
			"request_id": "string",
		},
	}
}

func mergeOpenAPIPath(current any, method string) map[string]any {
	path, _ := current.(map[string]any)
	if path == nil {
		path = map[string]any{}
	}
	path[method] = map[string]any{
		"responses": map[string]any{
			"200": map[string]any{"description": "OK"},
			"4XX": map[string]any{"description": "Client error"},
			"5XX": map[string]any{"description": "Server error"},
		},
	}
	return path
}

func (a *App) adminOptions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"token_statuses": []map[string]string{
			{"value": "all", "label": "全部"},
			{"value": "available", "label": "有效"},
			{"value": "cooling", "label": "冷却"},
			{"value": "disabled", "label": "禁用"},
		},
		"plans": []map[string]string{
			{"value": "all", "label": "全部计划"},
			{"value": "free", "label": "Free"},
			{"value": "plus", "label": "Plus"},
			{"value": "team", "label": "Team"},
			{"value": "pro", "label": "Pro"},
			{"value": "unknown", "label": "Unknown"},
		},
		"sorts": []map[string]string{
			{"value": "-created_at", "label": "最新入库"},
			{"value": "created_at", "label": "最早入库"},
			{"value": "-last_used_at", "label": "最近使用"},
			{"value": "status", "label": "状态优先"},
		},
		"import_item_statuses": []string{"queued", "validating", "validated", "published", "failed", "skipped", "canceled"},
	})
}

func (a *App) patchToken(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		Remark              *string `json:"remark"`
		PlanType            *string `json:"plan_type"`
		Email               *string `json:"email"`
		AccountID           *string `json:"account_id"`
		Source              *string `json:"source"`
		SourceFile          *string `json:"source_file"`
		IsActive            *bool   `json:"is_active"`
		ShareEnabled        *bool   `json:"share_enabled"`
		ShareEnabledCamel   *bool   `json:"shareEnabled"`
		ShareStatus         *string `json:"share_status"`
		ShareStatusCamel    *string `json:"shareStatus"`
		ShareDisabledReason *string `json:"share_disabled_reason"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.UpdateTokenMetadata(ctx, store.TokenMetadataUpdate{
		TokenID: id, Remark: payload.Remark, PlanType: payload.PlanType, Email: payload.Email,
		AccountID: payload.AccountID, Source: payload.Source, SourceFile: payload.SourceFile, IsActive: payload.IsActive,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	shareEnabled := payload.ShareEnabled
	if shareEnabled == nil {
		shareEnabled = payload.ShareEnabledCamel
	}
	if shareEnabled != nil {
		status := stringPtr(payload.ShareStatus)
		if status == "" {
			status = stringPtr(payload.ShareStatusCamel)
		}
		token, err = a.store.SetTokenSharingScoped(ctx, store.AllResources(), id, *shareEnabled, status, stringPtr(payload.ShareDisabledReason))
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("token not found"))
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_metadata_update", "api", "token", strconv.FormatInt(id, 10), payload)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) setTokenCooldown(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		CooldownUntil *time.Time `json:"cooldown_until"`
		Seconds       int        `json:"seconds"`
		Reason        string     `json:"reason"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	until := time.Time{}
	if payload.CooldownUntil != nil {
		until = payload.CooldownUntil.UTC()
	} else if payload.Seconds > 0 {
		until = time.Now().UTC().Add(time.Duration(payload.Seconds) * time.Second)
	}
	if until.IsZero() {
		writeError(w, http.StatusBadRequest, errors.New("cooldown_until or seconds is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.SetTokenCooldown(ctx, id, until, payload.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_cooldown_set", "api", "token", strconv.FormatInt(id, 10), payload)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) clearTokenCooldown(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.ClearTokenCooldown(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_cooldown_clear", "api", "token", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) clearTokenLastError(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.ClearTokenLastError(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_last_error_clear", "api", "token", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) updateTokenSecrets(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		AccessToken        string `json:"access_token"`
		RefreshToken       string `json:"refresh_token"`
		IDToken            string `json:"id_token"`
		AccountID          string `json:"account_id"`
		Email              string `json:"email"`
		PlanType           string `json:"plan_type"`
		PreserveActivation bool   `json:"preserve_activation"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(payload.AccessToken) == "" && strings.TrimSpace(payload.RefreshToken) == "" && strings.TrimSpace(payload.IDToken) == "" {
		writeError(w, http.StatusBadRequest, errors.New("at least one secret is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.UpdateTokenSecret(ctx, store.TokenSecretUpdate{
		TokenID: id, AccessToken: payload.AccessToken, RefreshToken: payload.RefreshToken,
		IDToken: payload.IDToken, AccountID: payload.AccountID, Email: payload.Email,
		PlanType: payload.PlanType, PreserveActivation: payload.PreserveActivation,
	}); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_secret_update", "api", "token", strconv.FormatInt(id, 10), map[string]any{
		"access_token":  strings.TrimSpace(payload.AccessToken) != "",
		"refresh_token": strings.TrimSpace(payload.RefreshToken) != "",
		"id_token":      strings.TrimSpace(payload.IDToken) != "",
	})
	token, _ := a.store.GetToken(ctx, id)
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "updated": true})
}

func (a *App) refreshTokenSecret(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
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
	refreshed, result := a.refreshProbeAccessToken(ctx, *token, defaultAdminProbeModel)
	if result != nil && result["outcome"] != "reactivated" {
		writeJSON(w, http.StatusOK, result)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_refresh", "api", "token", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"token": refreshed, "refreshed": true})
}

func (a *App) listTokenRefreshHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListTokenRefreshHistory(ctx, id, queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) mergeToken(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		IntoTokenID int64 `json:"into_token_id"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.MergeToken(ctx, id, payload.IntoTokenID); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_merge", "api", "token", strconv.FormatInt(id, 10), payload)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "merged_into_token_id": payload.IntoTokenID})
}

func (a *App) unmergeToken(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.UnmergeToken(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "token_unmerge", "api", "token", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, token)
}

func (a *App) exportTokens(w http.ResponseWriter, r *http.Request) {
	format := strings.ToLower(firstNonEmpty(r.URL.Query().Get("format"), "csv"))
	opts := tokenListOptionsFromRequest(r, store.TokenListOptions{Limit: queryInt(r, "limit", 500), Offset: queryInt(r, "offset", 0)})
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListTokens(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if format == "json" {
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="oaix-tokens.csv"`)
	writer := csv.NewWriter(w)
	_ = writer.Write([]string{"id", "email", "account_id", "plan_type", "remark", "source_file", "is_active", "cooldown_until", "disabled_at", "last_used_at", "last_error", "created_at"})
	for _, token := range items {
		_ = writer.Write([]string{
			strconv.FormatInt(token.ID, 10), stringPtr(token.Email), stringPtr(token.AccountID), stringPtr(token.PlanType),
			stringPtr(token.Remark), stringPtr(token.SourceFile), strconv.FormatBool(token.IsActive),
			timePtr(token.CooldownUntil), timePtr(token.DisabledAt), timePtr(token.LastUsedAt), stringPtr(token.LastError), token.CreatedAt.Format(time.RFC3339),
		})
	}
	writer.Flush()
}

func (a *App) createQuotaRefreshJob(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		TokenIDs []int64 `json:"token_ids"`
		All      bool    `json:"all"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var tokens []store.Token
	var err error
	if payload.All || len(payload.TokenIDs) == 0 {
		tokens, _, err = a.store.ListTokens(ctx, store.TokenListOptions{Limit: 500, Status: r.URL.Query().Get("status"), Plan: r.URL.Query().Get("plan")})
	} else {
		tokens, err = a.store.ListTokensByIDs(ctx, payload.TokenIDs)
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	ids := make([]int64, 0, len(tokens))
	for _, token := range tokens {
		ids = append(ids, token.ID)
	}
	if a.quota != nil {
		a.quota.clear(ids)
	}
	jobID := randomID("quota")
	job := &quotaRefreshJob{ID: jobID, Status: "running", Total: len(tokens), CreatedAt: time.Now().UTC()}
	a.quotaJobs.put(job)
	go a.runQuotaRefreshJob(jobID, tokens)
	_ = a.store.WriteAuditLog(ctx, "quota_refresh_job_create", "api", "quota_refresh_job", jobID, map[string]any{"token_count": len(ids), "all": payload.All})
	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func (a *App) runQuotaRefreshJob(jobID string, tokens []store.Token) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(max(1, len(tokens)))*defaultQuotaHTTPTimeout+30*time.Second)
	defer cancel()
	items, pending := a.adminTokenItems(ctx, tokens, true)
	now := time.Now().UTC()
	a.quotaJobs.update(jobID, func(job *quotaRefreshJob) {
		job.Status = "completed"
		job.Processed = len(items)
		job.Failed = len(tokens) - len(items)
		job.PendingIDs = pending
		job.Results = items
		job.FinishedAt = &now
	})
}

func (a *App) getQuotaRefreshJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	job, ok := a.quotaJobs.get(id)
	if !ok {
		writeError(w, http.StatusNotFound, errors.New("quota refresh job not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (a *App) listTokenQuotaHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListQuotaSnapshots(ctx, id, queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) costsByToken(w http.ResponseWriter, r *http.Request) {
	opts := requestLogOptionsFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.CostAggregatesByToken(ctx, opts, opts.Limit, opts.Offset)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) costsByAccount(w http.ResponseWriter, r *http.Request) {
	opts := requestLogOptionsFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.CostAggregatesByAccount(ctx, opts, opts.Limit, opts.Offset)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) reconcileRequestCosts(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	changed, err := a.store.ReconcileRecordedTokenCosts(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "request_costs_reconcile", "api", "request_costs", "", map[string]any{"changed": changed})
	writeJSON(w, http.StatusOK, map[string]any{"completed": true, "changed": changed})
}

func (a *App) reconcileStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	completed, err := a.store.RequestTokenCostsReconciled(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"completed": completed})
}

func (a *App) parseImport(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		Text   string `json:"text"`
		Tokens any    `json:"tokens"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8*1024*1024)).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var payloads []map[string]any
	var err error
	if strings.TrimSpace(payload.Text) != "" {
		payloads, err = parseImportTextPayloads(payload.Text)
	} else if payload.Tokens != nil {
		payloads, _, err = parseImportPayload(map[string]any{"tokens": payload.Tokens})
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	payloads = dedupeImportPayloads(payloads)
	writeJSON(w, http.StatusOK, map[string]any{
		"items":   payloads,
		"summary": map[string]any{"total": len(payloads), "deduplicated": true},
	})
}

func (a *App) uploadImport(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var all []map[string]any
	for _, headers := range r.MultipartForm.File {
		for _, header := range headers {
			file, err := header.Open()
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			data, err := io.ReadAll(io.LimitReader(file, 8*1024*1024))
			_ = file.Close()
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			payloads, err := parseImportTextPayloads(string(data))
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			for _, payload := range payloads {
				payload["source_file"] = header.Filename
				all = append(all, payload)
			}
		}
	}
	all = dedupeImportPayloads(all)
	writeJSON(w, http.StatusOK, map[string]any{"items": all, "summary": map[string]any{"total": len(all)}})
}

func (a *App) createImportJob(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var body any
	if err := json.NewDecoder(io.LimitReader(r.Body, 16*1024*1024)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	requestHash := jsonHash(body)
	if idempotencyKey != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		record, err := a.store.GetIdempotencyRecord(ctx, idempotencyKey, http.MethodPost, "/admin/import/jobs")
		cancel()
		if err == nil {
			if record.RequestHash != requestHash {
				writeError(w, http.StatusConflict, errors.New("Idempotency-Key was already used with a different request body"))
				return
			}
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, record.StatusCode, json.RawMessage(record.Response))
			return
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
	}
	payloads, queuePosition, err := parseImportPayloadExtended(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	payloads = dedupeImportPayloads(payloads)
	if len(payloads) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("tokens must contain at least one import item"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	job, err := a.store.CreateQueuedImportJob(ctx, payloads, queuePosition)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_import_job_create", "api", "import_job", strconv.FormatInt(job.ID, 10), map[string]any{
		"total":           len(payloads),
		"idempotency_key": r.Header.Get("Idempotency-Key"),
	})
	response := map[string]any{"job": job}
	if idempotencyKey != "" {
		_ = a.store.SaveIdempotencyRecord(ctx, idempotencyKey, http.MethodPost, "/admin/import/jobs", requestHash, http.StatusAccepted, response, 24*time.Hour)
	}
	writeJSON(w, http.StatusAccepted, response)
}

func (a *App) listImportJobs(w http.ResponseWriter, r *http.Request) {
	opts := store.ImportJobListOptions{
		Limit:  queryInt(r, "limit", 50),
		Offset: queryInt(r, "offset", 0),
		Status: r.URL.Query().Get("status"),
		Source: r.URL.Query().Get("source"),
		From:   queryTime(r, "from"),
		To:     queryTime(r, "to"),
		Sort:   r.URL.Query().Get("sort"),
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	jobs, total, err := a.store.ListImportJobsFiltered(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	summaries, err := importSummariesForJobs(ctx, a.store, jobs, true)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": summaries, "pagination": pagination(opts.Limit, opts.Offset, len(summaries), total)})
}

func importSummariesForJobs(ctx context.Context, st *store.Store, jobs []store.ImportJob, includeObservedCost bool) ([]store.ImportBatchSummary, error) {
	summaries := make([]store.ImportBatchSummary, 0, len(jobs))
	for _, job := range jobs {
		summary, err := st.GetImportJobSummary(ctx, job.ID, includeObservedCost)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, *summary)
	}
	return summaries, nil
}

func (a *App) getImportJobV2(w http.ResponseWriter, r *http.Request) {
	a.getImportJob(w, r)
}

func (a *App) listImportJobItems(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	opts := store.ImportItemListOptions{
		Limit:  queryInt(r, "limit", 100),
		Offset: queryInt(r, "offset", 0),
		Status: r.URL.Query().Get("status"),
		Action: r.URL.Query().Get("action"),
		Error:  r.URL.Query().Get("error"),
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListImportJobItemsFiltered(ctx, id, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": sanitizeImportItems(items), "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) listImportJobTokens(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	opts := tokenListOptionsFromRequest(r, store.TokenListOptions{
		Limit:       queryInt(r, "limit", 100),
		Offset:      queryInt(r, "offset", 0),
		ImportJobID: id,
	})
	opts.ImportJobID = id
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tokens, total, err := a.store.ListTokens(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	adminItems, pendingIDs := a.adminTokenItems(r.Context(), tokens, queryBool(r, "include_quota", false))
	counts, _ := a.store.TokenCounts(ctx)
	planCounts, _ := a.store.TokenPlanCounts(ctx, opts)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": adminItems, "pagination": pagination(opts.Limit, opts.Offset, len(adminItems), total),
		"counts": counts, "plan_counts": planCounts, "quota_refresh_pending_ids": pendingIDs,
	})
}

func (a *App) listImportJobFailedItems(w http.ResponseWriter, r *http.Request) {
	r.URL.RawQuery = appendQuery(r.URL.RawQuery, "status", "failed")
	a.listImportJobItems(w, r)
}

func (a *App) retryImportJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	job, err := a.store.RetryImportJob(ctx, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_import_job_retry", "api", "import_job", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (a *App) retryImportItem(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "item_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.RetryImportItem(ctx, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_import_item_retry", "api", "import_item", strconv.FormatInt(id, 10), map[string]any{"job_id": item.JobID})
	writeJSON(w, http.StatusOK, map[string]any{"item": sanitizeImportItems([]store.ImportItem{item})[0]})
}

func (a *App) skipImportItem(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "item_id")
	if !ok {
		return
	}
	var payload struct {
		Reason string `json:"reason"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.SkipImportItem(ctx, id, payload.Reason)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_import_item_skip", "api", "import_item", strconv.FormatInt(id, 10), map[string]any{"job_id": item.JobID, "reason": payload.Reason})
	writeJSON(w, http.StatusOK, map[string]any{"item": sanitizeImportItems([]store.ImportItem{item})[0]})
}

func (a *App) importJobEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	job, err := a.store.GetImportJobSummary(ctx, id, false)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	data, _ := json.Marshal(map[string]any{"job": job})
	_, _ = fmt.Fprintf(w, "event: import.job\n")
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
}

func (a *App) getOpenAIOAuthSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("session_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	session, err := a.store.GetOAuthSession(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("OAuth session not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if !authCanAccessOwner(authFromContext(r.Context()), session.OwnerUserID) {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": "OAuth session not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"session": session})
}

func (a *App) cancelOpenAIOAuthSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("session_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	session, err := a.store.GetOAuthSession(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("OAuth session not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if !authCanAccessOwner(authFromContext(r.Context()), session.OwnerUserID) {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": "OAuth session not found"})
		return
	}
	if err := a.store.CancelOAuthSession(ctx, id); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "oauth_session_cancel", "api", "oauth_session", id, map[string]any{"owner_user_id": session.OwnerUserID})
	writeJSON(w, http.StatusOK, map[string]any{"session_id": id, "status": "canceled"})
}

func (a *App) exchangeOpenAIOAuthSession(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	var body map[string]any
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	body["session_id"] = sessionID
	data, _ := json.Marshal(body)
	r.Body = io.NopCloser(strings.NewReader(string(data)))
	a.exchangeOpenAIOAuth(w, r)
}

func (a *App) getRequestLog(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("request_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.GetRequestLog(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("request log not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *App) exportRequests(w http.ResponseWriter, r *http.Request) {
	opts := requestLogOptionsFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListRequestLogsFiltered(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if strings.EqualFold(r.URL.Query().Get("format"), "json") {
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	writer := csv.NewWriter(w)
	_ = writer.Write([]string{"request_id", "endpoint", "model_name", "stream", "status_code", "success", "token_id", "account_id", "started_at", "duration_ms", "estimated_cost_usd", "error_message"})
	for _, item := range items {
		_ = writer.Write([]string{
			item.RequestID, item.Endpoint, stringPtr(item.ModelName), strconv.FormatBool(item.IsStream),
			intPtr(item.StatusCode), boolPtr(item.Success), int64Ptr(item.TokenID), stringPtr(item.AccountID),
			item.StartedAt.Format(time.RFC3339), intPtr(item.DurationMs), floatPtr(item.EstimatedCostUSD), stringPtr(item.ErrorMessage),
		})
	}
	writer.Flush()
}

func (a *App) cacheAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	payload, err := a.store.CacheAnalytics(ctx, queryInt(r, "hours", 24))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (a *App) errorAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, err := a.store.ErrorAnalytics(ctx, queryInt(r, "hours", 24), queryInt(r, "limit", 100))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) latencyAnalytics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	payload, err := a.store.LatencyAnalytics(ctx, queryInt(r, "hours", 24))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func (a *App) inflight(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"token_pool": a.tokens.Stats(),
		"upstream":   a.proxy.TransportStats(),
	})
}

func (a *App) workers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"embedded_worker": map[string]any{"enabled": true, "import_worker": "available"},
		"request_log":     a.logs.Stats(),
		"token_pool":      a.tokens.Stats(),
	})
}

func (a *App) requestLogOutbox(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	stats, err := a.store.RequestLogOutboxStats(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (a *App) drainRequestLogOutbox(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	count, err := a.store.DrainRequestLogOutbox(ctx, queryInt(r, "limit", 500))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "request_log_outbox_drain", "api", "request_log_outbox", "", map[string]any{"count": count})
	writeJSON(w, http.StatusOK, map[string]any{"drained": count})
}

func (a *App) aggregateHourlyStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	count, err := a.store.AggregateRequestHourlyStats(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"aggregated": count})
}

func (a *App) cleanupRequestLogs(w http.ResponseWriter, r *http.Request) {
	retention := queryInt(r, "retention_days", a.cfg.RequestLog.RetentionDays)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	count, err := a.store.DeleteOldRequestLogs(ctx, retention)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "request_log_cleanup", "api", "request_logs", "", map[string]any{"retention_days": retention, "deleted": count})
	writeJSON(w, http.StatusOK, map[string]any{"deleted": count, "retention_days": retention})
}

func (a *App) refreshTokenPool(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	err := a.tokens.Refresh(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot": a.tokens.Stats()})
}

func (a *App) tokenPoolSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot := a.tokens.Snapshot()
	items := make([]map[string]any, 0, len(snapshot.Ready))
	for _, token := range snapshot.Ready {
		items = append(items, map[string]any{
			"id":             token.Token.ID,
			"active_streams": token.Active.Load(),
			"last_used_at":   token.Token.LastUsedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": a.tokens.Stats(), "items": items})
}

func (a *App) resetIdleTransport(w http.ResponseWriter, r *http.Request) {
	a.proxy.CloseIdleConnections()
	_ = a.store.WriteAuditLog(r.Context(), "transport_reset_idle", "api", "transport", "", nil)
	writeJSON(w, http.StatusOK, map[string]any{"reset": true, "upstream": a.proxy.TransportStats()})
}

func (a *App) listPromptCacheLanes(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, total, err := a.store.ListPromptAffinityLanes(ctx, queryInt(r, "limit", 100), queryInt(r, "offset", 0))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(queryInt(r, "limit", 100), queryInt(r, "offset", 0), len(items), total)})
}

func (a *App) getPromptCacheLane(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimSpace(r.PathValue("prompt_key_hash"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.GetPromptAffinityLane(ctx, hash)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("prompt cache lane not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *App) deletePromptCacheLane(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimSpace(r.PathValue("prompt_key_hash"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeletePromptAffinityLane(ctx, hash); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("prompt cache lane not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "prompt_cache_lane_delete", "api", "prompt_cache_lane", hash, nil)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "prompt_key_hash": hash})
}

func (a *App) listResponseOwners(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	limit := queryInt(r, "limit", 100)
	offset := queryInt(r, "offset", 0)
	items, total, err := a.store.ListResponseOwnerBindings(ctx, limit, offset, queryInt64(r, "token_id", 0))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(limit, offset, len(items), total)})
}

func (a *App) deleteResponseOwner(w http.ResponseWriter, r *http.Request) {
	hash := strings.TrimSpace(r.PathValue("response_id_hash"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteResponseOwnerBinding(ctx, hash); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("response owner binding not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "prompt_cache_response_owner_delete", "api", "response_owner", hash, nil)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "response_id_hash": hash})
}

func (a *App) deleteTokenResponseOwners(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	count, err := a.store.DeleteTokenResponseOwners(ctx, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_response_owners_delete", "api", "token", strconv.FormatInt(id, 10), map[string]any{"deleted": count})
	writeJSON(w, http.StatusOK, map[string]any{"deleted": count})
}

func (a *App) promptCacheStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	cache, err := a.store.CacheAnalytics(ctx, queryInt(r, "hours", 24))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	owners, ownerTotal, _ := a.store.ListResponseOwnerBindings(ctx, 1, 0, 0)
	lanes, laneTotal, _ := a.store.ListPromptAffinityLanes(ctx, 1, 0)
	writeJSON(w, http.StatusOK, map[string]any{"cache": cache, "lane_count": laneTotal, "owner_count": ownerTotal, "lane_sample": lanes, "owner_sample": owners})
}

func (a *App) getSetting(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.PathValue("key"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	settings, err := a.store.ListSettings(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	for _, item := range settings {
		if item.Key == key {
			writeJSON(w, http.StatusOK, item)
			return
		}
	}
	writeError(w, http.StatusNotFound, errors.New("setting not found"))
}

func (a *App) deleteSetting(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.PathValue("key"))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tag, err := a.store.Pool().Exec(ctx, `delete from gateway_settings where key = $1`, key)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if tag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, errors.New("setting not found"))
		return
	}
	_ = a.store.WriteAuditLog(ctx, "setting_delete", "api", "setting", key, nil)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "key": key})
}

func (a *App) validateSetting(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, http.StatusOK, map[string]any{"valid": true, "key": strings.TrimSpace(r.PathValue("key"))})
}

func (a *App) settingsSchema(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"token_selection": map[string]any{
			"type":       "object",
			"properties": map[string]any{"active_stream_cap": map[string]any{"type": "integer", "minimum": 1}},
		},
		"generic": map[string]any{"type": "object"},
	})
}

func (a *App) listAuditLogs(w http.ResponseWriter, r *http.Request) {
	opts := store.AuditLogListOptions{
		Limit: queryInt(r, "limit", 100), Offset: queryInt(r, "offset", 0),
		Action: r.URL.Query().Get("action"), Actor: r.URL.Query().Get("actor"),
		TargetType: r.URL.Query().Get("target_type"), TargetID: r.URL.Query().Get("target_id"),
		From: queryTime(r, "from"), To: queryTime(r, "to"),
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, total, err := a.store.ListAuditLogs(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) getAuditLog(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "audit_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.GetAuditLog(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("audit log not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *App) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListAdminAPIKeys(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	for i := range items {
		items[i].KeyHash = redactHash(items[i].KeyHash)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key, err := a.store.CreateAdminAPIKey(ctx, payload.Name, payload.Role, "api")
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "admin_api_key_create", "api", "admin_api_key", strconv.FormatInt(key.ID, 10), map[string]any{"name": key.Name, "role": key.Role})
	key.KeyHash = redactHash(key.KeyHash)
	writeJSON(w, http.StatusCreated, key)
}

func (a *App) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "key_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key, err := a.store.RotateAdminAPIKey(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("API key not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "admin_api_key_rotate", "api", "admin_api_key", strconv.FormatInt(id, 10), nil)
	key.KeyHash = redactHash(key.KeyHash)
	writeJSON(w, http.StatusOK, key)
}

func (a *App) revokeAPIKey(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "key_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.RevokeAdminAPIKey(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("API key not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "admin_api_key_revoke", "api", "admin_api_key", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"revoked": true, "id": id})
}

func parseImportPayloadExtended(body any) ([]map[string]any, string, error) {
	if record, ok := body.(map[string]any); ok {
		if text, ok := record["text"].(string); ok && strings.TrimSpace(text) != "" {
			payloads, err := parseImportTextPayloads(text)
			queue := "front"
			if qp, ok := record["import_queue_position"].(string); ok && strings.TrimSpace(qp) != "" {
				queue = normalizeImportQueuePosition(qp)
			}
			return payloads, queue, err
		}
	}
	return parseImportPayload(body)
}

func parseImportTextPayloads(raw string) ([]map[string]any, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		payloads, _, err := parseImportPayload(decoded)
		if err == nil && len(payloads) > 0 {
			return payloads, nil
		}
	}
	out := []map[string]any{}
	for _, line := range strings.Split(text, "\n") {
		value := strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if value == "" || strings.HasPrefix(value, "#") {
			continue
		}
		if strings.Contains(value, "----") {
			parts := splitClean(value, "----")
			if len(parts) > 0 {
				payload := importPayloadFromString(parts[len(parts)-1])
				if strings.Contains(strings.ToLower(parts[0]), "@") {
					payload["email"] = parts[0]
				}
				out = append(out, payload)
				continue
			}
		}
		if strings.Contains(value, ",") {
			parts := splitClean(value, ",")
			if len(parts) >= 2 {
				payload := importPayloadFromString(parts[len(parts)-1])
				payload["account_id"] = parts[0]
				out = append(out, payload)
				continue
			}
		}
		out = append(out, importPayloadFromString(value))
	}
	return out, nil
}

func dedupeImportPayloads(payloads []map[string]any) []map[string]any {
	seen := map[string]struct{}{}
	out := make([]map[string]any, 0, len(payloads))
	for _, payload := range payloads {
		key := stringFromImportPayload(payload, "refresh_token", "refreshToken", "access_token", "accessToken", "token")
		if key == "" {
			data, _ := json.Marshal(payload)
			key = string(data)
		}
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, payload)
	}
	return out
}

func requestLogOptionsFromRequest(r *http.Request) store.RequestLogListOptions {
	opts := store.RequestLogListOptions{
		Limit: queryInt(r, "limit", 100), Offset: queryInt(r, "offset", 0),
		RequestID: r.URL.Query().Get("request_id"), Model: r.URL.Query().Get("model"),
		Endpoint: r.URL.Query().Get("endpoint"), TokenID: queryInt64(r, "token_id", 0),
		AccountID: r.URL.Query().Get("account_id"), From: queryTime(r, "from"), To: queryTime(r, "to"),
		OwnerUserID:      queryInt64(r, "user_id", 0),
		TokenOwnerUserID: queryInt64(r, "token_owner_user_id", 0),
		APIKeyID:         queryInt64(r, "api_key_id", 0),
		QueryError:       firstNonEmpty(r.URL.Query().Get("error"), r.URL.Query().Get("q")),
		IncludeTotal:     strings.EqualFold(r.URL.Query().Get("include_total"), "true"),
	}
	if status := queryInt(r, "status_code", 0); status > 0 {
		opts.StatusCode = &status
	}
	opts.Success = queryTriBool(r, "success")
	opts.Stream = queryTriBool(r, "stream")
	return opts
}

func pathInt64(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	id, err := strconv.ParseInt(strings.TrimSpace(r.PathValue(name)), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid %s", name))
		return 0, false
	}
	return id, true
}

func decodeJSON(r *http.Request, dest any) error {
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	if err := json.NewDecoder(io.LimitReader(r.Body, 4*1024*1024)).Decode(dest); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func pagination(limit int, offset int, returned int, total int) map[string]any {
	return map[string]any{
		"limit": limit, "offset": offset, "returned": returned, "total": total,
		"has_previous": offset > 0, "has_next": offset+returned < total,
		"next_offset": offset + returned,
	}
}

func queryInt64(r *http.Request, key string, fallback int64) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func queryTriBool(r *http.Request, key string) *bool {
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(key)))
	if raw == "" {
		return nil
	}
	value := raw == "1" || raw == "true" || raw == "yes" || raw == "on"
	if raw == "0" || raw == "false" || raw == "no" || raw == "off" {
		value = false
	}
	return &value
}

func queryTime(r *http.Request, key string) *time.Time {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if value, err := time.Parse(layout, raw); err == nil {
			utc := value.UTC()
			return &utc
		}
	}
	return nil
}

func appendQuery(raw string, key string, value string) string {
	if raw == "" {
		return key + "=" + value
	}
	return raw + "&" + key + "=" + value
}

func splitClean(value string, sep string) []string {
	parts := strings.Split(value, sep)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func randomID(prefix string) string {
	data := make([]byte, 12)
	if _, err := rand.Read(data); err != nil {
		sum := sha256.Sum256([]byte(time.Now().String()))
		return prefix + "_" + hex.EncodeToString(sum[:8])
	}
	return prefix + "_" + hex.EncodeToString(data)
}

func jsonHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func redactHash(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 12 {
		return value
	}
	return value[:8] + "..." + value[len(value)-4:]
}

func stringPtr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func timePtr(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func intPtr(value *int) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(*value)
}

func int64Ptr(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}

func boolPtr(value *bool) string {
	if value == nil {
		return ""
	}
	return strconv.FormatBool(*value)
}

func floatPtr(value *float64) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%.6f", *value)
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
