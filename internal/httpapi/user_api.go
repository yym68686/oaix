package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) registerUserAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/auth/register", a.registerUser)
	mux.HandleFunc("POST /api/auth/login", a.loginUser)
	mux.HandleFunc("GET /api/me", a.requireAuth(a.me))
	mux.HandleFunc("GET /api/me/api-keys", a.requireAuth(a.listMyAPIKeys))
	mux.HandleFunc("POST /api/me/api-keys", a.requireAuth(a.createMyAPIKey))
	mux.HandleFunc("DELETE /api/me/api-keys/{key_id}", a.requireAuth(a.revokeMyAPIKey))
	mux.HandleFunc("GET /api/me/usage", a.requireAuth(a.myUsage))
	mux.HandleFunc("GET /api/me/pool-summary", a.requireAuth(a.myPoolSummary))
	mux.HandleFunc("GET /api/me/settings", a.requireAuth(a.listMySettings))
	mux.HandleFunc("GET /api/me/settings/{key}", a.requireAuth(a.getMySetting))
	mux.HandleFunc("POST /api/me/settings/{key}", a.requireAuth(a.updateMySetting))
	mux.HandleFunc("DELETE /api/me/settings/{key}", a.requireAuth(a.deleteMySetting))
	mux.HandleFunc("GET /api/tokens", a.requireAuth(a.listMyTokens))
	mux.HandleFunc("GET /api/tokens/quota", a.requireAuth(a.listMyTokenQuota))
	mux.HandleFunc("POST /api/tokens/quota-refresh", a.requireAuth(a.createMyQuotaRefreshJob))
	mux.HandleFunc("GET /api/tokens/{token_id}", a.requireAuth(a.getMyToken))
	mux.HandleFunc("PATCH /api/tokens/{token_id}", a.requireAuth(a.patchMyToken))
	mux.HandleFunc("DELETE /api/tokens/{token_id}", a.requireAuth(a.deleteMyToken))
	mux.HandleFunc("POST /api/tokens/{token_id}/probe", a.requireAuth(a.probeMyToken))
	mux.HandleFunc("POST /api/import/parse", a.requireAuth(a.parseImport))
	mux.HandleFunc("POST /api/import/upload", a.requireAuth(a.uploadImport))
	mux.HandleFunc("POST /api/import/jobs", a.requireAuth(a.createMyImportJob))
	mux.HandleFunc("GET /api/import/jobs", a.requireAuth(a.listMyImportJobs))
	mux.HandleFunc("GET /api/import/jobs/{job_id}", a.requireAuth(a.getMyImportJob))
	mux.HandleFunc("POST /api/import/jobs/{job_id}/cancel", a.requireAuth(a.cancelMyImportJob))
	mux.HandleFunc("DELETE /api/import/jobs/{job_id}", a.requireAuth(a.deleteMyImportJob))
	mux.HandleFunc("GET /api/import/jobs/{job_id}/items", a.requireAuth(a.listMyImportJobItems))
	mux.HandleFunc("GET /api/import/jobs/{job_id}/tokens", a.requireAuth(a.listMyImportJobTokens))
	mux.HandleFunc("POST /api/oauth/openai/sessions", a.requireAuth(a.startOpenAIOAuth))
	mux.HandleFunc("GET /api/oauth/openai/sessions/{session_id}", a.requireAuth(a.getOpenAIOAuthSession))
	mux.HandleFunc("POST /api/oauth/openai/sessions/{session_id}/exchange", a.requireAuth(a.exchangeOpenAIOAuthSession))
	mux.HandleFunc("GET /api/requests", a.requireAuth(a.listMyRequests))
}

func (a *App) registerUser(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Email       string `json:"email"`
		Password    string `json:"password"`
		DisplayName string `json:"display_name"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, apiKey, err := a.store.CreateRegisteredUser(ctx, payload.Email, payload.Password, payload.DisplayName)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			status = http.StatusConflict
		}
		writeError(w, status, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_register", "self", "user", strconv.FormatInt(user.ID, 10), map[string]any{"email": user.Email})
	writeJSON(w, http.StatusCreated, map[string]any{"user": user, "api_key": apiKey})
}

func (a *App) loginUser(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Name     string `json:"name"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, err := a.store.AuthenticatePlatformUser(ctx, payload.Email, payload.Password)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid email or password"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	apiKey, err := a.store.CreateAPIKey(ctx, &user.ID, "user", firstNonEmpty(payload.Name, "login"), user.Role, &user.ID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_login_api_key_create", "self", "api_key", strconv.FormatInt(apiKey.ID, 10), map[string]any{"user_id": user.ID})
	writeJSON(w, http.StatusOK, map[string]any{"user": user, "api_key": apiKey})
}

func (a *App) me(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	if auth == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid or missing API key"})
		return
	}
	capabilities := []string{"tokens:read", "tokens:write", "imports:write", "requests:read"}
	if auth.IsAdmin || auth.IsService {
		capabilities = append(capabilities, "admin:read", "admin:write")
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"principal_type": auth.PrincipalType,
		"user":           auth.User,
		"role":           auth.Role,
		"scopes":         auth.Scopes,
		"capabilities":   capabilities,
		"act_as_user_id": auth.ActAsUserID,
	})
}

func (a *App) listMyAPIKeys(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := apiKeyManagementScope(r.Context(), a.store, w, auth)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListAPIKeys(ctx, scope, nil)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func apiKeyManagementScope(ctx context.Context, st *store.Store, w http.ResponseWriter, auth *AuthContext) (store.ResourceScope, bool) {
	if auth == nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": "User API key required"})
		return store.ResourceScope{}, false
	}
	if auth.UserID != nil {
		return store.OwnerResources(*auth.UserID), true
	}
	if auth.IsService || auth.IsAdmin {
		id, err := st.BootstrapUserID(ctx)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return store.ResourceScope{}, false
		}
		return store.OwnerResources(id), true
	}
	writeJSON(w, http.StatusForbidden, map[string]any{"detail": "User API key required"})
	return store.ResourceScope{}, false
}

func apiKeyManagementOwner(ctx context.Context, st *store.Store, w http.ResponseWriter, auth *AuthContext) (*int64, bool) {
	scope, ok := apiKeyManagementScope(ctx, st, w, auth)
	if !ok || scope.OwnerUserID == nil {
		return nil, false
	}
	return scope.OwnerUserID, true
}

func (a *App) createMyAPIKey(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	ownerID, ok := apiKeyManagementOwner(ctx, a.store, w, auth)
	if !ok {
		return
	}
	var payload struct {
		Name string `json:"name"`
	}
	_ = decodeJSON(r, &payload)
	role := "user"
	kind := "user"
	if auth != nil && (auth.IsService || auth.IsAdmin) {
		role = firstNonEmpty(auth.Role, "admin")
		if auth.IsService {
			kind = "service"
			role = "service"
		}
	}
	key, err := a.store.CreateAPIKey(ctx, ownerID, kind, payload.Name, role, auth.UserID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_api_key_create", firstNonEmpty(role, "self"), "api_key", strconv.FormatInt(key.ID, 10), map[string]any{"user_id": *ownerID})
	writeJSON(w, http.StatusCreated, map[string]any{"api_key": key})
}

func (a *App) revokeMyAPIKey(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	id, ok := pathInt64(w, r, "key_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	scope, ok := apiKeyManagementScope(ctx, a.store, w, auth)
	if !ok {
		return
	}
	if err := a.store.RevokeAPIKey(ctx, scope, id); errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "API key not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_api_key_revoke", firstNonEmpty(auth.Role, "self"), "api_key", strconv.FormatInt(id, 10), map[string]any{"user_id": scope.OwnerUserID})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *App) myUsage(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	usage, err := a.store.RequestUsageSummaryScoped(ctx, scope, queryInt(r, "hours", 24))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"usage": usage})
}

func (a *App) myPoolSummary(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	counts, err := a.store.TokenCountsScoped(ctx, scope)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	plans, _ := a.store.TokenPlanCountsScoped(ctx, scope, store.TokenListOptions{})
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "plan_counts": plans})
}

func (a *App) listMySettings(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListUserSettings(ctx, *scope.OwnerUserID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) getMySetting(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	key := strings.TrimSpace(r.PathValue("key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, errors.New("setting key is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	item, err := a.store.GetUserSetting(ctx, *scope.OwnerUserID, key)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("setting not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *App) updateMySetting(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
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
	item, err := a.store.UpsertUserSetting(ctx, *scope.OwnerUserID, key, body)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_setting_update", "self", "user_setting", key, map[string]any{"user_id": *scope.OwnerUserID})
	writeJSON(w, http.StatusOK, item)
}

func (a *App) deleteMySetting(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	key := strings.TrimSpace(r.PathValue("key"))
	if key == "" {
		writeError(w, http.StatusBadRequest, errors.New("setting key is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteUserSetting(ctx, *scope.OwnerUserID, key); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("setting not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_setting_delete", "self", "user_setting", key, map[string]any{"user_id": *scope.OwnerUserID})
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "key": key})
}

func (a *App) listMyTokens(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	limit := queryInt(r, "limit", 100)
	offset := queryInt(r, "offset", 0)
	opts := tokenListOptionsFromRequest(r, store.TokenListOptions{Limit: limit, Offset: offset})
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, total, err := a.store.ListTokensScoped(ctx, scope, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	adminItems, pendingIDs := a.adminTokenItems(r.Context(), items, queryBool(r, "include_quota", false))
	counts, _ := a.store.TokenCountsScoped(ctx, scope)
	planCounts, _ := a.store.TokenPlanCountsScoped(ctx, scope, opts)
	writeJSON(w, http.StatusOK, map[string]any{
		"counts":                    counts,
		"filtered_counts":           counts,
		"plan_counts":               planCounts,
		"pagination":                pagination(limit, offset, len(adminItems), total),
		"items":                     adminItems,
		"quota_refresh_pending_ids": pendingIDs,
	})
}

func (a *App) getMyToken(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetTokenScoped(ctx, scope, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
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

func (a *App) listMyTokenQuota(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	ids, err := parseAdminTokenIDs(r.URL.Query().Get("ids"), 100)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"items": []adminTokenItem{}})
		return
	}
	if queryBool(r, "force_refresh", false) && a.quota != nil {
		a.quota.clear(ids)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tokens, err := a.store.ListTokensByIDsScoped(ctx, scope, ids)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	items, pendingIDs := a.adminTokenItems(r.Context(), tokens, true)
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "quota_refresh_pending_ids": pendingIDs})
}

func (a *App) createMyQuotaRefreshJob(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
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
		tokens, _, err = a.store.ListTokensScoped(ctx, scope, store.TokenListOptions{Limit: 500, Status: r.URL.Query().Get("status"), Plan: r.URL.Query().Get("plan")})
	} else {
		tokens, err = a.store.ListTokensByIDsScoped(ctx, scope, payload.TokenIDs)
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
	_ = a.store.WriteAuditLog(ctx, "user_quota_refresh_job_create", "self", "quota_refresh_job", jobID, map[string]any{"user_id": *scope.OwnerUserID, "token_count": len(ids), "all": payload.All})
	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func (a *App) patchMyToken(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		Remark              *string `json:"remark"`
		IsActive            *bool   `json:"is_active"`
		ShareEnabled        *bool   `json:"share_enabled"`
		ShareEnabledCamel   *bool   `json:"shareEnabled"`
		ShareStatus         *string `json:"share_status"`
		ShareStatusCamel    *string `json:"shareStatus"`
		ShareDisabledReason *string `json:"share_disabled_reason"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var token *store.Token
	var err error
	shareEnabled := payload.ShareEnabled
	if shareEnabled == nil {
		shareEnabled = payload.ShareEnabledCamel
	}
	if shareEnabled != nil {
		status := stringPtr(payload.ShareStatus)
		if status == "" {
			status = stringPtr(payload.ShareStatusCamel)
		}
		token, err = a.store.SetTokenSharingScoped(ctx, scope, id, *shareEnabled, status, stringPtr(payload.ShareDisabledReason))
	} else if payload.Remark != nil {
		token, err = a.store.UpdateTokenRemarkScoped(ctx, scope, id, *payload.Remark)
	} else if payload.IsActive != nil {
		token, err = a.store.SetTokenActiveScoped(ctx, scope, id, *payload.IsActive, *payload.IsActive)
	} else {
		token, err = a.store.GetTokenScoped(ctx, scope, id)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_update", "self", "token", strconv.FormatInt(id, 10), map[string]any{"user_id": *scope.OwnerUserID, "remark": payload.Remark != nil, "is_active": payload.IsActive, "share_enabled": shareEnabled})
	_ = a.tokens.Refresh(ctx)
	writeJSON(w, http.StatusOK, map[string]any{"token": token})
}

func (a *App) deleteMyToken(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteTokenScoped(ctx, scope, id); errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_delete", "self", "token", strconv.FormatInt(id, 10), map[string]any{"user_id": *scope.OwnerUserID})
	_ = a.tokens.Refresh(ctx)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "deleted": true})
}

func (a *App) probeMyToken(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	var payload struct {
		Model string `json:"model"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetTokenScoped(ctx, scope, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	result := a.probeTokenWithAccess(r.Context(), *token, payload.Model)
	writeJSON(w, http.StatusOK, result)
}

func userScope(w http.ResponseWriter, auth *AuthContext) (store.ResourceScope, bool) {
	if auth == nil || auth.UserID == nil {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": "User API key required"})
		return store.ResourceScope{}, false
	}
	return store.OwnerResources(*auth.UserID), true
}

func (a *App) createMyImportJob(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	defer r.Body.Close()
	var body any
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16*1024*1024)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
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
	job, err := a.store.CreateQueuedImportJobForOwner(ctx, *scope.OwnerUserID, payloads, queuePosition)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_import_job_create", "self", "import_job", strconv.FormatInt(job.ID, 10), map[string]any{"total": len(payloads), "user_id": *scope.OwnerUserID})
	writeJSON(w, http.StatusAccepted, map[string]any{"job": job})
}

func (a *App) listMyImportJobs(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
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
	jobs, total, err := a.store.ListImportJobsFilteredScoped(ctx, scope, opts)
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

func (a *App) getMyImportJob(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	summary, err := a.store.GetImportJobSummaryScoped(ctx, scope, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "import job not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}

func (a *App) cancelMyImportJob(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, err := a.store.GetImportJobScoped(ctx, scope, id); errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "import job not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	job, err := a.store.CancelImportJob(ctx, id)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_import_job_cancel", "self", "import_job", strconv.FormatInt(id, 10), map[string]any{"user_id": *scope.OwnerUserID})
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (a *App) deleteMyImportJob(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if _, err := a.store.GetImportJobScoped(ctx, scope, id); errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "import job not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if queryBool(r, "dry_run", false) {
		result, err := a.store.DeleteImportJobDryRun(ctx, id)
		if err != nil {
			if strings.Contains(err.Error(), "cannot delete") {
				writeError(w, http.StatusConflict, err)
				return
			}
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"dry_run": true, "job": result.ImportJob, "token_ids": result.TokenIDs, "deleted_tokens": result.DeletedTokens})
		return
	}
	result, err := a.store.DeleteImportJobWithTokens(ctx, id)
	if err != nil {
		if strings.Contains(err.Error(), "cannot delete") {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.tokens.Refresh(ctx)
	_ = a.store.WriteAuditLog(ctx, "user_token_import_job_delete", "self", "import_job", strconv.FormatInt(id, 10), map[string]any{"user_id": *scope.OwnerUserID, "deleted_tokens": result.DeletedTokens})
	writeJSON(w, http.StatusOK, map[string]any{"job": result.ImportJob, "token_ids": result.TokenIDs, "deleted_tokens": result.DeletedTokens})
}

func (a *App) listMyImportJobItems(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	opts := store.ImportItemListOptions{Limit: queryInt(r, "limit", 100), Offset: queryInt(r, "offset", 0), Status: r.URL.Query().Get("status"), Action: r.URL.Query().Get("action"), Error: r.URL.Query().Get("error")}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListImportJobItemsFilteredScoped(ctx, scope, id, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": sanitizeImportItems(items), "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) listMyImportJobTokens(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "job_id")
	if !ok {
		return
	}
	opts := tokenListOptionsFromRequest(r, store.TokenListOptions{Limit: queryInt(r, "limit", 100), Offset: queryInt(r, "offset", 0), ImportJobID: id})
	opts.ImportJobID = id
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tokens, total, err := a.store.ListTokensScoped(ctx, scope, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	adminItems, pendingIDs := a.adminTokenItems(r.Context(), tokens, queryBool(r, "include_quota", false))
	counts, _ := a.store.TokenCountsScoped(ctx, scope)
	planCounts, _ := a.store.TokenPlanCountsScoped(ctx, scope, opts)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": adminItems, "pagination": pagination(opts.Limit, opts.Offset, len(adminItems), total),
		"counts": counts, "plan_counts": planCounts, "quota_refresh_pending_ids": pendingIDs,
	})
}

func (a *App) listMyRequests(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok {
		return
	}
	a.writeRequestList(w, r, scope)
}
