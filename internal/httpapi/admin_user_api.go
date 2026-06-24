package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) registerPlatformAdminAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/admin/users", a.requireAuth(a.listPlatformUsers))
	mux.HandleFunc("POST /api/admin/users", a.requireAuth(a.createPlatformUser))
	mux.HandleFunc("GET /api/admin/users/{user_id}", a.requireAuth(a.getPlatformUser))
	mux.HandleFunc("PATCH /api/admin/users/{user_id}", a.requireAuth(a.patchPlatformUser))
	mux.HandleFunc("GET /api/admin/users/{user_id}/api-keys", a.requireAuth(a.listPlatformUserAPIKeys))
	mux.HandleFunc("POST /api/admin/users/{user_id}/api-keys", a.requireAuth(a.createPlatformUserAPIKey))
	mux.HandleFunc("DELETE /api/admin/users/{user_id}/api-keys/{key_id}", a.requireAuth(a.revokePlatformUserAPIKey))
	mux.HandleFunc("GET /api/admin/users/{user_id}/tokens", a.requireAuth(a.listPlatformUserTokens))
	mux.HandleFunc("GET /api/admin/users/{user_id}/import/jobs", a.requireAuth(a.listPlatformUserImportJobs))
	mux.HandleFunc("GET /api/admin/users/{user_id}/requests", a.requireAuth(a.listPlatformUserRequests))
	mux.HandleFunc("GET /api/admin/users/{user_id}/usage", a.requireAuth(a.platformUserUsage))
	mux.HandleFunc("GET /api/admin/pool-summary", a.requireAuth(a.platformPoolSummary))
	mux.HandleFunc("GET /api/admin/pool-summary/by-user", a.requireAuth(a.platformPoolSummaryByUser))
	mux.HandleFunc("GET /api/admin/analytics/users", a.requireAuth(a.platformPoolSummaryByUser))
	mux.HandleFunc("GET /api/admin/requests", a.requireAuth(a.listPlatformRequests))
	mux.HandleFunc("GET /api/admin/requests/export", a.requireAuth(a.exportRequests))
	mux.HandleFunc("GET /api/admin/audit-logs", a.requireAuth(a.listPlatformAuditLogs))
	mux.HandleFunc("GET /api/admin/audit-logs/{audit_id}", a.requireAuth(a.getPlatformAuditLog))
	a.registerSub2APIAdminRoutes(mux)
}

func requirePlatformAdmin(w http.ResponseWriter, r *http.Request) (*AuthContext, bool) {
	auth := authFromContext(r.Context())
	if auth == nil || (!auth.IsAdmin && !auth.IsService) {
		writeJSON(w, http.StatusForbidden, map[string]any{"detail": "Admin permission required"})
		return nil, false
	}
	return auth, true
}

func (a *App) listPlatformUsers(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	opts := store.PlatformUserListOptions{
		Limit:             queryInt(r, "limit", 100),
		Offset:            queryInt(r, "offset", 0),
		Query:             r.URL.Query().Get("q"),
		Role:              r.URL.Query().Get("role"),
		Status:            r.URL.Query().Get("status"),
		Plan:              r.URL.Query().Get("plan"),
		ActiveWithinHours: queryInt(r, "active_within_hours", 0),
		InactiveForHours:  queryInt(r, "inactive_for_hours", 0),
		Sort:              r.URL.Query().Get("sort"),
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	users, total, err := a.store.ListPlatformUsers(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": users, "pagination": pagination(opts.Limit, opts.Offset, len(users), total)})
}

func (a *App) createPlatformUser(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var payload struct {
		Email        string `json:"email"`
		Password     string `json:"password"`
		DisplayName  string `json:"display_name"`
		Role         string `json:"role"`
		CreateAPIKey bool   `json:"create_api_key"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, err := a.store.CreatePlatformUserWithPassword(ctx, payload.Email, payload.Password, payload.DisplayName, payload.Role)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var key *store.CreatedAPIKey
	if payload.CreateAPIKey {
		created, err := a.store.CreateAPIKey(ctx, &user.ID, "user", "initial", user.Role, auth.UserID)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		key = &created
	}
	_ = a.store.WriteAuditLog(ctx, "platform_user_create", auth.Role, "user", strconv.FormatInt(user.ID, 10), map[string]any{"email": user.Email, "role": user.Role})
	writeJSON(w, http.StatusCreated, map[string]any{"user": user, "api_key": key})
}

func (a *App) getPlatformUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	id, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, err := a.store.GetPlatformUser(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "user not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

func (a *App) patchPlatformUser(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	var patch store.PlatformUserPatch
	if err := decodeJSON(r, &patch); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	user, err := a.store.UpdatePlatformUser(ctx, id, patch)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "user not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "platform_user_update", auth.Role, "user", strconv.FormatInt(id, 10), patch)
	writeJSON(w, http.StatusOK, map[string]any{"user": user})
}

func (a *App) listPlatformUserAPIKeys(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListAPIKeys(ctx, store.AllResources(), &userID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) createPlatformUserAPIKey(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	var payload struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	_ = decodeJSON(r, &payload)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key, err := a.store.CreateAPIKey(ctx, &userID, "user", payload.Name, firstNonEmpty(payload.Role, "user"), auth.UserID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "platform_user_api_key_create", auth.Role, "api_key", strconv.FormatInt(key.ID, 10), map[string]any{"user_id": userID})
	writeJSON(w, http.StatusCreated, map[string]any{"api_key": key})
}

func (a *App) revokePlatformUserAPIKey(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	keyID, ok := pathInt64(w, r, "key_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.RevokeAPIKey(ctx, store.AllResources(), keyID); errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "api key not found"})
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "platform_user_api_key_revoke", auth.Role, "api_key", strconv.FormatInt(keyID, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *App) listPlatformUserTokens(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	a.writeTokenList(w, r, store.OwnerResources(userID))
}

func (a *App) listPlatformUserImportJobs(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	opts := store.ImportJobListOptions{Limit: queryInt(r, "limit", 50), Offset: queryInt(r, "offset", 0), Status: r.URL.Query().Get("status"), Source: r.URL.Query().Get("source"), From: queryTime(r, "from"), To: queryTime(r, "to"), Sort: r.URL.Query().Get("sort")}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	jobs, total, err := a.store.ListImportJobsFilteredScoped(ctx, store.OwnerResources(userID), opts)
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

func (a *App) listPlatformUserRequests(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	opts := requestLogOptionsFromRequest(r)
	opts.TokenOwnerUserID = userID
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListRequestLogsFilteredScoped(ctx, store.AllResources(), opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) listPlatformRequests(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	a.writeRequestList(w, r, store.AllResources())
}

func (a *App) listPlatformAuditLogs(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	a.listAuditLogs(w, r)
}

func (a *App) getPlatformAuditLog(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	a.getAuditLog(w, r)
}

func (a *App) platformUserUsage(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	userID, ok := pathInt64(w, r, "user_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	counts, _ := a.store.TokenCountsScoped(ctx, store.OwnerResources(userID))
	usageByOwner, _ := a.store.RequestUsageByTokenOwner(ctx, []int64{userID}, queryInt(r, "hours", 24))
	observedCostsByOwner, _ := a.store.TokenObservedCostsByOwner(ctx, []int64{userID})
	usage := usageByOwner[userID]
	usage.ObservedCostUSD = observedCostsByOwner[userID]
	writeJSON(w, http.StatusOK, map[string]any{"pool": counts, "usage": usage})
}

func (a *App) platformPoolSummary(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	counts, err := a.store.TokenCountsScoped(ctx, store.AllResources())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	plans, _ := a.store.TokenPlanCountsScoped(ctx, store.AllResources(), store.TokenListOptions{})
	errorCounts, _ := a.store.TokenErrorCountsScoped(ctx, store.AllResources(), 10)
	writeJSON(w, http.StatusOK, map[string]any{"counts": counts, "plan_counts": plans, "error_counts": errorCounts})
}

func (a *App) platformPoolSummaryByUser(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	opts := store.PlatformUserListOptions{
		Limit:             queryInt(r, "limit", 100),
		Offset:            queryInt(r, "offset", 0),
		Query:             r.URL.Query().Get("q"),
		Status:            r.URL.Query().Get("status"),
		Role:              r.URL.Query().Get("role"),
		Plan:              r.URL.Query().Get("plan"),
		ActiveWithinHours: queryInt(r, "active_within_hours", 0),
		InactiveForHours:  queryInt(r, "inactive_for_hours", 0),
		Sort:              r.URL.Query().Get("sort"),
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	users, total, err := a.store.ListPlatformUsers(ctx, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	ownerIDs := make([]int64, 0, len(users))
	for _, user := range users {
		ownerIDs = append(ownerIDs, user.ID)
	}
	usageByOwner, _ := a.store.RequestUsageByTokenOwner(ctx, ownerIDs, queryInt(r, "hours", 24))
	observedCostsByOwner, _ := a.store.TokenObservedCostsByOwner(ctx, ownerIDs)
	items := make([]map[string]any, 0, len(users))
	for _, user := range users {
		counts, _ := a.store.TokenCountsScoped(ctx, store.OwnerResources(user.ID))
		plans, _ := a.store.TokenPlanCountsScoped(ctx, store.OwnerResources(user.ID), store.TokenListOptions{})
		usage := usageByOwner[user.ID]
		usage.ObservedCostUSD = observedCostsByOwner[user.ID]
		items = append(items, map[string]any{"user": user, "counts": counts, "plan_counts": plans, "usage": usage})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}

func (a *App) writeTokenList(w http.ResponseWriter, r *http.Request, scope store.ResourceScope) {
	limit := queryInt(r, "limit", 100)
	offset := queryInt(r, "offset", 0)
	opts := tokenListOptionsFromRequest(r, store.TokenListOptions{Limit: limit, Offset: offset})
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tokens, total, err := a.store.ListTokensScoped(ctx, scope, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	asOf := store.TokenListStatusAsOf(opts)
	adminItems, pendingIDs := a.adminTokenItemsAt(r.Context(), tokens, queryBool(r, "include_quota", false), asOf)
	counts, _ := a.store.TokenCountsScopedAt(ctx, scope, asOf)
	planCounts, _ := a.store.TokenPlanCountsScoped(ctx, scope, opts)
	writeJSON(w, http.StatusOK, map[string]any{"items": adminItems, "pagination": pagination(limit, offset, len(adminItems), total), "counts": counts, "plan_counts": planCounts, "quota_refresh_pending_ids": pendingIDs})
}

func (a *App) writeRequestList(w http.ResponseWriter, r *http.Request, scope store.ResourceScope) {
	opts := requestLogOptionsFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, total, err := a.store.ListRequestLogsFilteredScoped(ctx, scope, opts)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "pagination": pagination(opts.Limit, opts.Offset, len(items), total)})
}
