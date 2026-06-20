package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/sub2api"
)

type sub2APITargetPayload struct {
	Name                 string   `json:"name"`
	BaseURL              string   `json:"base_url"`
	AdminKey             *string  `json:"admin_key"`
	Enabled              *bool    `json:"enabled"`
	OwnerUserID          int64    `json:"owner_user_id"`
	PlanFilters          []string `json:"plan_filters"`
	TargetGroupIDs       []int64  `json:"target_group_ids"`
	TargetGroupNames     []string `json:"target_group_names"`
	CheckIntervalSeconds int      `json:"check_interval_seconds"`
	MinAvailable         int      `json:"min_available"`
	TopUpBatchSize       int      `json:"top_up_batch_size"`
	AutoSyncNew          *bool    `json:"auto_sync_new"`
}

func (a *App) registerSub2APIAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/admin/sub2api/targets", a.requireAuth(a.listSub2APITargets))
	mux.HandleFunc("POST /api/admin/sub2api/targets", a.requireAuth(a.createSub2APITarget))
	mux.HandleFunc("GET /api/admin/sub2api/targets/{target_id}", a.requireAuth(a.getSub2APITarget))
	mux.HandleFunc("PATCH /api/admin/sub2api/targets/{target_id}", a.requireAuth(a.updateSub2APITarget))
	mux.HandleFunc("DELETE /api/admin/sub2api/targets/{target_id}", a.requireAuth(a.deleteSub2APITarget))
	mux.HandleFunc("GET /api/admin/sub2api/targets/{target_id}/groups", a.requireAuth(a.getSub2APITargetGroups))
	mux.HandleFunc("POST /api/admin/sub2api/targets/{target_id}/check", a.requireAuth(a.checkSub2APITarget))
	mux.HandleFunc("POST /api/admin/sub2api/targets/{target_id}/sync", a.requireAuth(a.syncSub2APITarget))
	mux.HandleFunc("GET /api/admin/sub2api/targets/{target_id}/runs", a.requireAuth(a.listSub2APITargetRuns))
	mux.HandleFunc("POST /api/admin/sub2api/probe-groups", a.requireAuth(a.probeSub2APIGroups))
	mux.HandleFunc("GET /api/admin/sub2api/runs", a.requireAuth(a.listSub2APIRuns))
}

func (a *App) sub2APISyncer() *sub2api.Syncer {
	return sub2api.NewSyncer(a.store, nil, a.logger, a.cfg.Upstream.OAuthClientID)
}

func (a *App) listSub2APITargets(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListSub2APISyncTargets(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) createSub2APITarget(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var payload sub2APITargetPayload
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	target, err := a.store.CreateSub2APISyncTarget(ctx, sub2APITargetInput(payload))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "sub2api_target_create", auth.Role, "sub2api_target", strconv.FormatInt(target.ID, 10), redactSub2APIPayload(payload))
	writeJSON(w, http.StatusCreated, map[string]any{"target": target})
}

func (a *App) getSub2APITarget(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	target, err := a.store.GetSub2APISyncTarget(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("sub2api target not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"target": target})
}

func (a *App) updateSub2APITarget(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	var payload sub2APITargetPayload
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	target, err := a.store.UpdateSub2APISyncTarget(ctx, id, sub2APITargetInput(payload))
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("sub2api target not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "sub2api_target_update", auth.Role, "sub2api_target", strconv.FormatInt(target.ID, 10), redactSub2APIPayload(payload))
	writeJSON(w, http.StatusOK, map[string]any{"target": target})
}

func (a *App) deleteSub2APITarget(w http.ResponseWriter, r *http.Request) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteSub2APISyncTarget(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("sub2api target not found"))
		return
	} else if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "sub2api_target_delete", auth.Role, "sub2api_target", strconv.FormatInt(id, 10), nil)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (a *App) getSub2APITargetGroups(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	target, err := a.store.GetSub2APISyncTarget(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("sub2api target not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	groups, err := a.sub2APISyncer().ListGroups(ctx, target)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": groups})
}

func (a *App) probeSub2APIGroups(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	var payload struct {
		BaseURL  string `json:"base_url"`
		AdminKey string `json:"admin_key"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	groups, err := a.sub2APISyncer().ProbeGroups(ctx, payload.BaseURL, payload.AdminKey)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": groups})
}

func (a *App) checkSub2APITarget(w http.ResponseWriter, r *http.Request) {
	a.runSub2APITarget(w, r, "check")
}

func (a *App) syncSub2APITarget(w http.ResponseWriter, r *http.Request) {
	a.runSub2APITarget(w, r, "sync")
}

func (a *App) runSub2APITarget(w http.ResponseWriter, r *http.Request, mode string) {
	auth, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	var run store.Sub2APISyncRun
	var err error
	if mode == "check" {
		run, err = a.sub2APISyncer().CheckTarget(ctx, id, "check")
	} else {
		run, err = a.sub2APISyncer().SyncTarget(ctx, id, "manual")
	}
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("sub2api target not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "sub2api_target_"+mode, auth.Role, "sub2api_target", strconv.FormatInt(id, 10), map[string]any{"run_id": run.ID, "status": run.Status})
	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (a *App) listSub2APITargetRuns(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	id, ok := pathInt64(w, r, "target_id")
	if !ok {
		return
	}
	a.writeSub2APIRuns(w, r, id)
}

func (a *App) listSub2APIRuns(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	a.writeSub2APIRuns(w, r, queryInt64(r, "target_id", 0))
}

func (a *App) writeSub2APIRuns(w http.ResponseWriter, r *http.Request, targetID int64) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListSub2APISyncRuns(ctx, targetID, queryInt(r, "limit", 80))
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func sub2APITargetInput(payload sub2APITargetPayload) store.Sub2APISyncTargetInput {
	return store.Sub2APISyncTargetInput{
		Name:                 payload.Name,
		BaseURL:              payload.BaseURL,
		AdminKey:             payload.AdminKey,
		Enabled:              payload.Enabled,
		OwnerUserID:          payload.OwnerUserID,
		PlanFilters:          payload.PlanFilters,
		TargetGroupIDs:       payload.TargetGroupIDs,
		TargetGroupNames:     payload.TargetGroupNames,
		CheckIntervalSeconds: payload.CheckIntervalSeconds,
		MinAvailable:         payload.MinAvailable,
		TopUpBatchSize:       payload.TopUpBatchSize,
		AutoSyncNew:          payload.AutoSyncNew,
	}
}

func redactSub2APIPayload(payload sub2APITargetPayload) map[string]any {
	return map[string]any{
		"name":                   payload.Name,
		"base_url":               payload.BaseURL,
		"admin_key_set":          payload.AdminKey != nil && *payload.AdminKey != "",
		"enabled":                payload.Enabled,
		"owner_user_id":          payload.OwnerUserID,
		"plan_filters":           payload.PlanFilters,
		"target_group_ids":       payload.TargetGroupIDs,
		"target_group_names":     payload.TargetGroupNames,
		"check_interval_seconds": payload.CheckIntervalSeconds,
		"min_available":          payload.MinAvailable,
		"top_up_batch_size":      payload.TopUpBatchSize,
		"auto_sync_new":          payload.AutoSyncNew,
	}
}
