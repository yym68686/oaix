package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/modelaccess"
	"github.com/yym68686/oaix/internal/store"
)

type tokenModelAccessPlan struct {
	Plan            string   `json:"plan"`
	Label           string   `json:"label"`
	TokenCount      int      `json:"token_count"`
	Models          []string `json:"models"`
	AvailableModels []string `json:"available_models"`
	CapabilityKnown bool     `json:"capability_known"`
	Inherited       []string `json:"inherited_models"`
	OverrideModels  []string `json:"override_models,omitempty"`
	Overridden      bool     `json:"overridden"`
}

func (a *App) getMyTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	a.writeTokenModelAccess(w, r, false)
}

func (a *App) getAdminTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	a.writeTokenModelAccess(w, r, true)
}

func (a *App) updateMyTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok || scope.OwnerUserID == nil {
		return
	}
	planModels, err := decodeTokenModelAccessBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	settings, err := a.store.UpsertUserTokenModelAccess(ctx, *scope.OwnerUserID, planModels)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_model_access_update", "self", "user_setting", store.TokenModelAccessSettingKey, map[string]any{
		"user_id": *scope.OwnerUserID, "plan_models": settings.PlanModels,
	})
	a.refreshTokenPoolSettings(ctx, *scope.OwnerUserID)
	a.writeTokenModelAccessWithSettings(w, r, false, settings)
}

func (a *App) deleteMyTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok || scope.OwnerUserID == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	err := a.store.DeleteUserTokenModelAccess(ctx, *scope.OwnerUserID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "user_token_model_access_reset", "self", "user_setting", store.TokenModelAccessSettingKey, map[string]any{"user_id": *scope.OwnerUserID})
	a.refreshTokenPoolSettings(ctx, *scope.OwnerUserID)
	a.writeTokenModelAccessWithSettings(w, r, false, store.TokenModelAccessSettings{PlanModels: map[string][]string{}})
}

func (a *App) updateAdminTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	planModels, err := decodeTokenModelAccessBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	settings, err := a.store.UpsertTokenModelAccess(ctx, planModels)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_model_access_update", "api", "setting", store.TokenModelAccessSettingKey, map[string]any{"plan_models": settings.PlanModels})
	if a.tokens != nil {
		if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
			a.logger.Warn("global token pool refresh after model access update failed", "error", err)
		}
		if _, err := a.tokens.RefreshActiveOwners(ctx); err != nil && a.logger != nil {
			a.logger.Warn("owner token pool refresh after model access update failed", "error", err)
		}
	}
	a.writeTokenModelAccessWithSettings(w, r, true, settings)
}

func (a *App) deleteAdminTokenModelAccess(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	err := a.store.DeleteTokenModelAccess(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "token_model_access_reset", "api", "setting", store.TokenModelAccessSettingKey, nil)
	if a.tokens != nil {
		_ = a.tokens.Refresh(ctx)
		_, _ = a.tokens.RefreshActiveOwners(ctx)
	}
	a.writeTokenModelAccessWithSettings(w, r, true, store.TokenModelAccessSettings{PlanModels: map[string][]string{}})
}

func decodeTokenModelAccessBody(r *http.Request) (map[string][]string, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 128*1024))
	if err != nil {
		return nil, err
	}
	return store.ParseTokenModelAccessPayload(body)
}

func (a *App) writeTokenModelAccess(w http.ResponseWriter, r *http.Request, admin bool) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	settings := store.TokenModelAccessSettings{PlanModels: map[string][]string{}}
	var err error
	if admin {
		settings, err = a.store.GetTokenModelAccess(ctx)
	} else {
		auth := authFromContext(r.Context())
		scope, ok := userScope(w, auth)
		if !ok || scope.OwnerUserID == nil {
			return
		}
		settings, err = a.store.GetUserTokenModelAccess(ctx, *scope.OwnerUserID)
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeTokenModelAccessWithSettings(w, r, admin, settings)
}

func (a *App) writeTokenModelAccessWithSettings(w http.ResponseWriter, r *http.Request, admin bool, settings store.TokenModelAccessSettings) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	var ownerID int64
	if !admin {
		auth := authFromContext(r.Context())
		scope, ok := userScope(w, auth)
		if !ok || scope.OwnerUserID == nil {
			return
		}
		ownerID = *scope.OwnerUserID
	}
	if ownerID > 0 && a.tokens != nil && a.modelCatalog != nil {
		_, _, _ = a.resolveOfficialModelsCatalog(ctx, ownerID, defaultCodexClientVersion)
	}
	representativeOwners := make(map[string]int64)
	if admin && a.tokens != nil {
		for _, runtimeToken := range a.tokens.Snapshot().Ready {
			if runtimeToken == nil || runtimeToken.Token.OwnerUserID <= 0 {
				continue
			}
			plan := store.CanonicalTokenPlan("")
			if runtimeToken.Token.PlanType != nil {
				plan = store.CanonicalTokenPlan(*runtimeToken.Token.PlanType)
			}
			if _, exists := representativeOwners[plan]; !exists {
				representativeOwners[plan] = runtimeToken.Token.OwnerUserID
			}
		}
		if a.modelCatalog != nil {
			var wait sync.WaitGroup
			for plan, representativeOwnerID := range representativeOwners {
				plan := plan
				representativeOwnerID := representativeOwnerID
				wait.Add(1)
				go func() {
					defer wait.Done()
					_, _, _ = a.resolveOfficialModelsPlanCatalog(ctx, representativeOwnerID, plan, defaultCodexClientVersion)
				}()
			}
			wait.Wait()
		}
	}
	countsScope := store.AllResources()
	if ownerID > 0 {
		countsScope = store.OwnerResources(ownerID)
	}
	counts, err := a.store.TokenPlanCountsScoped(ctx, countsScope, store.TokenListOptions{})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	global, err := a.store.GetTokenModelAccess(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if global.PlanModels == nil {
		global.PlanModels = map[string][]string{}
	}
	canonicalCounts := map[string]store.TokenPlanCount{}
	planOrder := make([]string, 0, len(counts)+4)
	for _, item := range counts {
		plan := store.CanonicalTokenPlan(item.Plan)
		current := canonicalCounts[plan]
		if current.Plan == "" {
			current.Plan, current.Label = plan, item.Label
			planOrder = append(planOrder, plan)
		}
		current.Count += item.Count
		canonicalCounts[plan] = current
	}
	for _, plan := range []string{"free", "plus", "team", "pro", "unknown"} {
		if _, exists := canonicalCounts[plan]; !exists {
			canonicalCounts[plan] = store.TokenPlanCount{Plan: plan, Label: tokenModelPlanLabel(plan)}
			planOrder = append(planOrder, plan)
		}
	}
	for plan := range global.PlanModels {
		if _, exists := canonicalCounts[plan]; !exists {
			canonicalCounts[plan] = store.TokenPlanCount{Plan: plan, Label: tokenModelPlanLabel(plan)}
			planOrder = append(planOrder, plan)
		}
	}
	for plan := range settings.PlanModels {
		if _, exists := canonicalCounts[plan]; !exists {
			canonicalCounts[plan] = store.TokenPlanCount{Plan: plan, Label: tokenModelPlanLabel(plan)}
			planOrder = append(planOrder, plan)
		}
	}
	seen := map[string]struct{}{}
	ordered := make([]string, 0, len(planOrder))
	for _, plan := range planOrder {
		if _, ok := seen[plan]; ok {
			continue
		}
		seen[plan] = struct{}{}
		ordered = append(ordered, plan)
	}
	planOrder = ordered
	plans := make([]tokenModelAccessPlan, 0, len(planOrder))
	for _, plan := range planOrder {
		count := canonicalCounts[plan]
		available := modelaccess.ModelIDs()
		capabilityKnown := false
		if ownerID > 0 && a.tokens != nil {
			if detected, ok := a.tokens.ModelsForOwnerPlan(ownerID, plan, defaultCodexClientVersion, time.Now().UTC()); ok {
				available = append([]string(nil), detected...)
				capabilityKnown = true
				if modelaccess.DefaultAllows(plan, "gpt-image-2") {
					available = append(available, "gpt-image-2")
				}
				available, _ = modelaccess.NormalizeModels(available)
			}
		} else if admin && a.tokens != nil {
			if representativeOwnerID := representativeOwners[plan]; representativeOwnerID > 0 {
				if detected, ok := a.tokens.ModelsForOwnerPlan(representativeOwnerID, plan, defaultCodexClientVersion, time.Now().UTC()); ok {
					available = append([]string(nil), detected...)
					capabilityKnown = true
					if modelaccess.DefaultAllows(plan, "gpt-image-2") {
						available = append(available, "gpt-image-2")
					}
					available, _ = modelaccess.NormalizeModels(available)
				}
			}
		}
		inherited := modelaccess.DefaultModels(plan, available)
		if !admin {
			if configured, ok := global.PlanModels[plan]; ok {
				inherited = append([]string(nil), configured...)
			}
		}
		if capabilityKnown {
			inherited = intersectModelIDs(inherited, available)
		}
		effective := inherited
		overrideModels, overridden := settings.PlanModels[plan]
		if overridden {
			effective = append([]string(nil), overrideModels...)
			if capabilityKnown {
				effective = intersectModelIDs(effective, available)
			}
		}
		plans = append(plans, tokenModelAccessPlan{
			Plan: plan, Label: firstNonEmpty(count.Label, plan), TokenCount: count.Count,
			Models: effective, AvailableModels: available, CapabilityKnown: capabilityKnown,
			Inherited: inherited, OverrideModels: append([]string(nil), overrideModels...), Overridden: overridden,
		})
	}
	sort.SliceStable(plans, func(i, j int) bool {
		order := map[string]int{"free": 0, "plus": 1, "team": 2, "pro": 3, "unknown": 4}
		left, lok := order[plans[i].Plan]
		right, rok := order[plans[j].Plan]
		if lok && rok && left != right {
			return left < right
		}
		if lok != rok {
			return lok
		}
		return plans[i].Plan < plans[j].Plan
	})
	modelByID := make(map[string]modelaccess.Model)
	for _, model := range modelaccess.Models() {
		modelByID[model.ID] = model
	}
	for _, plan := range plans {
		for _, modelID := range append(append(append([]string(nil), plan.AvailableModels...), plan.Models...), plan.Inherited...) {
			if _, exists := modelByID[modelID]; !exists {
				modelByID[modelID] = modelaccess.Model{ID: modelID, Label: modelID}
			}
		}
	}
	models := make([]modelaccess.Model, 0, len(modelByID))
	for _, model := range modelByID {
		models = append(models, model)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	writeJSON(w, http.StatusOK, map[string]any{
		"models": models, "plans": plans,
		"plan_models": settings.PlanModels, "updated_at": settings.UpdatedAt,
		"administrator_plan_models": global.PlanModels,
	})
}

func tokenModelPlanLabel(plan string) string {
	switch plan {
	case "free":
		return "Free"
	case "plus":
		return "Plus"
	case "team":
		return "Team"
	case "pro":
		return "Pro"
	case "unknown":
		return "Unknown"
	default:
		return plan
	}
}

func intersectModelIDs(models, available []string) []string {
	allowed := make(map[string]struct{}, len(available))
	for _, model := range available {
		allowed[model] = struct{}{}
	}
	filtered := make([]string, 0, len(models))
	for _, model := range models {
		if _, ok := allowed[model]; ok {
			filtered = append(filtered, model)
		}
	}
	return filtered
}
