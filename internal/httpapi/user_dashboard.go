package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) myDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok || scope.OwnerUserID == nil {
		return
	}
	opts, err := dashboardOptionsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	payload, err := a.store.UserDashboardScoped(ctx, scope, opts)
	if errors.Is(err, store.ErrInvalidDashboardDates) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"dashboard":           payload,
		"current_concurrency": a.proxy.ActiveRequestsForOwner(*scope.OwnerUserID),
	})
}

func (a *App) myConcurrency(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	auth := authFromContext(r.Context())
	scope, ok := userScope(w, auth)
	if !ok || scope.OwnerUserID == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"current_concurrency": a.proxy.ActiveRequestsForOwner(*scope.OwnerUserID),
		"generated_at":        time.Now().UTC(),
	})
}

func parseUserDashboardRange(value string) (store.UserDashboardRange, error) {
	rangeValue := store.UserDashboardRange(strings.ToLower(strings.TrimSpace(value)))
	if rangeValue == "" {
		return store.UserDashboardToday, nil
	}
	switch rangeValue {
	case store.UserDashboardToday, store.UserDashboardWeek, store.UserDashboardMonth, store.UserDashboardYear, store.UserDashboardCustom:
		return rangeValue, nil
	default:
		return "", errors.New("range must be today, week, month, year, or custom")
	}
}
