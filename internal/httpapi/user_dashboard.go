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
	rangeValue, err := parseUserDashboardRange(r.URL.Query().Get("range"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	timezoneName := strings.TrimSpace(r.URL.Query().Get("timezone"))
	if timezoneName == "" {
		timezoneName = "UTC"
	}
	if len(timezoneName) > 64 {
		writeError(w, http.StatusBadRequest, errors.New("invalid timezone"))
		return
	}
	location, err := time.LoadLocation(timezoneName)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("invalid timezone"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	payload, err := a.store.UserDashboardScoped(ctx, scope, store.UserDashboardOptions{
		Now:      time.Now().UTC(),
		Location: location,
		Range:    rangeValue,
	})
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
		return store.UserDashboardMonth, nil
	}
	switch rangeValue {
	case store.UserDashboardToday, store.UserDashboardWeek, store.UserDashboardMonth, store.UserDashboardYear:
		return rangeValue, nil
	default:
		return "", errors.New("range must be today, week, month, or year")
	}
}
