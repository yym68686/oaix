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

func (a *App) adminDashboard(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	opts, err := dashboardOptionsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	payload, err := a.store.UserDashboardScoped(ctx, store.AllResources(), opts)
	if errors.Is(err, store.ErrInvalidDashboardDates) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"dashboard":           payload,
		"current_concurrency": a.proxy.ActiveRequests(),
	})
}

func (a *App) adminConcurrency(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"current_concurrency": a.proxy.ActiveRequests(),
		"generated_at":        time.Now().UTC(),
	})
}

func dashboardOptionsFromRequest(r *http.Request) (store.UserDashboardOptions, error) {
	rangeValue, err := parseUserDashboardRange(r.URL.Query().Get("range"))
	if err != nil {
		return store.UserDashboardOptions{}, err
	}
	timezoneName := strings.TrimSpace(r.URL.Query().Get("timezone"))
	if timezoneName == "" {
		timezoneName = "UTC"
	}
	if len(timezoneName) > 64 {
		return store.UserDashboardOptions{}, errors.New("invalid timezone")
	}
	location, err := time.LoadLocation(timezoneName)
	if err != nil {
		return store.UserDashboardOptions{}, errors.New("invalid timezone")
	}
	opts := store.UserDashboardOptions{
		Now:      time.Now().UTC(),
		Location: location,
		Range:    rangeValue,
	}
	if rangeValue == store.UserDashboardCustom {
		opts.CustomFrom, err = time.ParseInLocation(time.DateOnly, r.URL.Query().Get("from"), location)
		if err != nil {
			return store.UserDashboardOptions{}, errors.New("from must be YYYY-MM-DD")
		}
		opts.CustomTo, err = time.ParseInLocation(time.DateOnly, r.URL.Query().Get("to"), location)
		if err != nil {
			return store.UserDashboardOptions{}, errors.New("to must be YYYY-MM-DD")
		}
	}
	return opts, nil
}
