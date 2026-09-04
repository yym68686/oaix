package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) getOrdinary429Cooldown(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	settings, err := a.store.GetOrdinary429CooldownSettings(ctx, a.configuredOrdinary429Cooldown())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeOrdinary429Cooldown(w, settings)
}

func (a *App) updateOrdinary429Cooldown(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		CooldownSeconds *int64 `json:"cooldown_seconds"`
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, errors.New("request body must contain one JSON object"))
		return
	}
	if payload.CooldownSeconds == nil {
		writeError(w, http.StatusBadRequest, errors.New("cooldown_seconds is required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	settings, err := a.store.UpdateOrdinary429CooldownSettings(ctx, *payload.CooldownSeconds)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if a.proxy != nil {
		a.proxy.SetOrdinary429Cooldown(time.Duration(settings.CooldownSeconds) * time.Second)
	}
	_ = a.store.WriteAuditLog(ctx, "ordinary_429_cooldown_updated", "admin", "setting", store.Ordinary429CooldownSettingKey, map[string]any{
		"cooldown_seconds": settings.CooldownSeconds,
	})
	a.writeOrdinary429Cooldown(w, settings)
}

func (a *App) resetOrdinary429Cooldown(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	err := a.store.DeleteOrdinary429CooldownSettings(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	configured := a.configuredOrdinary429Cooldown()
	if a.proxy != nil {
		a.proxy.SetOrdinary429Cooldown(configured)
	}
	_ = a.store.WriteAuditLog(ctx, "ordinary_429_cooldown_reset", "admin", "setting", store.Ordinary429CooldownSettingKey, nil)
	a.writeOrdinary429Cooldown(w, store.Ordinary429CooldownSettings{CooldownSeconds: int64(configured / time.Second)})
}

func (a *App) writeOrdinary429Cooldown(w http.ResponseWriter, settings store.Ordinary429CooldownSettings) {
	configured := a.configuredOrdinary429Cooldown()
	writeJSON(w, http.StatusOK, map[string]any{
		"cooldown_seconds":         settings.CooldownSeconds,
		"default_cooldown_seconds": int64(configured / time.Second),
		"overridden":               settings.UpdatedAt != nil,
		"updated_at":               settings.UpdatedAt,
	})
}

func (a *App) configuredOrdinary429Cooldown() time.Duration {
	if a != nil && a.cfg.TokenPool.DefaultCooldown > 0 {
		seconds := int64(a.cfg.TokenPool.DefaultCooldown / time.Second)
		if resolved, err := store.ParseOrdinary429CooldownSeconds(seconds); err == nil {
			return time.Duration(resolved) * time.Second
		}
	}
	return store.DefaultOrdinary429Cooldown
}
