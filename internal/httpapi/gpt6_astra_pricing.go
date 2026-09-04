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

const gpt6AstraLongContextThresholdTokens = 272_000

func (a *App) getGPT6AstraLongContextPricing(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	settings, err := a.store.GetGPT6AstraLongContextPricingSettings(ctx)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeGPT6AstraLongContextPricing(w, settings)
}

func (a *App) updateGPT6AstraLongContextPricing(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Enabled *bool `json:"enabled"`
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
	if payload.Enabled == nil {
		writeError(w, http.StatusBadRequest, errors.New("enabled is required"))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	settings, err := a.store.UpdateGPT6AstraLongContextPricingSettings(ctx, *payload.Enabled)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if a.proxy != nil {
		a.proxy.SetGPT6AstraLongContextPricing(settings.Enabled)
	}
	_ = a.store.WriteAuditLog(ctx, "gpt6_astra_long_context_pricing_updated", "admin", "setting", store.GPT6AstraLongContextSettingKey, map[string]any{
		"enabled": settings.Enabled,
	})
	a.writeGPT6AstraLongContextPricing(w, settings)
}

func (a *App) resetGPT6AstraLongContextPricing(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	err := a.store.DeleteGPT6AstraLongContextPricingSettings(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if a.proxy != nil {
		a.proxy.SetGPT6AstraLongContextPricing(false)
	}
	_ = a.store.WriteAuditLog(ctx, "gpt6_astra_long_context_pricing_reset", "admin", "setting", store.GPT6AstraLongContextSettingKey, nil)
	a.writeGPT6AstraLongContextPricing(w, store.GPT6AstraLongContextPricingSettings{})
}

func (a *App) writeGPT6AstraLongContextPricing(w http.ResponseWriter, settings store.GPT6AstraLongContextPricingSettings) {
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":                       settings.Enabled,
		"default_enabled":               false,
		"long_context_threshold_tokens": gpt6AstraLongContextThresholdTokens,
		"input_multiplier":              2,
		"output_multiplier":             1.5,
		"overridden":                    settings.UpdatedAt != nil,
		"updated_at":                    settings.UpdatedAt,
	})
}
