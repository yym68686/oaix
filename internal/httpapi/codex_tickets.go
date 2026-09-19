package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/yym68686/oaix/internal/codexticket"
)

func (a *App) getCodexTickets(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	p, err := a.store.LoadCodexTicketPolicy(ctx)
	if err != nil {
		writeError(w, 503, errors.New("cannot read Codex ticket settings"))
		return
	}
	result := map[string]any{
		"policy": p, "harvest_proxy_url": codexticket.MaskProxy(p.HarvestProxyURL),
		"harvest_proxy_configured": p.HarvestProxyURL != "", "target_length": codexticket.TargetLength,
		"ttl_seconds": int(codexticket.TTL / time.Second), "refresh_before_seconds": int(codexticket.RefreshBefore / time.Second),
		"max_concurrent_probes": codexticket.MaxConcurrentProbes,
	}
	if a.proxy != nil && a.proxy.CodexTickets() != nil {
		result["runtime_policy"] = a.proxy.CodexTickets().Policy()
		result["stats"] = a.proxy.CodexTickets().Stats()
		result["tickets"] = a.proxy.CodexTickets().Statuses()
	}
	writeJSON(w, 200, result)
}

func (a *App) updateCodexTickets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var payload struct {
		Enabled    *bool     `json:"enabled"`
		FailClosed *bool     `json:"fail_closed"`
		Models     *[]string `json:"models"`
		ProxyURL   *string   `json:"harvest_proxy_url"`
		ClearProxy bool      `json:"clear_harvest_proxy"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		writeError(w, 400, errors.New("invalid Codex ticket settings"))
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, 400, errors.New("request body must contain one JSON object"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.UpdateCodexTicketPolicy(ctx, payload.Enabled, payload.FailClosed, payload.Models, payload.ProxyURL, payload.ClearProxy); err != nil {
		// Validation and persistence errors never echo a proxy URL or password.
		writeError(w, 400, errors.New("cannot save Codex ticket settings; check model names and proxy URL"))
		return
	}
	if a.proxy != nil && a.proxy.CodexTickets() != nil {
		_ = a.proxy.CodexTickets().ReloadPolicy(ctx)
	}
	_ = a.store.WriteAuditLog(ctx, "codex_ticket_settings_updated", "admin", "setting", codexticket.SettingKey, map[string]any{"enabled": payload.Enabled, "fail_closed": payload.FailClosed, "models": payload.Models, "proxy_changed": payload.ProxyURL != nil || payload.ClearProxy})
	a.getCodexTickets(w, r)
}
