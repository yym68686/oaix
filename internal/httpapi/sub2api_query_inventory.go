package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func sub2APISelectedIDs(r *http.Request) ([]int64, error) {
	values := append([]string{}, r.URL.Query()["ids"]...)
	values = append(values, r.URL.Query()["ids[]"]...)
	parts := []string{}
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				parts = append(parts, part)
			}
		}
	}
	values = parts
	if len(values) == 0 {
		return nil, nil
	}
	ids := []int64{}
	for _, value := range values {
		id, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || id <= 0 || id > 2147483647 {
			return nil, fmt.Errorf("invalid account or proxy ID")
		}
		ids = append(ids, id)
	}
	return ids, nil
}
func (a *App) sub2APIReadAll(ctx context.Context, owner int64, opts store.TokenListOptions) ([]store.Token, error) {
	out := []store.Token{}
	for page := 1; ; page++ {
		rows, total, err := a.sub2APIReadPage(ctx, owner, opts, page, 500)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
		if len(rows) < 500 || len(out) >= total {
			return out, nil
		}
	}
}
func (a *App) sub2APIExportAccounts(w http.ResponseWriter, r *http.Request) {
	owner, ok := a.sub2APIQueryOwner(w, r)
	if !ok {
		return
	}
	opts := sub2APIListOptions(r)
	ids, err := sub2APISelectedIDs(r)
	if err != nil {
		sub2APIReply(w, 400, nil, err.Error())
		return
	}
	if ids != nil {
		opts = store.TokenListOptions{IDs: ids, Sort: "oldest"}
	}
	rows, err := a.sub2APIReadAll(r.Context(), owner, opts)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account export unavailable")
		return
	}
	items, err := a.sub2APIAccountDTOs(r.Context(), owner, rows, false)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account export unavailable")
		return
	}
	proxies := []map[string]any{}
	include := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("include_proxies")))
	if include != "false" && include != "0" && include != "no" && include != "off" {
		channels, err := a.store.ListProxyChannels(r.Context(), owner)
		if err != nil {
			sub2APIReply(w, 503, nil, "Proxy export unavailable")
			return
		}
		used := map[int64]bool{}
		for _, item := range items {
			if id, ok := item["proxy_id"].(int64); ok {
				used[id] = true
				item["proxy_key"] = fmt.Sprintf("oaix-proxy-%d", id)
			}
		}
		for _, p := range channels {
			if used[p.ID] {
				proxies = append(proxies, sub2APIProxyExportDTO(p))
			}
		}
	}
	// Native OAIX export exposes metadata only. The bundle keeps that boundary;
	// unsupported raw-secret backup is explicitly marked instead of invented.
	for _, item := range items {
		for _, key := range []string{"id", "credentials_status", "proxy_id", "proxy_fallback_origin_id", "status", "error_message", "last_used_at", "created_at", "updated_at", "schedulable", "rate_limited_at", "rate_limit_reset_at", "overload_until", "temp_unschedulable_until", "temp_unschedulable_reason", "session_window_start", "session_window_end", "session_window_status", "group_ids", "groups", "account_groups", "current_concurrency", "load_factor"} {
			delete(item, key)
		}
	}
	sub2APIReply(w, 200, sub2APIDataBundle(items, proxies), "")
}
func sub2APIDataBundle(accounts, proxies []map[string]any) map[string]any {
	return map[string]any{"type": "sub2api-data", "version": 1, "exported_at": time.Now().UTC().Format(time.RFC3339), "accounts": accounts, "proxies": proxies, "credentials_redacted": true}
}
func sub2APIProxyDTO(p store.ProxyChannel) map[string]any {
	return map[string]any{"id": p.ID, "name": p.Name, "protocol": p.Protocol, "host": p.Host, "port": p.Port, "username": "", "status": "active", "created_at": p.CreatedAt, "updated_at": p.UpdatedAt, "expires_at": nil, "fallback_mode": "none", "backup_proxy_id": nil, "expiry_warn_days": 0, "account_count": p.AccountCount}
}
func sub2APIProxyExportDTO(p store.ProxyChannel) map[string]any {
	return map[string]any{"proxy_key": fmt.Sprintf("oaix-proxy-%d", p.ID), "name": p.Name, "protocol": p.Protocol, "host": p.Host, "port": p.Port, "status": "active", "fallback_mode": "none"}
}
func (a *App) sub2APIProxies(w http.ResponseWriter, r *http.Request) {
	owner, ok := a.sub2APIQueryOwner(w, r)
	if !ok {
		return
	}
	channels, err := a.store.ListProxyChannels(r.Context(), owner)
	if err != nil {
		sub2APIReply(w, 503, nil, "Proxy store unavailable")
		return
	}
	if raw := r.PathValue("id"); raw != "" {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id <= 0 {
			sub2APIReply(w, 400, nil, "Invalid proxy ID")
			return
		}
		var found *store.ProxyChannel
		for i := range channels {
			if channels[i].ID == id {
				found = &channels[i]
				break
			}
		}
		if found == nil {
			sub2APIReply(w, 404, nil, "Proxy not found")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/accounts") || strings.HasSuffix(r.URL.Path, "/stats") {
			rows, err := a.sub2APIReadAll(r.Context(), owner, store.TokenListOptions{ProxyChannelID: id})
			if err != nil {
				sub2APIReply(w, 503, nil, "Proxy account query unavailable")
				return
			}
			if strings.HasSuffix(r.URL.Path, "/accounts") {
				items := []map[string]any{}
				for _, t := range rows {
					items = append(items, map[string]any{"id": t.ID, "name": sub2APIAccountName(t), "platform": "openai", "type": "oauth", "notes": t.Remark})
				}
				sub2APIReply(w, 200, items, "")
				return
			}
			active := 0
			for _, t := range rows {
				if _, ready := sub2APIAccountState(t, time.Now().UTC()); ready {
					active++
				}
			}
			sub2APIReply(w, 200, map[string]any{"total_accounts": len(rows), "active_accounts": active, "total_requests": 0, "success_rate": 100.0, "average_latency": 0}, "")
			return
		}
		sub2APIReply(w, 200, sub2APIProxyDTO(*found), "")
		return
	}
	q := r.URL.Query()
	selected, err := sub2APISelectedIDs(r)
	if err != nil {
		sub2APIReply(w, 400, nil, err.Error())
		return
	}
	selectedSet := map[int64]bool{}
	for _, id := range selected {
		selectedSet[id] = true
	}
	filtered := []store.ProxyChannel{}
	for _, p := range channels {
		if selected != nil && !selectedSet[p.ID] {
			continue
		}
		if v := q.Get("protocol"); v != "" && v != p.Protocol {
			continue
		}
		if v := q.Get("status"); v != "" && v != "active" {
			continue
		}
		if search := strings.ToLower(strings.TrimSpace(q.Get("search"))); search != "" && !strings.Contains(strings.ToLower(p.Name+" "+p.Host), search) {
			continue
		}
		filtered = append(filtered, p)
	}
	sort.Slice(filtered, func(i, j int) bool {
		less := filtered[i].ID < filtered[j].ID
		if q.Get("sort_by") == "name" && filtered[i].Name != filtered[j].Name {
			less = filtered[i].Name < filtered[j].Name
		}
		if q.Get("sort_order") != "asc" {
			return !less
		}
		return less
	})
	if strings.HasSuffix(r.URL.Path, "/data") {
		items := []map[string]any{}
		for _, p := range filtered {
			items = append(items, sub2APIProxyExportDTO(p))
		}
		sub2APIReply(w, 200, sub2APIDataBundle([]map[string]any{}, items), "")
		return
	}
	items := []map[string]any{}
	for _, p := range filtered {
		items = append(items, sub2APIProxyDTO(p))
	}
	if strings.HasSuffix(r.URL.Path, "/all") {
		sub2APIReply(w, 200, items, "")
		return
	}
	page, size := sub2APIPagination(r)
	start := len(items)
	if page <= len(items)/size+1 {
		start = min(len(items), (page-1)*size)
	}
	end := min(len(items), start+size)
	sub2APIReply(w, 200, sub2APIPage(items[start:end], len(items), page, size), "")
}
func (a *App) sub2APIQueryPlaceholder(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	var data any = []any{}
	switch {
	case strings.HasSuffix(path, "/upstream-billing-probe/settings"):
		data = map[string]any{"enabled": false, "interval_minutes": 0}
	case strings.HasSuffix(path, "/ollama-cloud-usage/settings"):
		data = map[string]any{"enabled": false, "interval_minutes": 0, "debounce_minutes": 0}
	case strings.HasSuffix(path, "/default-model-mapping"):
		data = map[string]string{}
	case strings.HasSuffix(path, "/live-capability"):
		data = map[string]any{"supported": false, "reason": "Live attestation is not available in OAIX"}
	case strings.HasSuffix(path, "/models-list-candidates"):
		data = map[string]any{"models": []string{}}
	case strings.HasSuffix(path, "/stats"):
		data = map[string]any{"total_api_keys": 0, "active_api_keys": 0, "total_requests": 0, "total_cost": 0.0}
	case path == "/api/v1/admin/groups" || strings.HasSuffix(path, "/api-keys"):
		page, size := sub2APIPagination(r)
		data = sub2APIPage([]any{}, 0, page, size)
	case r.PathValue("id") != "" && strings.HasSuffix(path, "/"+r.PathValue("id")):
		id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
		data = sub2APIGroupPlaceholder(id)
	}
	sub2APIReply(w, 200, data, "")
}

// Preserve the source group's required field types without introducing a group
// model or granting any of its optional capabilities.
func sub2APIGroupPlaceholder(id int64) map[string]any {
	data := map[string]any{"id": id, "name": "", "description": "", "platform": "openai", "status": "inactive", "subscription_type": "standard", "created_at": time.Time{}, "updated_at": time.Time{}, "account_count": 0, "sort_order": 0, "rpm_limit": 0}
	for _, key := range []string{"rate_multiplier", "image_rate_multiplier", "batch_image_discount_multiplier", "batch_image_hold_multiplier", "video_rate_multiplier", "peak_rate_multiplier"} {
		data[key] = 1.0
	}
	for _, key := range []string{"daily_limit_usd", "weekly_limit_usd", "monthly_limit_usd", "image_price_1k", "image_price_2k", "image_price_4k", "video_price_480p", "video_price_720p", "video_price_1080p", "web_search_price_per_call", "search_price_per_1k", "audio_realtime_price_per_min", "audio_tts_price_per_million_chars", "audio_stt_price_per_hour", "fallback_group_id", "fallback_group_id_on_invalid_request"} {
		data[key] = nil
	}
	for _, key := range []string{"is_exclusive", "long_context_pricing_enabled", "allow_image_generation", "allow_batch_image_generation", "image_rate_independent", "video_rate_independent", "peak_rate_enabled", "claude_code_only", "allow_messages_dispatch", "allow_live", "require_oauth_only", "require_privacy_set", "force_openai_fast", "free_openai_fast", "profit_control_enabled", "model_routing_enabled", "mcp_xml_inject"} {
		data[key] = false
	}
	for _, key := range []string{"peak_start", "peak_end", "max_reasoning_effort", "max_reasoning_effort_over_limit", "default_mapped_model"} {
		data[key] = ""
	}
	for _, key := range []string{"profit_min_margin", "profit_safety_buffer"} {
		data[key] = 0.0
	}
	for _, key := range []string{"reasoning_effort_mappings", "model_pricing", "supported_model_scopes", "account_groups"} {
		data[key] = []any{}
	}
	for _, key := range []string{"model_routing", "messages_dispatch_model_config", "models_list_config", "codex_models_manifest_config"} {
		data[key] = map[string]any{}
	}
	return data
}
