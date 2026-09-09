package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/store"
)

// All foreign routes and response vocabulary stay at the HTTP boundary.
func (a *App) registerSub2APIQueryRoutes(mux *http.ServeMux) {
	for path, handler := range map[string]http.HandlerFunc{
		"GET /api/v1/admin/accounts":                                   a.sub2APIListAccounts,
		"GET /api/v1/admin/accounts/{id}":                              a.sub2APIGetAccount,
		"GET /api/v1/admin/accounts/data":                              a.sub2APIExportAccounts,
		"GET /api/v1/admin/accounts/{id}/usage":                        a.sub2APIAccountUsage,
		"GET /api/v1/admin/accounts/{id}/stats":                        a.sub2APIAccountStats,
		"GET /api/v1/admin/accounts/{id}/today-stats":                  a.sub2APIAccountStats,
		"POST /api/v1/admin/accounts/usage/batch":                      a.sub2APIBatchQuery,
		"POST /api/v1/admin/accounts/today-stats/batch":                a.sub2APIBatchQuery,
		"GET /api/v1/admin/accounts/{id}/temp-unschedulable":           a.sub2APITempUnschedulable,
		"GET /api/v1/admin/accounts/{id}/models":                       a.sub2APIAccountModels,
		"GET /api/v1/admin/accounts/upstream-billing-rates":            a.sub2APIListAccounts,
		"GET /api/v1/admin/accounts/upstream-billing-probe/settings":   a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/accounts/ollama-cloud-usage/settings":       a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/accounts/{id}/ollama-cloud-usage":           a.sub2APIOllamaPlaceholder,
		"GET /api/v1/admin/accounts/antigravity/default-model-mapping": a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/openai/accounts/{id}/quota":                 a.sub2APIAccountUsage,
		"GET /api/v1/admin/proxies":                                    a.sub2APIProxies,
		"GET /api/v1/admin/proxies/all":                                a.sub2APIProxies,
		"GET /api/v1/admin/proxies/data":                               a.sub2APIProxies,
		"GET /api/v1/admin/proxies/{id}":                               a.sub2APIProxies,
		"GET /api/v1/admin/proxies/{id}/stats":                         a.sub2APIProxies,
		"GET /api/v1/admin/proxies/{id}/accounts":                      a.sub2APIProxies,
		"GET /api/v1/admin/groups":                                     a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/all":                                 a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/usage-summary":                       a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/capacity-summary":                    a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/live-capability":                     a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}":                                a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}/models-list-candidates":         a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}/composite-routes":               a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}/stats":                          a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}/rate-multipliers":               a.sub2APIQueryPlaceholder,
		"GET /api/v1/admin/groups/{id}/api-keys":                       a.sub2APIQueryPlaceholder,
	} {
		mux.HandleFunc(path, a.sub2APIAuth(handler, false))
	}
	// Unknown compatibility paths must never fall through to the HTML SPA.
	mux.HandleFunc("GET /api/v1/admin/", a.sub2APIAuth(func(w http.ResponseWriter, r *http.Request) {
		sub2APIReply(w, 404, nil, "Unknown compatibility API endpoint")
	}, false))
}

func sub2APIPagination(r *http.Request) (int, int) {
	page, size := 1, 20
	if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v > 0 {
		page = v
	}
	raw := r.URL.Query().Get("page_size")
	if raw == "" {
		raw = r.URL.Query().Get("limit")
	}
	if v, err := strconv.Atoi(raw); err == nil && v > 0 && v <= 1000 {
		size = v
	}
	return page, size
}
func sub2APIPage(items any, total, page, size int) map[string]any {
	return map[string]any{"items": items, "total": total, "page": page, "page_size": size, "pages": max(1, (total+size-1)/size)}
}
func sub2APIListOptions(r *http.Request) store.TokenListOptions {
	q := r.URL.Query()
	now := time.Now().UTC()
	opts := store.TokenListOptions{Query: strings.TrimSpace(q.Get("search")), Sort: "name", StatusAsOf: &now}
	if len([]rune(opts.Query)) > 100 {
		opts.Query = string([]rune(opts.Query)[:100])
	}
	switch q.Get("status") {
	case "", "all":
	case "active":
		opts.Status = "ready"
	case "rate_limited":
		opts.Status = "cooling"
		ready := true
		opts.CredentialReady = &ready
	case "temp_unschedulable":
		opts.Status = "backoff"
	case "inactive", "disabled":
		opts.Status = "paused"
	case "error":
		opts.Status = "credential_error"
	default:
		opts.IDs = []int64{0}
	}
	if p := q.Get("platform"); p != "" && p != "openai" {
		opts.IDs = []int64{0}
	}
	if typ := q.Get("type"); typ != "" && typ != "oauth" {
		opts.IDs = []int64{0}
	}
	switch q.Get("sort_by") {
	case "id":
		opts.Sort = "oldest"
	case "created_at", "last_used_at", "status":
		opts.Sort = q.Get("sort_by")
	}
	if q.Get("sort_order") == "desc" {
		switch opts.Sort {
		case "oldest":
			opts.Sort = "newest"
		case "name", "created_at", "last_used_at", "status":
			opts.Sort = "-" + opts.Sort
		}
	}
	// Groups, privacy and foreign scheduler policies have no native equivalent.
	return opts
}

// Page in native bounded chunks; sub2api accepts page_size up to 1000 while
// the native store intentionally caps one read at 500.
func (a *App) sub2APIReadPage(ctx context.Context, owner int64, opts store.TokenListOptions, page, size int) ([]store.Token, int, error) {
	if page > int(^uint(0)>>1)/size {
		return []store.Token{}, 0, errors.New("page is too large")
	}
	opts.Offset = (page - 1) * size
	opts.Limit = min(size, 500)
	items, total, err := a.store.ListTokensScoped(ctx, store.OwnerResources(owner), opts)
	if err != nil {
		return nil, 0, err
	}
	if size > 500 && len(items) == 500 && opts.Offset+len(items) < total {
		opts.Offset += 500
		opts.Limit = size - 500
		more, _, err := a.store.ListTokensScoped(ctx, store.OwnerResources(owner), opts)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, more...)
	}
	if items == nil {
		items = []store.Token{}
	}
	return items, total, nil
}
func (a *App) sub2APIQueryOwner(w http.ResponseWriter, r *http.Request) (int64, bool) {
	owner, err := a.sub2APIImportOwner(r)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account store unavailable")
		return 0, false
	}
	return owner, true
}
func (a *App) sub2APIQueryToken(w http.ResponseWriter, r *http.Request) (*store.Token, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 || id > 2147483647 {
		sub2APIReply(w, 400, nil, "Invalid account ID")
		return nil, false
	}
	owner, ok := a.sub2APIQueryOwner(w, r)
	if !ok {
		return nil, false
	}
	token, err := a.store.GetTokenScoped(r.Context(), store.OwnerResources(owner), id)
	if errors.Is(err, pgx.ErrNoRows) {
		sub2APIReply(w, 404, nil, "Account not found")
		return nil, false
	}
	if err != nil {
		sub2APIReply(w, 503, nil, "Account store unavailable")
		return nil, false
	}
	return token, true
}
func sub2APIAccountName(token store.Token) string {
	return firstNonEmpty(strings.TrimSpace(stringPtr(token.Remark)), stringPtr(token.Email), stringPtr(token.AccountID), fmt.Sprintf("Codex %d", token.ID))
}
func sub2APIAccountState(token store.Token, now time.Time) (string, bool) {
	if !token.IsActive || token.DisabledAt != nil {
		if strings.TrimSpace(stringPtr(token.LastError)) != "" {
			return "error", false
		}
		return "inactive", false
	}
	if token.AccessToken == "" && !token.IsAgentIdentity() {
		return "error", false
	}
	return "active", token.CooldownUntil == nil || !token.CooldownUntil.After(now)
}
func (a *App) sub2APIAccountDTOs(ctx context.Context, owner int64, rows []store.Token, lite bool) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(rows))
	ids := make([]int64, 0, len(rows))
	for _, t := range rows {
		ids = append(ids, t.ID)
	}
	bindings, err := a.store.TokenProxyChannelIDs(ctx, store.OwnerResources(owner), ids)
	if err != nil {
		return nil, err
	}
	caps, err := a.store.GetUserTokenConcurrency(ctx, owner)
	if err != nil {
		return nil, err
	}
	active := a.activeStreamsByTokenID(rows)
	now := time.Now().UTC()
	for _, token := range rows {
		item := sub2APIAccountResponse(sub2APIAccountInput{Name: sub2APIAccountName(token), Notes: token.Remark}, token)
		state, ready := sub2APIAccountState(token, now)
		item["status"] = state
		item["schedulable"] = ready
		// Native last_error may contain upstream payloads. Export a bounded category,
		// never the raw message (which can contain credentials or request contents).
		if state == "error" {
			item["error_message"] = "OAIX account credentials are unavailable or the account was disabled after an error"
		}
		if id := bindings[token.ID]; id > 0 {
			item["proxy_id"] = id
		}
		if cap, ok := caps.ActiveStreamCapForPlan(token.PlanType); ok {
			token.UserActiveStreamCap = &cap
		}
		item["concurrency"] = a.tokens.ActiveStreamCapForToken(token)
		item["current_concurrency"] = active[token.ID]
		item["rate_limit_reset_at"] = nil
		if token.CooldownUntil != nil && token.CooldownUntil.After(now) {
			if store.TokenHasTransientRetryBackoff(token, now) {
				item["temp_unschedulable_until"] = token.CooldownUntil
				item["temp_unschedulable_reason"] = "Temporary upstream retry backoff"
			} else {
				item["rate_limit_reset_at"] = token.CooldownUntil
			}
		}
		if lite {
			delete(item, "groups")
			delete(item, "account_groups")
		}
		out = append(out, item)
	}
	return out, nil
}
func (a *App) sub2APIListAccounts(w http.ResponseWriter, r *http.Request) {
	owner, ok := a.sub2APIQueryOwner(w, r)
	if !ok {
		return
	}
	page, size := sub2APIPagination(r)
	if page > int(^uint(0)>>1)/size {
		sub2APIReply(w, 400, nil, "Page is too large")
		return
	}
	rows, total, err := a.sub2APIReadPage(r.Context(), owner, sub2APIListOptions(r), page, size)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account query unavailable")
		return
	}
	if strings.HasSuffix(r.URL.Path, "/upstream-billing-rates") {
		items := make([]map[string]any, 0, len(rows))
		for _, t := range rows {
			items = append(items, map[string]any{"account_id": t.ID, "snapshot": nil})
		}
		sub2APIReply(w, 200, map[string]any{"items": items, "total": total, "page": page, "page_size": size}, "")
		return
	}
	items, err := a.sub2APIAccountDTOs(r.Context(), owner, rows, r.URL.Query().Get("lite") == "1" || r.URL.Query().Get("lite") == "true")
	if err != nil {
		sub2APIReply(w, 503, nil, "Account details unavailable")
		return
	}
	sub2APIReply(w, 200, sub2APIPage(items, total, page, size), "")
}
func (a *App) sub2APIGetAccount(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	items, err := a.sub2APIAccountDTOs(r.Context(), token.OwnerUserID, []store.Token{*token}, false)
	if err != nil {
		sub2APIReply(w, 503, nil, "Account details unavailable")
		return
	}
	sub2APIReply(w, 200, items[0], "")
}
func (a *App) sub2APITempUnschedulable(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	data := map[string]any{"active": false}
	if store.TokenHasTransientRetryBackoff(*token, time.Now().UTC()) {
		data["active"] = true
		data["state"] = map[string]any{"until_unix": token.CooldownUntil.Unix(), "triggered_at_unix": token.UpdatedAt.Unix(), "status_code": 0, "matched_keyword": "", "rule_index": 0, "error_message": "Temporary upstream retry backoff"}
	}
	sub2APIReply(w, 200, data, "")
}
func (a *App) sub2APIOllamaPlaceholder(w http.ResponseWriter, r *http.Request) {
	token, ok := a.sub2APIQueryToken(w, r)
	if !ok {
		return
	}
	sub2APIReply(w, 200, map[string]any{"account_id": token.ID, "eligible": false, "configured": false, "auto_refresh_enabled": false, "encryption_key_configured": false}, "")
}
