package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/store"
)

const sub2APIImportMaxBytes = 32 << 20
const sub2APIImportMaxAccounts = 1000

func (a *App) registerSub2APIImportRoutes(mux *http.ServeMux) {
	for path, handler := range map[string]http.HandlerFunc{
		"POST /api/v1/admin/accounts":                      a.sub2APICreateAccount,
		"POST /api/v1/admin/accounts/batch":                a.sub2APIBatchAccounts,
		"POST /api/v1/admin/accounts/data":                 a.sub2APIImportData,
		"POST /api/v1/admin/accounts/import/codex-session": a.sub2APIImportSession,
	} {
		mux.HandleFunc(path, a.sub2APIImportAuth(handler))
	}
}

func sub2APIReply(w http.ResponseWriter, status int, data any, message string) {
	code := status
	if status < 400 {
		code = 0
		message = "success"
	}
	reply := map[string]any{"code": code, "message": message}
	if data != nil {
		reply["data"] = data
	}
	writeJSON(w, status, reply)
}

func (a *App) sub2APIImportAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// The foreign admin-key header carries an existing OAIX key. Match the
		// source API's x-api-key precedence without changing native authentication.
		authRequest := r.Clone(r.Context())
		if key := strings.TrimSpace(r.Header.Get("X-API-Key")); key != "" {
			authRequest.Header.Del("Authorization")
		}
		auth, ok := a.authenticateRequest(authRequest)
		if !ok {
			sub2APIReply(w, 401, nil, "Invalid or missing admin API key")
			return
		}
		if !sub2APIImportAllowed(auth) {
			sub2APIReply(w, 403, nil, "Writable, active OAIX API key required")
			return
		}
		if err := a.applyActAsUser(r.Context(), r, auth); err != nil {
			sub2APIReply(w, 403, nil, "Invalid account owner scope")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		next(w, withAuthContext(r.WithContext(ctx), auth))
	}
}

func sub2APIImportAllowed(auth *AuthContext) bool {
	return auth != nil && (auth.IsAdmin || auth.IsService || (auth.UserID != nil && *auth.UserID > 0)) && !auth.ReadOnly && !blockedStatus(auth)
}

func sub2APIDecode(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, sub2APIImportMaxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	err := decoder.Decode(dst)
	if err == nil {
		var extra any
		if err = decoder.Decode(&extra); errors.Is(err, io.EOF) {
			return true
		}
	}
	status := http.StatusBadRequest
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		status = http.StatusRequestEntityTooLarge
	}
	sub2APIReply(w, status, nil, "Invalid JSON request body")
	return false
}

func (a *App) sub2APIImportOwner(r *http.Request) (int64, error) {
	auth := authFromContext(r.Context())
	if auth != nil && auth.ActAsUserID != nil {
		return *auth.ActAsUserID, nil
	}
	if auth != nil && !auth.IsService && auth.UserID != nil && *auth.UserID > 0 {
		return *auth.UserID, nil
	}
	if a.store == nil {
		return 0, errors.New("store unavailable")
	}
	return a.store.BootstrapUserID(r.Context())
}

func (a *App) sub2APIImportAccounts(ctx context.Context, ownerID int64, accounts []sub2APIAccountInput) ([]sub2APISessionItem, map[int64]store.Token, error) {
	items := make([]sub2APISessionItem, len(accounts))
	payloads := make([]map[string]any, 0, len(accounts))
	positions := make([]int, 0, len(accounts))
	for i, item := range accounts {
		items[i] = sub2APISessionItem{Index: i + 1, Name: item.Name, Action: "failed"}
		payload, err := item.nativePayload()
		if err == nil && item.ProxyID != nil && *item.ProxyID != 0 {
			if *item.ProxyID < 0 {
				err = errors.New("invalid proxy channel id")
			} else if _, lookupErr := a.store.ProxyChannelURL(ctx, ownerID, *item.ProxyID); lookupErr != nil {
				err = errors.New("proxy channel does not belong to the account owner or is unavailable")
			}
		}
		if err != nil {
			items[i].Message = err.Error()
			continue
		}
		payloads = append(payloads, payload)
		positions = append(positions, i)
	}
	tokens := map[int64]store.Token{}
	if len(payloads) == 0 {
		return items, tokens, nil
	}
	// Reuse native validation, owner-scoped identity matching, persistence, audit,
	// and snapshot publication. No foreign job/state machine is created.
	_, result, err := a.completeTokenImportForOwner(ctx, ownerID, payloads, "front", "admin_api", "api")
	if err != nil {
		return nil, nil, errors.New("account import could not be completed")
	}
	for _, token := range result.Tokens {
		tokens[token.ID] = token
	}
	for _, item := range result.Items {
		if item.Index < 0 || item.Index >= len(positions) {
			return nil, nil, errors.New("invalid native import result")
		}
		out := &items[positions[item.Index]]
		out.Action = item.Action
		if item.TokenID != nil {
			out.AccountID = *item.TokenID
		}
		// Upstream refresh/database errors can include credentials. Do not echo
		// those errors through a foreign response schema.
		if item.Action == "failed" {
			out.Message = "Codex credentials could not be imported"
		}
	}
	return items, tokens, nil
}

func (a *App) sub2APICreateAccount(w http.ResponseWriter, r *http.Request) {
	var req sub2APIAccountInput
	if !sub2APIDecode(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		sub2APIReply(w, 400, nil, "name is required")
		return
	}
	if _, err := req.nativePayload(); err != nil {
		sub2APIReply(w, 400, nil, err.Error())
		return
	}
	owner, err := a.sub2APIImportOwner(r)
	if err != nil {
		sub2APIReply(w, 503, nil, "account store unavailable")
		return
	}
	items, tokens, err := a.sub2APIImportAccounts(r.Context(), owner, []sub2APIAccountInput{req})
	if err != nil {
		sub2APIReply(w, 503, nil, err.Error())
		return
	}
	item := items[0]
	if item.Action == "failed" {
		sub2APIReply(w, 400, nil, item.Message)
		return
	}
	token, ok := tokens[item.AccountID]
	if !ok {
		sub2APIReply(w, 503, nil, "account import result unavailable")
		return
	}
	proxyID, err := a.store.TokenProxyChannelID(r.Context(), token.ID)
	if err != nil {
		sub2APIReply(w, 503, nil, "account proxy configuration unavailable")
		return
	}
	req.ProxyID = nil
	if proxyID > 0 {
		req.ProxyID = &proxyID
	}
	sub2APIReply(w, 200, sub2APIAccountResponse(req, token), "")
}

func sub2APIPlaceholder(value json.RawMessage, fallback any) any {
	if len(value) == 0 || string(value) == "null" {
		return fallback
	}
	return value
}

func sub2APIAccountResponse(req sub2APIAccountInput, token store.Token) map[string]any {
	credentials := map[string]any{}
	if token.Email != nil {
		credentials["email"] = *token.Email
	}
	if token.AccountID != nil {
		credentials["chatgpt_account_id"] = *token.AccountID
	}
	if token.PlanType != nil {
		credentials["plan_type"] = *token.PlanType
	}
	status := "inactive"
	if token.IsActive {
		status = "active"
	}
	return map[string]any{
		"id": token.ID, "name": req.Name, "notes": req.Notes, "platform": "openai", "type": "oauth",
		"credentials": credentials, "credentials_status": map[string]bool{
			"has_access_token": token.AccessToken != "", "has_refresh_token": !token.IsAccessTokenOnly() && !token.IsAgentIdentity() && token.RefreshToken != "",
			"has_id_token": req.Credentials["id_token"] != nil, "has_agent_private_key": token.IsAgentIdentity(),
		},
		"extra": map[string]any{}, "proxy_id": req.ProxyID, "proxy_fallback_origin_id": nil,
		"concurrency": sub2APIPlaceholder(req.Concurrency, 0), "priority": sub2APIPlaceholder(req.Priority, 0),
		"rate_multiplier": sub2APIPlaceholder(req.RateMultiplier, 1), "load_factor": sub2APIPlaceholder(req.LoadFactor, nil),
		"status": status, "error_message": "", "last_used_at": token.LastUsedAt,
		"expires_at": sub2APIPlaceholder(req.ExpiresAt, nil), "auto_pause_on_expired": sub2APIPlaceholder(req.AutoPauseOnExpired, true),
		"created_at": token.CreatedAt, "updated_at": token.UpdatedAt, "schedulable": token.IsActive,
		"rate_limited_at": nil, "rate_limit_reset_at": token.CooldownUntil, "overload_until": nil,
		"temp_unschedulable_until": nil, "temp_unschedulable_reason": "",
		"session_window_start": nil, "session_window_end": nil, "session_window_status": "",
		"group_ids": []int64{}, "groups": []any{}, "account_groups": []any{},
	}
}

func (a *App) sub2APIBatchAccounts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Accounts []sub2APIAccountInput `json:"accounts"`
	}
	if !sub2APIDecode(w, r, &req) {
		return
	}
	if len(req.Accounts) == 0 || len(req.Accounts) > sub2APIImportMaxAccounts {
		sub2APIReply(w, 400, nil, "accounts must contain 1 to 1000 entries")
		return
	}
	owner, err := a.sub2APIImportOwner(r)
	if err != nil {
		sub2APIReply(w, 503, nil, "account store unavailable")
		return
	}
	items, _, err := a.sub2APIImportAccounts(r.Context(), owner, req.Accounts)
	if err != nil {
		sub2APIReply(w, 503, nil, err.Error())
		return
	}
	success := 0
	results := make([]map[string]any, 0, len(items))
	for _, item := range items {
		entry := map[string]any{"name": item.Name, "success": item.Action != "failed"}
		if item.Action == "failed" {
			entry["error"] = item.Message
		} else {
			success++
			entry["id"] = item.AccountID
		}
		results = append(results, entry)
	}
	sub2APIReply(w, 200, map[string]any{"success": success, "failed": len(items) - success, "results": results}, "")
}

func (a *App) sub2APIImportSession(w http.ResponseWriter, r *http.Request) {
	var req sub2APISessionRequest
	if !sub2APIDecode(w, r, &req) {
		return
	}
	entries, err := sub2APISessionEntries(req)
	if err != nil {
		sub2APIReply(w, 400, nil, err.Error())
		return
	}
	if len(entries) > sub2APIImportMaxAccounts {
		sub2APIReply(w, 400, nil, "at most 1000 accounts per import")
		return
	}
	accounts := make([]sub2APIAccountInput, 0, len(entries))
	for i, entry := range entries {
		raw, _ := entry.(map[string]any)
		if text, ok := entry.(string); ok {
			raw = map[string]any{"access_token": text}
		}
		name := stringFromImportPayload(raw, "name", "email")
		if user, ok := raw["user"].(map[string]any); ok && name == "" {
			name = stringFromImportPayload(user, "name", "email")
		}
		if req.Name != "" {
			name = req.Name
			if len(entries) > 1 {
				name = fmt.Sprintf("%s-%d", req.Name, i+1)
			}
		}
		if name == "" {
			name = fmt.Sprintf("codex-import-%d", i+1)
		}
		account := sub2APIAccountInput{Name: name, Notes: req.Notes, Platform: "openai", Type: "oauth", Credentials: raw, ProxyID: req.ProxyID}
		// Session input does not make a non-Codex platform supported.
		if platform := stringFromImportPayload(raw, "platform"); platform != "" && platform != "openai" {
			account.Platform = platform
		}
		if kind := stringFromImportPayload(raw, "type"); kind != "" && kind != "codex" && kind != "oauth" {
			account.Type = kind
		}
		accounts = append(accounts, account)
	}
	owner, err := a.sub2APIImportOwner(r)
	if err != nil {
		sub2APIReply(w, 503, nil, "account store unavailable")
		return
	}
	items, _, err := a.sub2APIImportAccounts(r.Context(), owner, accounts)
	if err != nil {
		sub2APIReply(w, 503, nil, err.Error())
		return
	}
	result := sub2APISessionResult{Total: len(items), Items: items}
	for _, item := range items {
		switch item.Action {
		case "created":
			result.Created++
		case "updated":
			result.Updated++
		case "skipped":
			result.Skipped++
		default:
			result.Failed++
			result.Errors = append(result.Errors, sub2APIImportMessage{Index: item.Index, Name: item.Name, Message: item.Message})
		}
	}
	sub2APIReply(w, 200, result, "")
}

type sub2APIDataProxy struct {
	ProxyKey string `json:"proxy_key"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type sub2APIDataError struct {
	Kind     string `json:"kind"`
	Name     string `json:"name,omitempty"`
	ProxyKey string `json:"proxy_key,omitempty"`
	Message  string `json:"message"`
}

type sub2APIDataResult struct {
	ProxyCreated   int                `json:"proxy_created"`
	ProxyReused    int                `json:"proxy_reused"`
	ProxyFailed    int                `json:"proxy_failed"`
	AccountCreated int                `json:"account_created"`
	AccountFailed  int                `json:"account_failed"`
	Errors         []sub2APIDataError `json:"errors,omitempty"`
}

func (a *App) sub2APIImportData(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Data struct {
			Type     string                `json:"type"`
			Version  int                   `json:"version"`
			Proxies  []sub2APIDataProxy    `json:"proxies"`
			Accounts []sub2APIAccountInput `json:"accounts"`
		} `json:"data"`
	}
	if !sub2APIDecode(w, r, &req) {
		return
	}
	if (req.Data.Type != "" && req.Data.Type != "sub2api-data" && req.Data.Type != "sub2api-bundle") || (req.Data.Version != 0 && req.Data.Version != 1) {
		sub2APIReply(w, 400, nil, "unsupported data type or version")
		return
	}
	if req.Data.Accounts == nil || req.Data.Proxies == nil || len(req.Data.Accounts) > sub2APIImportMaxAccounts || len(req.Data.Proxies) > 1000 {
		sub2APIReply(w, 400, nil, "data.accounts and data.proxies are required (at most 1000 each)")
		return
	}
	// Empty imports are capability checks and create no import jobs or accounts.
	if len(req.Data.Accounts) == 0 && len(req.Data.Proxies) == 0 {
		sub2APIReply(w, 200, sub2APIDataResult{}, "")
		return
	}
	owner, err := a.sub2APIImportOwner(r)
	if err != nil {
		sub2APIReply(w, 503, nil, "account store unavailable")
		return
	}
	result := sub2APIDataResult{}
	proxyIDs, err := a.sub2APIImportProxies(r.Context(), owner, req.Data.Proxies, &result)
	if err != nil {
		sub2APIReply(w, 503, nil, "proxy store unavailable")
		return
	}
	accounts := make([]sub2APIAccountInput, 0, len(req.Data.Accounts))
	for _, account := range req.Data.Accounts {
		// Data-format IDs are foreign identifiers, resolved only through proxy_key.
		account.ProxyID = nil
		if account.ProxyKey != nil && *account.ProxyKey != "" {
			id, ok := proxyIDs[*account.ProxyKey]
			if !ok {
				result.AccountFailed++
				result.Errors = append(result.Errors, sub2APIDataError{Kind: "account", Name: account.Name, Message: "referenced proxy could not be imported"})
				continue
			}
			account.ProxyID = &id
		}
		accounts = append(accounts, account)
	}
	items, _, err := a.sub2APIImportAccounts(r.Context(), owner, accounts)
	if err != nil {
		sub2APIReply(w, 503, nil, err.Error())
		return
	}
	for _, item := range items {
		if item.Action == "failed" {
			result.AccountFailed++
			result.Errors = append(result.Errors, sub2APIDataError{Kind: "account", Name: item.Name, Message: item.Message})
		} else {
			// Source data imports expose no updated counter; this counts successful
			// imports, including native identity upserts, without inventing new IDs.
			result.AccountCreated++
		}
	}
	sub2APIReply(w, 200, result, "")
}

func (a *App) sub2APIImportProxies(ctx context.Context, owner int64, proxies []sub2APIDataProxy, result *sub2APIDataResult) (map[string]int64, error) {
	ids := map[string]int64{}
	if len(proxies) == 0 {
		return ids, nil
	}
	existing, err := a.store.ListProxyChannels(ctx, owner)
	if err != nil {
		return nil, err
	}
	byURL := map[string]int64{}
	for _, item := range existing {
		u, err := a.store.ProxyChannelURL(ctx, owner, item.ID)
		if err != nil {
			return nil, err
		}
		byURL[u.String()] = item.ID
	}
	for _, item := range proxies {
		u := &url.URL{Scheme: strings.ToLower(strings.TrimSpace(item.Protocol)), Host: net.JoinHostPort(strings.Trim(strings.TrimSpace(item.Host), "[]"), strconv.Itoa(item.Port))}
		if item.Username != "" || item.Password != "" {
			u.User = url.UserPassword(item.Username, item.Password)
		}
		normalized, err := egress.Parse(u.String())
		if err != nil {
			result.ProxyFailed++
			result.Errors = append(result.Errors, sub2APIDataError{Kind: "proxy", Name: item.Name, Message: "invalid proxy configuration"})
			continue
		}
		if id, ok := byURL[normalized.String()]; ok {
			ids[item.ProxyKey] = id
			result.ProxyReused++
			continue
		}
		id, err := a.store.SaveProxyChannel(ctx, owner, 0, item.Name, normalized.String())
		if err != nil {
			result.ProxyFailed++
			result.Errors = append(result.Errors, sub2APIDataError{Kind: "proxy", Name: item.Name, Message: "proxy could not be imported"})
			continue
		}
		byURL[normalized.String()] = id
		ids[item.ProxyKey] = id
		result.ProxyCreated++
	}
	return ids, nil
}
