package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/egress"
	"github.com/yym68686/oaix/internal/store"
)

func (a *App) registerProxyRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/proxies", a.requireAuth(a.listMyProxies))
	mux.HandleFunc("POST /api/proxies", a.requireAuth(a.saveMyProxy))
	mux.HandleFunc("POST /api/proxies/parse", a.requireAuth(a.parseMyProxy))
	mux.HandleFunc("POST /api/proxies/{proxy_id}", a.requireAuth(a.saveMyProxy))
	mux.HandleFunc("DELETE /api/proxies/{proxy_id}", a.requireAuth(a.deleteMyProxy))
	mux.HandleFunc("POST /api/proxies/{proxy_id}/test", a.requireAuth(a.testMyProxy))
	for _, prefix := range []string{"/api/tokens/", "/api/admin/tokens/"} {
		mux.HandleFunc("GET "+prefix+"{token_id}/proxy", a.requireAuth(a.tokenProxySettings))
		mux.HandleFunc("POST "+prefix+"{token_id}/proxy", a.requireAuth(a.tokenProxySettings))
	}
}

func proxyError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "账号或代理渠道不存在"})
	case errors.Is(err, store.ErrProxyLimit):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, store.ErrProxyInUse):
		writeError(w, http.StatusConflict, err)
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "代理配置操作失败，请稍后重试"})
	}
}

func (a *App) listMyProxies(w http.ResponseWriter, r *http.Request) {
	scope, ok := a.tokenSelfScope(r.Context(), w, authFromContext(r.Context()))
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, err := a.store.ListProxyChannels(ctx, *scope.OwnerUserID)
	if err != nil {
		proxyError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *App) parseMyProxy(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Proxy string `json:"proxy"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeJSON(w, 400, map[string]any{"detail": "代理请求格式无效"})
		return
	}
	u, err := egress.Parse(payload.Proxy)
	if err != nil {
		writeError(w, 400, err)
		return
	}
	writeJSON(w, 200, map[string]any{"protocol": u.Scheme, "host": u.Hostname(), "port": u.Port(), "has_auth": u.User != nil})
}

func (a *App) saveMyProxy(w http.ResponseWriter, r *http.Request) {
	scope, ok := a.tokenSelfScope(r.Context(), w, authFromContext(r.Context()))
	if !ok {
		return
	}
	var id int64
	if r.PathValue("proxy_id") != "" {
		id, ok = pathInt64(w, r, "proxy_id")
		if !ok {
			return
		}
	}
	var payload struct {
		Name  string `json:"name"`
		Proxy string `json:"proxy"`
	}
	if err := decodeJSON(r, &payload); err != nil {
		writeJSON(w, 400, map[string]any{"detail": "代理请求格式无效"})
		return
	}
	if strings.TrimSpace(payload.Name) == "" || len([]rune(strings.TrimSpace(payload.Name))) > 128 {
		writeJSON(w, 400, map[string]any{"detail": "代理名称需为 1–128 个字符"})
		return
	}
	if id == 0 || strings.TrimSpace(payload.Proxy) != "" {
		if _, err := egress.Parse(payload.Proxy); err != nil {
			writeError(w, 400, err)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	savedID, err := a.store.SaveProxyChannel(ctx, *scope.OwnerUserID, id, payload.Name, payload.Proxy)
	if err != nil {
		proxyError(w, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "proxy_channel_save", proxyAuditActor(authFromContext(ctx)), "proxy_channel", strconv.FormatInt(savedID, 10), map[string]any{"owner_user_id": *scope.OwnerUserID})
	status := http.StatusOK
	if id == 0 {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"id": savedID})
}

func (a *App) deleteMyProxy(w http.ResponseWriter, r *http.Request) {
	scope, ok := a.tokenSelfScope(r.Context(), w, authFromContext(r.Context()))
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "proxy_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.store.DeleteProxyChannel(ctx, *scope.OwnerUserID, id); err != nil {
		proxyError(w, err)
		return
	}
	_ = a.store.WriteAuditLog(ctx, "proxy_channel_delete", proxyAuditActor(authFromContext(ctx)), "proxy_channel", strconv.FormatInt(id, 10), nil)
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *App) tokenProxySettings(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	var scope store.ResourceScope
	if strings.HasPrefix(r.URL.Path, "/api/admin/") {
		if auth == nil || !(auth.IsAdmin || auth.IsService) {
			writeJSON(w, 403, map[string]any{"detail": "Admin role required"})
			return
		}
		scope = auth.resourceScope()
	} else {
		var ok bool
		scope, ok = a.tokenSelfScope(r.Context(), w, auth)
		if !ok {
			return
		}
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetTokenScoped(ctx, scope, id)
	if err != nil {
		proxyError(w, err)
		return
	}
	if r.Method == http.MethodPost {
		var payload struct {
			ProxyChannelID *int64 `json:"proxy_channel_id"`
		}
		if err := decodeJSON(r, &payload); err != nil || payload.ProxyChannelID == nil || *payload.ProxyChannelID < 0 {
			writeJSON(w, 400, map[string]any{"detail": "请选择代理渠道，或传入 0 取消代理"})
			return
		}
		if err := a.store.SetTokenProxyChannel(ctx, scope, id, *payload.ProxyChannelID); err != nil {
			proxyError(w, err)
			return
		}
		_ = a.store.WriteAuditLog(ctx, "token_proxy_update", proxyAuditActor(auth), "token", strconv.FormatInt(id, 10), map[string]any{"proxy_channel_id": *payload.ProxyChannelID})
	}
	channelID, err := a.store.TokenProxyChannelID(ctx, id)
	if err != nil {
		proxyError(w, err)
		return
	}
	items, err := a.store.ListProxyChannels(ctx, token.OwnerUserID)
	if err != nil {
		proxyError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"proxy_channel_id": channelID, "items": items})
}

// Tests use a fixed HTTPS endpoint and no account credentials or caller URL.
// Results are returned as runtime facts, never used to overwrite configuration.
var proxyTestSlots = make(chan struct{}, 8)

func (a *App) testMyProxy(w http.ResponseWriter, r *http.Request) {
	scope, ok := a.tokenSelfScope(r.Context(), w, authFromContext(r.Context()))
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "proxy_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	u, err := a.store.ProxyChannelURL(ctx, *scope.OwnerUserID, id)
	if err != nil {
		proxyError(w, err)
		return
	}
	select {
	case proxyTestSlots <- struct{}{}:
		defer func() { <-proxyTestSlots }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"detail": "代理测试繁忙，请稍后重试"})
		return
	}
	started := time.Now()
	result := map[string]any{"ok": false, "checked_at": started.UTC(), "target": "chatgpt.com", "message": "代理连接失败，请检查地址、认证信息和网络可用性"}
	tr := egress.NewTransport(http.DefaultTransport.(*http.Transport))
	client := &http.Client{Transport: tr, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	req, _ := http.NewRequestWithContext(egress.WithProxy(ctx, u), http.MethodGet, "https://chatgpt.com/cdn-cgi/trace", nil)
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
		result["status_code"] = resp.StatusCode
		result["message"] = "已连接代理，但上游拒绝访问（HTTP " + strconv.Itoa(resp.StatusCode) + "）"
		if resp.StatusCode == http.StatusOK && readErr == nil {
			for _, line := range strings.Split(string(body), "\n") {
				if value, found := strings.CutPrefix(line, "ip="); found {
					if ip, parseErr := netip.ParseAddr(strings.TrimSpace(value)); parseErr == nil {
						result["ok"], result["exit_ip"], result["message"] = true, ip.String(), "代理可用，已成功连接 ChatGPT；账号可用性请在账号详情中测试"
					}
				}
			}
			if result["ok"] == false {
				result["message"] = "代理返回了非预期响应，未能确认出口 IP"
			}
		}
	}
	result["duration_ms"] = time.Since(started).Milliseconds()
	writeJSON(w, 200, result)
}

func proxyAuditActor(auth *AuthContext) string {
	if auth == nil {
		return "unknown"
	}
	if auth.UserID != nil {
		return "user:" + strconv.FormatInt(*auth.UserID, 10)
	}
	if auth.APIKeyID != nil {
		return "api_key:" + strconv.FormatInt(*auth.APIKeyID, 10)
	}
	return auth.PrincipalType
}
