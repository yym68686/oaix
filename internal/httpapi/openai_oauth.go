package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/oauth"
)

const (
	openAIOAuthAuthorizeURL = "https://auth.openai.com/oauth/authorize"
	openAIOAuthSessionTTL   = 15 * time.Minute
	openAIOAuthDefaultScope = "openid profile email offline_access"
)

type openAIOAuthSession struct {
	ID            string
	State         string
	CodeVerifier  string
	RedirectURI   string
	QueuePosition string
	CreatedAt     time.Time
}

type openAIOAuthSessionStore struct {
	mu      sync.Mutex
	byState map[string]openAIOAuthSession
}

func newOpenAIOAuthSessionStore() *openAIOAuthSessionStore {
	return &openAIOAuthSessionStore{byState: make(map[string]openAIOAuthSession)}
}

func (s *openAIOAuthSessionStore) put(session openAIOAuthSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupLocked(time.Now())
	s.byState[session.State] = session
}

func (s *openAIOAuthSessionStore) pop(state string) (openAIOAuthSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.cleanupLocked(now)
	session, ok := s.byState[state]
	if !ok {
		return openAIOAuthSession{}, false
	}
	delete(s.byState, state)
	if now.Sub(session.CreatedAt) > openAIOAuthSessionTTL {
		return openAIOAuthSession{}, false
	}
	if subtle.ConstantTimeCompare([]byte(session.State), []byte(state)) != 1 {
		return openAIOAuthSession{}, false
	}
	return session, true
}

func (s *openAIOAuthSessionStore) cleanupLocked(now time.Time) {
	for state, session := range s.byState {
		if now.Sub(session.CreatedAt) > openAIOAuthSessionTTL {
			delete(s.byState, state)
		}
	}
}

func (a *App) startOpenAIOAuth(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var body struct {
		RedirectURI         string `json:"redirect_uri"`
		ImportQueuePosition string `json:"import_queue_position"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	queuePosition := normalizeImportQueuePosition(body.ImportQueuePosition)
	redirectURI := a.oauthRedirectURI(r, body.RedirectURI)
	state, err := randomHex(32)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	sessionID, err := randomHex(16)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	codeVerifier, err := randomHex(64)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	authURL, err := a.openAIAuthorizationURL(redirectURI, state, codeVerifier)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.oauth.put(openAIOAuthSession{
		ID:            sessionID,
		State:         state,
		CodeVerifier:  codeVerifier,
		RedirectURI:   redirectURI,
		QueuePosition: queuePosition,
		CreatedAt:     time.Now(),
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"auth_url":       authURL,
		"session_id":     sessionID,
		"state":          state,
		"redirect_uri":   redirectURI,
		"expires_in_sec": int(openAIOAuthSessionTTL.Seconds()),
	})
}

func (a *App) openAIOAuthCallback(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if value := strings.TrimSpace(query.Get("error")); value != "" {
		a.redirectOAuthResult(w, r, 0, fmt.Sprintf("%s %s", value, query.Get("error_description")))
		return
	}
	state := strings.TrimSpace(query.Get("state"))
	code := strings.TrimSpace(query.Get("code"))
	if state == "" || code == "" {
		a.redirectOAuthResult(w, r, 0, "OAuth 回调缺少 code 或 state")
		return
	}
	session, ok := a.oauth.pop(state)
	if !ok {
		a.redirectOAuthResult(w, r, 0, "OAuth 会话已过期，请重新授权")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	client := oauth.NewHTTPClient(a.cfg.Upstream.OAuthTokenURL)
	client.ClientID = a.cfg.Upstream.OAuthClientID
	result, err := client.ExchangeAuthorizationCode(ctx, oauth.AuthorizationCodeRequest{
		Code:         code,
		RedirectURI:  session.RedirectURI,
		CodeVerifier: session.CodeVerifier,
	})
	if err != nil {
		a.redirectOAuthResult(w, r, 0, sanitizeImportError(err))
		return
	}
	if strings.TrimSpace(result.RefreshToken) == "" {
		a.redirectOAuthResult(w, r, 0, "OAuth 返回缺少 refresh_token")
		return
	}
	payload := map[string]any{
		"access_token":  result.AccessToken,
		"refresh_token": result.RefreshToken,
		"source":        "chatgpt_oauth",
		"type":          "codex",
	}
	if result.IDToken != "" {
		payload["id_token"] = result.IDToken
	}
	if result.AccountID != "" {
		payload["account_id"] = result.AccountID
		payload["chatgpt_account_id"] = result.AccountID
	}
	if result.Email != "" {
		payload["email"] = result.Email
	}
	if result.PlanType != "" {
		payload["plan_type"] = result.PlanType
	}
	job, _, err := a.completeTokenImport(ctx, []map[string]any{payload}, session.QueuePosition, "chatgpt_oauth", "oauth")
	if err != nil {
		a.redirectOAuthResult(w, r, 0, sanitizeImportError(err))
		return
	}
	a.redirectOAuthResult(w, r, job.ID, "")
}

func (a *App) redirectOAuthResult(w http.ResponseWriter, r *http.Request, jobID int64, errorMessage string) {
	values := url.Values{}
	if jobID > 0 {
		values.Set("oauth_job", fmt.Sprintf("%d", jobID))
	} else {
		values.Set("oauth_error", strings.TrimSpace(errorMessage))
	}
	target := "/imports"
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (a *App) openAIAuthorizationURL(redirectURI string, state string, codeVerifier string) (string, error) {
	clientID := strings.TrimSpace(a.cfg.Upstream.OAuthClientID)
	if clientID == "" {
		return "", errors.New("oauth client id is not configured")
	}
	challenge := codeChallenge(codeVerifier)
	values := url.Values{}
	values.Set("response_type", "code")
	values.Set("client_id", clientID)
	values.Set("redirect_uri", redirectURI)
	values.Set("scope", openAIAuthorizationScope(a.cfg.Upstream.OAuthScope))
	values.Set("state", state)
	values.Set("code_challenge", challenge)
	values.Set("code_challenge_method", "S256")
	values.Set("id_token_add_organizations", "true")
	values.Set("codex_cli_simplified_flow", "true")
	return openAIOAuthAuthorizeURL + "?" + values.Encode(), nil
}

func (a *App) oauthRedirectURI(r *http.Request, requested string) string {
	base := publicOrigin(r)
	fallback := base + "/auth/callback"
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return fallback
	}
	parsed, err := url.Parse(requested)
	if err != nil || !parsed.IsAbs() || parsed.Path != "/auth/callback" {
		return fallback
	}
	origin, err := url.Parse(base)
	if err != nil || !strings.EqualFold(parsed.Host, origin.Host) {
		return fallback
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fallback
	}
	return parsed.Scheme + "://" + parsed.Host + "/auth/callback"
}

func publicOrigin(r *http.Request) string {
	proto := firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))
	host := firstForwardedValue(r.Header.Get("X-Forwarded-Host"))
	if proto == "" || host == "" {
		if forwardedProto, forwardedHost := parseForwardedHeader(r.Header.Get("Forwarded")); proto == "" || host == "" {
			if proto == "" {
				proto = forwardedProto
			}
			if host == "" {
				host = forwardedHost
			}
		}
	}
	if host == "" {
		host = r.Host
	}
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else if shouldDefaultHTTPS(host) {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	return strings.TrimRight(proto, "/") + "://" + host
}

func firstForwardedValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ",")
	return strings.TrimSpace(parts[0])
}

func parseForwardedHeader(value string) (string, string) {
	value = firstForwardedValue(value)
	if value == "" {
		return "", ""
	}
	var proto string
	var host string
	for _, part := range strings.Split(value, ";") {
		key, val, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		val = strings.Trim(strings.TrimSpace(val), `"`)
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "proto":
			proto = val
		case "host":
			host = val
		}
	}
	return proto, host
}

func shouldDefaultHTTPS(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host != "" &&
		!strings.HasPrefix(host, "localhost") &&
		!strings.HasPrefix(host, "127.") &&
		!strings.HasPrefix(host, "[::1]") &&
		!strings.HasPrefix(host, "::1")
}

func normalizeImportQueuePosition(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "back") {
		return "back"
	}
	return "front"
}

func openAIAuthorizationScope(scope string) string {
	fields := strings.Fields(scope)
	if len(fields) == 0 {
		return openAIOAuthDefaultScope
	}
	seen := make(map[string]struct{}, len(fields)+1)
	out := make([]string, 0, len(fields)+1)
	for _, field := range fields {
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	if _, ok := seen["offline_access"]; !ok {
		out = append(out, "offline_access")
	}
	return strings.Join(out, " ")
}

func randomHex(size int) (string, error) {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func codeChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
