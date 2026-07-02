package oauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type RefreshResult struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	ExpiresIn    int
	AccountID    string
	Email        string
	PlanType     string
}

type AuthorizationCodeRequest struct {
	Code         string
	RedirectURI  string
	CodeVerifier string
}

type IDTokenIdentity struct {
	AccountID      string
	Email          string
	PlanType       string
	UserID         string
	OrganizationID string
}

type Client interface {
	Refresh(ctx context.Context, refreshToken string) (RefreshResult, error)
}

type ClientIDRefresher interface {
	RefreshWithClientID(ctx context.Context, refreshToken string, clientID string) (RefreshResult, error)
}

type HTTPClient struct {
	TokenURL   string
	ClientID   string
	Scope      string
	HTTPClient *http.Client
	MaxRetries int
	metrics    Metrics
}

type Metrics struct {
	RefreshAttempts int64 `json:"refresh_attempts"`
	RefreshSuccess  int64 `json:"refresh_success"`
	RefreshFailures int64 `json:"refresh_failures"`
	RefreshRetries  int64 `json:"refresh_retries"`
}

type ErrorClass string

const (
	ErrorClassRetryable ErrorClass = "retryable"
	ErrorClassInvalid   ErrorClass = "invalid"
	ErrorClassTerminal  ErrorClass = "terminal"
)

func NewHTTPClient(tokenURL string) *HTTPClient {
	return &HTTPClient{
		TokenURL: tokenURL,
		Scope:    "openid profile email",
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		MaxRetries: 3,
	}
}

func (c *HTTPClient) Refresh(ctx context.Context, refreshToken string) (RefreshResult, error) {
	return c.refresh(ctx, refreshToken, c.ClientID)
}

func (c *HTTPClient) RefreshWithClientID(ctx context.Context, refreshToken string, clientID string) (RefreshResult, error) {
	return c.refresh(ctx, refreshToken, strings.TrimSpace(clientID))
}

func (c *HTTPClient) refresh(ctx context.Context, refreshToken string, clientID string) (RefreshResult, error) {
	if c == nil || c.TokenURL == "" {
		return RefreshResult{}, errors.New("oauth token url is not configured")
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	maxRetries := c.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		atomic.AddInt64(&c.metrics.RefreshAttempts, 1)
		result, status, err := c.refreshOnce(ctx, refreshToken, clientID)
		if err == nil {
			atomic.AddInt64(&c.metrics.RefreshSuccess, 1)
			return result, nil
		}
		lastErr = err
		if ClassifyStatus(status) != ErrorClassRetryable || attempt == maxRetries {
			break
		}
		atomic.AddInt64(&c.metrics.RefreshRetries, 1)
		select {
		case <-ctx.Done():
			return RefreshResult{}, ctx.Err()
		case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
		}
	}
	atomic.AddInt64(&c.metrics.RefreshFailures, 1)
	return RefreshResult{}, lastErr
}

func (c *HTTPClient) Stats() Metrics {
	if c == nil {
		return Metrics{}
	}
	return Metrics{
		RefreshAttempts: atomic.LoadInt64(&c.metrics.RefreshAttempts),
		RefreshSuccess:  atomic.LoadInt64(&c.metrics.RefreshSuccess),
		RefreshFailures: atomic.LoadInt64(&c.metrics.RefreshFailures),
		RefreshRetries:  atomic.LoadInt64(&c.metrics.RefreshRetries),
	}
}

func (c *HTTPClient) ExchangeAuthorizationCode(ctx context.Context, request AuthorizationCodeRequest) (RefreshResult, error) {
	if c == nil || c.TokenURL == "" {
		return RefreshResult{}, errors.New("oauth token url is not configured")
	}
	if c.ClientID == "" {
		return RefreshResult{}, errors.New("oauth client id is not configured")
	}
	if strings.TrimSpace(request.Code) == "" {
		return RefreshResult{}, errors.New("oauth authorization code is required")
	}
	if strings.TrimSpace(request.RedirectURI) == "" {
		return RefreshResult{}, errors.New("oauth redirect uri is required")
	}
	if strings.TrimSpace(request.CodeVerifier) == "" {
		return RefreshResult{}, errors.New("oauth code verifier is required")
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", c.ClientID)
	form.Set("code", strings.TrimSpace(request.Code))
	form.Set("redirect_uri", strings.TrimSpace(request.RedirectURI))
	form.Set("code_verifier", strings.TrimSpace(request.CodeVerifier))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return RefreshResult{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "codex-cli/0.91.0")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return RefreshResult{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RefreshResult{}, fmt.Errorf("oauth authorization code exchange failed with status %d: %s", resp.StatusCode, string(body))
	}
	return ParseRefreshResponse(body)
}

func (c *HTTPClient) refreshOnce(ctx context.Context, refreshToken string, clientID string) (RefreshResult, int, error) {
	form := url.Values{}
	if clientID != "" {
		form.Set("client_id", clientID)
	}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	if c.Scope != "" {
		form.Set("scope", c.Scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return RefreshResult{}, 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "codex-cli/0.91.0")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return RefreshResult{}, 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RefreshResult{}, resp.StatusCode, fmt.Errorf("oauth refresh failed with status %d: %s", resp.StatusCode, string(body))
	}
	result, err := ParseRefreshResponse(body)
	if err != nil {
		return RefreshResult{}, resp.StatusCode, err
	}
	return result, resp.StatusCode, nil
}

func ParseRefreshResponse(body []byte) (RefreshResult, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return RefreshResult{}, err
	}
	result := RefreshResult{
		AccessToken:  stringField(payload, "access_token"),
		RefreshToken: stringField(payload, "refresh_token"),
		IDToken:      stringField(payload, "id_token"),
		AccountID:    stringField(payload, "account_id"),
		Email:        stringField(payload, "email"),
		PlanType:     stringField(payload, "plan_type"),
	}
	if expires, ok := payload["expires_in"].(float64); ok {
		result.ExpiresIn = int(expires)
	}
	if identity, err := ParseIDTokenIdentity(result.IDToken); err == nil {
		if result.AccountID == "" {
			result.AccountID = identity.AccountID
		}
		if result.Email == "" {
			result.Email = identity.Email
		}
		if result.PlanType == "" {
			result.PlanType = identity.PlanType
		}
	}
	if result.AccessToken == "" {
		return RefreshResult{}, errors.New("oauth response missing access_token")
	}
	return result, nil
}

func ParseIDTokenIdentity(idToken string) (IDTokenIdentity, error) {
	parts := strings.Split(strings.TrimSpace(idToken), ".")
	if len(parts) < 2 {
		return IDTokenIdentity{}, errors.New("id_token is not a jwt")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return IDTokenIdentity{}, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return IDTokenIdentity{}, err
	}
	identity := IDTokenIdentity{
		AccountID: stringField(claims, "account_id"),
		Email:     stringField(claims, "email"),
		PlanType:  stringField(claims, "plan_type"),
		UserID:    stringField(claims, "user_id"),
	}
	if identity.AccountID == "" {
		identity.AccountID = stringField(claims, "sub")
	}
	if identity.PlanType == "" {
		identity.PlanType = stringField(claims, "plan")
	}
	if authClaims, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if value := stringField(authClaims, "chatgpt_account_id"); value != "" {
			identity.AccountID = value
		} else if value := stringField(authClaims, "account_id"); value != "" {
			identity.AccountID = value
		}
		if value := stringField(authClaims, "chatgpt_user_id"); value != "" {
			identity.UserID = value
		} else if value := stringField(authClaims, "user_id"); value != "" {
			identity.UserID = value
		}
		if value := stringField(authClaims, "chatgpt_plan_type"); value != "" {
			identity.PlanType = value
		} else if value := stringField(authClaims, "plan_type"); value != "" {
			identity.PlanType = value
		}
		if orgs, ok := authClaims["organizations"].([]any); ok && len(orgs) > 0 {
			if org, ok := orgs[0].(map[string]any); ok {
				identity.OrganizationID = stringField(org, "id")
			}
		}
	}
	return identity, nil
}

func ClassifyStatus(status int) ErrorClass {
	switch {
	case status == http.StatusTooManyRequests || status >= 500 || status == 0:
		return ErrorClassRetryable
	case status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden:
		return ErrorClassInvalid
	default:
		return ErrorClassTerminal
	}
}

func DedupRefreshHistory(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func stringField(payload map[string]any, key string) string {
	if value, ok := payload[key].(string); ok {
		return value
	}
	return ""
}

type DisabledClient struct{}

func (DisabledClient) Refresh(context.Context, string) (RefreshResult, error) {
	return RefreshResult{}, errors.New("oauth refresh worker is not configured")
}
