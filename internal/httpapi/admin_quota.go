package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

const (
	defaultWHAMUsageURL      = "https://chatgpt.com/backend-api/wham/usage"
	defaultWHAMResetURL      = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume"
	defaultWHAMUserAgent     = "codex_cli_rs/0.125.0 (Debian 13.0.0; x86_64) WindowsTerminal"
	defaultWHAMOriginator    = "Codex Desktop"
	defaultWHAMLanguage      = "zh-CN"
	quotaWindow5HSeconds     = 5 * 60 * 60
	quotaWindow7DSeconds     = 7 * 24 * 60 * 60
	defaultQuotaTTLSeconds   = 60
	defaultQuotaConcurrency  = 4
	defaultQuotaSyncLimit    = 4
	defaultQuotaAsyncLimit   = 32
	defaultQuotaHTTPTimeout  = 8 * time.Second
	defaultQuotaBodyMaxBytes = 2 * 1024 * 1024
)

type adminTokenItem struct {
	store.Token
	Status          string              `json:"status"`
	ActiveStreams   int64               `json:"active_streams"`
	ActiveStreamCap int64               `json:"active_stream_cap"`
	ObservedCostUSD *float64            `json:"observed_cost_usd"`
	Quota           *codexQuotaSnapshot `json:"quota,omitempty"`
}

type codexQuotaWindow struct {
	ID                 string     `json:"id"`
	Label              string     `json:"label"`
	LimitWindowSeconds *int       `json:"limit_window_seconds,omitempty"`
	UsedPercent        *float64   `json:"used_percent,omitempty"`
	RemainingPercent   *float64   `json:"remaining_percent,omitempty"`
	ResetAt            *time.Time `json:"reset_at,omitempty"`
	Exhausted          bool       `json:"exhausted"`
}

type codexQuotaResetCredits struct {
	AvailableCount int `json:"available_count"`
}

type codexQuotaSnapshot struct {
	FetchedAt             time.Time               `json:"fetched_at"`
	Error                 *string                 `json:"error"`
	PlanType              *string                 `json:"plan_type"`
	Disabled              bool                    `json:"disabled,omitempty"`
	Windows               []codexQuotaWindow      `json:"windows"`
	RateLimitResetCredits *codexQuotaResetCredits `json:"rate_limit_reset_credits,omitempty"`
}

type codexQuotaResetCredit struct {
	ID              string `json:"id,omitempty"`
	ResetType       string `json:"reset_type,omitempty"`
	Status          string `json:"status,omitempty"`
	GrantedAt       string `json:"granted_at,omitempty"`
	ExpiresAt       string `json:"expires_at,omitempty"`
	RedeemStartedAt string `json:"redeem_started_at,omitempty"`
	RedeemedAt      string `json:"redeemed_at,omitempty"`
}

type codexQuotaResetResult struct {
	Code         string                 `json:"code"`
	Credit       *codexQuotaResetCredit `json:"credit,omitempty"`
	WindowsReset int                    `json:"windows_reset"`
}

type cachedQuotaSnapshot struct {
	snapshot  *codexQuotaSnapshot
	expiresAt time.Time
}

type adminQuotaService struct {
	client            *http.Client
	oauthClient       oauth.Client
	store             *store.Store
	usageURL          string
	resetURL          string
	userAgent         string
	ttl               time.Duration
	concurrency       int
	syncRefreshLimit  int
	asyncRefreshLimit int
	sem               chan struct{}
	mu                sync.Mutex
	cache             map[int64]cachedQuotaSnapshot
	pending           map[int64]struct{}
}

func newAdminQuotaService(cfg config.Config, tokenStore *store.Store) *adminQuotaService {
	concurrency := envIntDefault("OAIX_ADMIN_QUOTA_CONCURRENCY", defaultQuotaConcurrency)
	if concurrency <= 0 {
		concurrency = defaultQuotaConcurrency
	}
	timeoutSeconds := envIntDefault("OAIX_ADMIN_QUOTA_TIMEOUT_SECONDS", int(defaultQuotaHTTPTimeout/time.Second))
	if timeoutSeconds <= 0 {
		timeoutSeconds = int(defaultQuotaHTTPTimeout / time.Second)
	}
	ttlSeconds := envIntDefault("OAIX_ADMIN_QUOTA_TTL_SECONDS", defaultQuotaTTLSeconds)
	if ttlSeconds <= 0 {
		ttlSeconds = defaultQuotaTTLSeconds
	}
	oauthClient := oauth.NewHTTPClient(cfg.Upstream.OAuthTokenURL)
	oauthClient.ClientID = cfg.Upstream.OAuthClientID
	oauthClient.Scope = cfg.Upstream.OAuthScope
	return &adminQuotaService{
		client: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
		},
		oauthClient:       oauthClient,
		store:             tokenStore,
		usageURL:          firstEnv("OAIX_WHAM_USAGE_URL", "WHAM_USAGE_URL", defaultWHAMUsageURL),
		resetURL:          firstEnv("OAIX_WHAM_RATE_LIMIT_RESET_URL", "WHAM_RATE_LIMIT_RESET_URL", defaultWHAMResetURL),
		userAgent:         firstEnv("OAIX_WHAM_USER_AGENT", "WHAM_USER_AGENT", defaultWHAMUserAgent),
		ttl:               time.Duration(ttlSeconds) * time.Second,
		concurrency:       concurrency,
		syncRefreshLimit:  envIntDefault("OAIX_ADMIN_QUOTA_SYNC_REFRESH_LIMIT", defaultQuotaSyncLimit),
		asyncRefreshLimit: envIntDefault("OAIX_ADMIN_QUOTA_BACKGROUND_REFRESH_LIMIT", defaultQuotaAsyncLimit),
		sem:               make(chan struct{}, concurrency),
		cache:             map[int64]cachedQuotaSnapshot{},
		pending:           map[int64]struct{}{},
	}
}

func (a *App) adminTokenItems(parent context.Context, tokens []store.Token, includeQuota bool) ([]adminTokenItem, []int64) {
	return a.adminTokenItemsAt(parent, tokens, includeQuota, time.Now().UTC())
}

func (a *App) adminTokenItemsAt(parent context.Context, tokens []store.Token, includeQuota bool, now time.Time) ([]adminTokenItem, []int64) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	quotaByID := map[int64]*codexQuotaSnapshot{}
	var pendingIDs []int64
	if includeQuota && a.quota != nil && len(tokens) > 0 {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		defer cancel()
		quotaByID, pendingIDs = a.quota.collect(ctx, tokens)
		a.syncQuotaPlanTypes(parent, quotaByID)
	}

	observedCostByID := map[int64]*float64{}
	if len(tokens) > 0 {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		var err error
		observedCostByID, err = a.store.TokenObservedCosts(ctx, tokens)
		if err != nil {
			if observedCostByID == nil {
				observedCostByID = map[int64]*float64{}
			}
			if a.logger != nil {
				a.logger.Warn("admin token observed costs load failed", "error", err)
			}
		}
	}

	activeByID := a.activeStreamsByTokenID(tokens)
	cap := a.tokens.ActiveStreamCap()
	items := make([]adminTokenItem, 0, len(tokens))
	disabledFromQuota := false
	for _, token := range tokens {
		if quota := quotaByID[token.ID]; quota != nil && quota.PlanType != nil {
			token.PlanType = quota.PlanType
		}
		if quotaSnapshotDisablesToken(quotaByID[token.ID]) {
			if token.IsActive {
				disabledFromQuota = true
			}
			token.IsActive = false
		}
		items = append(items, adminTokenItem{
			Token:           token,
			Status:          adminTokenStatus(token, now),
			ActiveStreams:   activeByID[token.ID],
			ActiveStreamCap: cap,
			ObservedCostUSD: observedCostByID[token.ID],
			Quota:           quotaByID[token.ID],
		})
	}
	if disabledFromQuota && a.tokens != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
			a.logger.Warn("token pool refresh after quota disable failed", "error", err)
		}
	}
	return items, pendingIDs
}

func adminTokenStatus(token store.Token, now time.Time) string {
	if !token.IsActive || token.DisabledAt != nil {
		return "disabled"
	}
	if token.CooldownUntil != nil && token.CooldownUntil.After(now) && !store.TokenHasTransientRetryBackoff(token, now) {
		return "cooling"
	}
	return "active"
}

func (a *App) syncQuotaPlanTypes(parent context.Context, quotaByID map[int64]*codexQuotaSnapshot) {
	if a.store == nil || len(quotaByID) == 0 {
		return
	}
	planByTokenID := make(map[int64]string, len(quotaByID))
	for tokenID, quota := range quotaByID {
		if quota == nil || quota.PlanType == nil {
			continue
		}
		plan := strings.TrimSpace(*quota.PlanType)
		if plan == "" {
			continue
		}
		planByTokenID[tokenID] = plan
	}
	if len(planByTokenID) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := a.store.UpdateTokenPlanTypes(ctx, planByTokenID); err != nil && a.logger != nil {
		a.logger.Warn("admin quota plan type sync failed", "error", err)
	}
}

func (a *App) activeStreamsByTokenID(tokenRows []store.Token) map[int64]int64 {
	values := make(map[int64]int64, len(tokenRows))
	if a.tokens == nil {
		return values
	}
	for _, token := range tokenRows {
		values[token.ID] = a.tokens.ActiveStreamsForToken(token.ID, token.OwnerUserID)
	}
	return values
}

func (s *adminQuotaService) collect(ctx context.Context, tokens []store.Token) (map[int64]*codexQuotaSnapshot, []int64) {
	out := make(map[int64]*codexQuotaSnapshot, len(tokens))
	var refresh []store.Token
	var pendingIDs []int64
	now := time.Now().UTC()

	for _, token := range tokens {
		if !quotaEligible(token) {
			continue
		}
		if snapshot, fresh := s.cached(token.ID, now, true); snapshot != nil {
			out[token.ID] = snapshot
			if fresh {
				continue
			}
		}
		if s.isPending(token.ID) {
			pendingIDs = append(pendingIDs, token.ID)
			continue
		}
		refresh = append(refresh, token)
	}

	syncLimit := s.syncRefreshLimit
	if syncLimit < 0 {
		syncLimit = 0
	}
	if syncLimit > len(refresh) {
		syncLimit = len(refresh)
	}
	if syncLimit > 0 {
		for id, snapshot := range s.fetchMany(ctx, refresh[:syncLimit]) {
			if snapshot != nil {
				out[id] = snapshot
			}
		}
	}
	if syncLimit < len(refresh) {
		pendingIDs = append(pendingIDs, s.scheduleBackground(refresh[syncLimit:])...)
	}
	return out, dedupeInt64(pendingIDs)
}

func quotaEligible(token store.Token) bool {
	return token.IsActive && token.DisabledAt == nil && (strings.TrimSpace(token.AccessToken) != "" || strings.TrimSpace(token.RefreshToken) != "")
}

func quotaActionEligible(token store.Token) bool {
	return strings.TrimSpace(token.AccessToken) != "" || strings.TrimSpace(token.RefreshToken) != ""
}

var errQuotaTokenUnavailable = errors.New("token has no usable access or refresh token")

type quotaUpstreamError struct {
	status int
	detail string
}

func (e *quotaUpstreamError) Error() string {
	return e.detail
}

func (s *adminQuotaService) cached(tokenID int64, now time.Time, includeStale bool) (*codexQuotaSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cached, ok := s.cache[tokenID]
	if !ok || cached.snapshot == nil {
		return nil, false
	}
	fresh := cached.expiresAt.After(now)
	if !fresh && !includeStale {
		delete(s.cache, tokenID)
		return nil, false
	}
	return cached.snapshot, fresh
}

func (s *adminQuotaService) clear(tokenIDs []int64) {
	if s == nil || len(tokenIDs) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range tokenIDs {
		delete(s.cache, id)
		delete(s.pending, id)
	}
}

func (s *adminQuotaService) isPending(tokenID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[tokenID]
	return ok
}

func (s *adminQuotaService) scheduleBackground(tokens []store.Token) []int64 {
	if len(tokens) == 0 {
		return nil
	}
	limit := s.asyncRefreshLimit
	if limit <= 0 {
		limit = defaultQuotaAsyncLimit
	}
	if len(tokens) > limit {
		tokens = tokens[:limit]
	}

	batch := make([]store.Token, 0, len(tokens))
	ids := make([]int64, 0, len(tokens))
	s.mu.Lock()
	for _, token := range tokens {
		if _, exists := s.pending[token.ID]; exists {
			continue
		}
		s.pending[token.ID] = struct{}{}
		batch = append(batch, token)
		ids = append(ids, token.ID)
	}
	s.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(len(batch))*s.client.Timeout+15*time.Second)
		defer cancel()
		_ = s.fetchMany(ctx, batch)
		s.mu.Lock()
		for _, id := range ids {
			delete(s.pending, id)
		}
		s.mu.Unlock()
	}()
	return ids
}

func (s *adminQuotaService) fetchMany(ctx context.Context, tokens []store.Token) map[int64]*codexQuotaSnapshot {
	results := make(map[int64]*codexQuotaSnapshot, len(tokens))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, token := range tokens {
		token := token
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot := s.fetchSnapshot(ctx, token)
			mu.Lock()
			results[token.ID] = snapshot
			mu.Unlock()
		}()
	}
	wg.Wait()
	return results
}

func (s *adminQuotaService) fetchSnapshot(ctx context.Context, token store.Token) *codexQuotaSnapshot {
	now := time.Now().UTC()
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return nil
	}

	if strings.TrimSpace(token.AccessToken) == "" {
		refreshed, err := s.refreshQuotaToken(ctx, token)
		if err != nil {
			if contextError(err) {
				return nil
			}
			return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
		}
		token = refreshed
	}

	statusCode, body, err := s.requestUsage(ctx, token)
	if err != nil {
		if contextError(err) {
			return nil
		}
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
	}
	if quotaStatusShouldRefresh(statusCode) {
		if refreshed, refreshErr := s.refreshQuotaToken(ctx, token); refreshErr == nil {
			token = refreshed
			statusCode, body, err = s.requestUsage(ctx, token)
			if err != nil {
				if contextError(err) {
					return nil
				}
				return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
			}
		}
	}
	if statusCode < 200 || statusCode >= 300 {
		detail := responseErrorDetail(statusCode, body)
		if quotaResponseShouldDisable(statusCode, body) {
			s.disableTokenFromQuota(ctx, token.ID)
		}
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, detail))
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, "Quota endpoint returned invalid JSON"))
	}
	snapshot, err := parseCodexQuotaPayload(payload, now)
	if err != nil {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
	}
	return s.storeSnapshot(token.ID, snapshot)
}

func (s *adminQuotaService) queryResetCredits(ctx context.Context, token store.Token) (*codexQuotaSnapshot, error) {
	if s == nil {
		return nil, errors.New("quota service is not configured")
	}
	if !quotaActionEligible(token) {
		return nil, errQuotaTokenUnavailable
	}
	snapshot := s.fetchSnapshot(ctx, token)
	if snapshot == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("quota query did not complete")
	}
	return snapshot, nil
}

func (s *adminQuotaService) resetCredit(ctx context.Context, token store.Token) (*codexQuotaResetResult, error) {
	if s == nil {
		return nil, errors.New("quota service is not configured")
	}
	if !quotaActionEligible(token) {
		return nil, errQuotaTokenUnavailable
	}
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if strings.TrimSpace(token.AccessToken) == "" {
		refreshed, err := s.refreshQuotaToken(ctx, token)
		if err != nil {
			return nil, err
		}
		token = refreshed
	}

	redeemRequestID, err := generateQuotaRedeemRequestID()
	if err != nil {
		return nil, err
	}
	statusCode, body, result, err := s.requestResetCredit(ctx, token, redeemRequestID)
	if err != nil {
		return nil, err
	}
	if quotaStatusShouldRefresh(statusCode) {
		if refreshed, refreshErr := s.refreshQuotaToken(ctx, token); refreshErr == nil {
			token = refreshed
			statusCode, body, result, err = s.requestResetCredit(ctx, token, redeemRequestID)
			if err != nil {
				return nil, err
			}
		}
	}
	if statusCode < 200 || statusCode >= 300 {
		return nil, &quotaUpstreamError{status: quotaUpstreamHTTPStatus(statusCode), detail: responseErrorDetail(statusCode, body)}
	}
	if result == nil {
		return nil, errors.New("quota reset endpoint returned an empty response")
	}
	s.clear([]int64{token.ID})
	return result, nil
}

func (s *adminQuotaService) requestUsage(ctx context.Context, token store.Token) (int, []byte, error) {
	accessToken := strings.TrimSpace(token.AccessToken)
	if accessToken == "" {
		return 0, nil, fmt.Errorf("token has no access token")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.usageURL, nil)
	if err != nil {
		return 0, nil, err
	}
	s.setWHAMHeaders(req, token, true)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, defaultQuotaBodyMaxBytes))
	return resp.StatusCode, body, nil
}

func (s *adminQuotaService) requestResetCredit(ctx context.Context, token store.Token, redeemRequestID string) (int, []byte, *codexQuotaResetResult, error) {
	accessToken := strings.TrimSpace(token.AccessToken)
	if accessToken == "" {
		return 0, nil, nil, fmt.Errorf("token has no access token")
	}
	bodyBytes, err := json.Marshal(map[string]string{"redeem_request_id": redeemRequestID})
	if err != nil {
		return 0, nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.resetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, nil, nil, err
	}
	s.setWHAMHeaders(req, token, true)

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, defaultQuotaBodyMaxBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, responseBody, nil, nil
	}
	var payload codexQuotaResetResult
	if err := json.Unmarshal(responseBody, &payload); err != nil {
		return resp.StatusCode, responseBody, nil, fmt.Errorf("quota reset endpoint returned invalid JSON")
	}
	return resp.StatusCode, responseBody, &payload, nil
}

func (s *adminQuotaService) setWHAMHeaders(req *http.Request, token store.Token, contentJSON bool) {
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token.AccessToken))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Oai-Language", defaultWHAMLanguage)
	req.Header.Set("Originator", defaultWHAMOriginator)
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-Mode", "no-cors")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Priority", "u=4, i")
	if contentJSON {
		req.Header.Set("Content-Type", "application/json")
	}
	if token.AccountID != nil && strings.TrimSpace(*token.AccountID) != "" {
		req.Header.Set("Chatgpt-Account-Id", strings.TrimSpace(*token.AccountID))
	}
}

func quotaStatusShouldRefresh(statusCode int) bool {
	return statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden || statusCode == http.StatusNotFound
}

func quotaUpstreamHTTPStatus(status int) int {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return status
	case status == http.StatusTooManyRequests:
		return http.StatusTooManyRequests
	case status >= 400 && status < 500:
		return http.StatusBadGateway
	case status >= 500:
		return http.StatusBadGateway
	default:
		return http.StatusBadGateway
	}
}

func quotaErrorHTTPStatus(err error) int {
	if errors.Is(err, errQuotaTokenUnavailable) {
		return http.StatusBadRequest
	}
	if contextError(err) {
		return http.StatusGatewayTimeout
	}
	var upstream *quotaUpstreamError
	if errors.As(err, &upstream) && upstream.status > 0 {
		return upstream.status
	}
	return http.StatusBadGateway
}

func contextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *adminQuotaService) refreshQuotaToken(ctx context.Context, token store.Token) (store.Token, error) {
	refreshToken := strings.TrimSpace(token.RefreshToken)
	if refreshToken == "" {
		return token, fmt.Errorf("token has no refresh token")
	}
	if s.oauthClient == nil {
		return token, fmt.Errorf("oauth refresh client is not configured")
	}
	result, err := s.oauthClient.Refresh(ctx, refreshToken)
	if err != nil {
		return token, err
	}
	expiresAt := (*time.Time)(nil)
	if result.ExpiresIn > 0 {
		expires := time.Now().UTC().Add(time.Duration(result.ExpiresIn) * time.Second)
		expiresAt = &expires
	}
	if s.store != nil {
		if err := s.store.UpdateTokenSecret(ctx, store.TokenSecretUpdate{
			TokenID:      token.ID,
			AccessToken:  result.AccessToken,
			RefreshToken: result.RefreshToken,
			IDToken:      result.IDToken,
			ExpiresAt:    expiresAt,
			AccountID:    result.AccountID,
			Email:        result.Email,
			PlanType:     result.PlanType,
		}); err != nil {
			return token, err
		}
	}
	token.AccessToken = result.AccessToken
	if strings.TrimSpace(result.RefreshToken) != "" {
		token.RefreshToken = result.RefreshToken
	}
	if strings.TrimSpace(result.AccountID) != "" {
		value := result.AccountID
		token.AccountID = &value
	}
	if strings.TrimSpace(result.Email) != "" {
		value := result.Email
		token.Email = &value
	}
	if strings.TrimSpace(result.PlanType) != "" {
		value := result.PlanType
		token.PlanType = &value
	}
	return token, nil
}

func (a *App) getTokenQuotaResetCredits(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetToken(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeTokenQuotaResetCredits(w, r, *token)
}

func (a *App) getMyTokenQuotaResetCredits(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := a.tokenSelfScope(r.Context(), w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetTokenScoped(ctx, scope, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeTokenQuotaResetCredits(w, r, *token)
}

func (a *App) writeTokenQuotaResetCredits(w http.ResponseWriter, r *http.Request, token store.Token) {
	if a.quota == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("quota service is not configured"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	snapshot, err := a.quota.queryResetCredits(ctx, token)
	if err != nil {
		writeError(w, quotaErrorHTTPStatus(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"quota":                    snapshot,
		"rate_limit_reset_credits": snapshot.RateLimitResetCredits,
	})
}

func (a *App) resetTokenQuotaCredit(w http.ResponseWriter, r *http.Request) {
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetToken(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, errors.New("token not found"))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeTokenQuotaReset(w, r, *token, "api", map[string]any{"owner_user_id": token.OwnerUserID})
}

func (a *App) resetMyTokenQuotaCredit(w http.ResponseWriter, r *http.Request) {
	auth := authFromContext(r.Context())
	scope, ok := a.tokenSelfScope(r.Context(), w, auth)
	if !ok {
		return
	}
	id, ok := pathInt64(w, r, "token_id")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	token, err := a.store.GetTokenScoped(ctx, scope, id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "token not found"})
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	details := map[string]any{}
	if scope.OwnerUserID != nil {
		details["user_id"] = *scope.OwnerUserID
	}
	a.writeTokenQuotaReset(w, r, *token, "self", details)
}

func (a *App) writeTokenQuotaReset(w http.ResponseWriter, r *http.Request, token store.Token, actor string, details map[string]any) {
	if a.quota == nil {
		writeError(w, http.StatusServiceUnavailable, errors.New("quota service is not configured"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := a.quota.resetCredit(ctx, token)
	if err != nil {
		writeError(w, quotaErrorHTTPStatus(err), err)
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	activationCtx, activationCancel := context.WithTimeout(context.Background(), 5*time.Second)
	activatedToken, activationErr := a.store.SetTokenActiveScoped(activationCtx, tokenQuotaActivationScope(token), token.ID, true, true)
	activationCancel()
	if activationErr != nil {
		details["auto_available"] = false
		details["auto_available_error"] = activationErr.Error()
		if a.logger != nil {
			a.logger.Warn("token quota reset auto-enable failed", "token_id", token.ID, "owner_user_id", token.OwnerUserID, "error", activationErr)
		}
	} else {
		token = *activatedToken
		details["auto_available"] = true
		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if a.tokens != nil {
			if err := a.tokens.Refresh(refreshCtx); err != nil && a.logger != nil {
				a.logger.Warn("token pool refresh after quota reset failed", "token_id", token.ID, "error", err)
			}
			if token.OwnerUserID > 0 {
				if err := a.tokens.RefreshOwner(refreshCtx, token.OwnerUserID); err != nil && a.logger != nil {
					a.logger.Warn("owner token pool refresh after quota reset failed", "token_id", token.ID, "owner_user_id", token.OwnerUserID, "error", err)
				}
			}
		}
		refreshCancel()
		a.syncSub2APIAvailabilityAsync(&token, "quota_reset_auto_available")
	}
	details["windows_reset"] = result.WindowsReset
	details["code"] = result.Code
	auditCtx, auditCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer auditCancel()
	_ = a.store.WriteAuditLog(auditCtx, "token_quota_reset_credit_consume", actor, "token", strconv.FormatInt(token.ID, 10), details)
	writeJSON(w, http.StatusOK, result)
}

func tokenQuotaActivationScope(token store.Token) store.ResourceScope {
	if token.OwnerUserID > 0 {
		return store.OwnerResources(token.OwnerUserID)
	}
	return store.AllResources()
}

func (s *adminQuotaService) storeSnapshot(tokenID int64, snapshot *codexQuotaSnapshot) *codexQuotaSnapshot {
	if s.store != nil && snapshot != nil {
		_ = s.store.SaveQuotaSnapshot(context.Background(), tokenID, snapshot, snapshot.PlanType, snapshot.Error)
	}
	s.mu.Lock()
	s.cache[tokenID] = cachedQuotaSnapshot{
		snapshot:  snapshot,
		expiresAt: snapshot.FetchedAt.Add(s.ttl),
	}
	if len(s.cache) > 2000 {
		now := time.Now().UTC()
		for id, cached := range s.cache {
			if cached.expiresAt.Before(now) {
				delete(s.cache, id)
			}
		}
	}
	s.mu.Unlock()
	return snapshot
}

func quotaErrorSnapshot(fetchedAt time.Time, message string) *codexQuotaSnapshot {
	errText := shortenError(message, 220)
	return &codexQuotaSnapshot{
		FetchedAt: fetchedAt,
		Error:     &errText,
		Disabled:  quotaErrorMessageShouldDisable(errText),
		Windows:   []codexQuotaWindow{},
	}
}

func (s *adminQuotaService) disableTokenFromQuota(ctx context.Context, tokenID int64) {
	if s.store == nil || tokenID <= 0 {
		return
	}
	disableCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.store.SetTokenActive(disableCtx, tokenID, false, true)
}

func quotaResponseShouldDisable(status int, body []byte) bool {
	if status != http.StatusPaymentRequired {
		return false
	}
	return strings.Contains(strings.ToLower(string(body)), "deactivated_workspace")
}

func quotaErrorMessageShouldDisable(message string) bool {
	return strings.Contains(strings.ToLower(message), "deactivated_workspace")
}

func quotaSnapshotDisablesToken(snapshot *codexQuotaSnapshot) bool {
	if snapshot == nil {
		return false
	}
	if snapshot.Disabled {
		return true
	}
	return snapshot.Error != nil && quotaErrorMessageShouldDisable(*snapshot.Error)
}

func parseCodexQuotaPayload(payload map[string]any, now time.Time) (*codexQuotaSnapshot, error) {
	if payload == nil {
		return nil, fmt.Errorf("Quota payload must be a JSON object")
	}
	rateLimit := firstMapping(payload, "rate_limit", "rateLimit")
	fiveHour, weekly := findCodexWindows(rateLimit)
	limitReached := firstValue(rateLimit, "limit_reached", "limitReached")
	allowed := firstValue(rateLimit, "allowed")
	windows := make([]codexQuotaWindow, 0, 2)
	if window := buildCodexWindow("code-5h", "5h", fiveHour, limitReached, allowed, now); window != nil {
		windows = append(windows, *window)
	}
	if window := buildCodexWindow("code-7d", "7d", weekly, limitReached, allowed, now); window != nil {
		windows = append(windows, *window)
	}
	planType := normalizePlanType(firstValue(payload, "plan_type", "planType"))
	return &codexQuotaSnapshot{
		FetchedAt:             now,
		Error:                 nil,
		PlanType:              planType,
		Windows:               windows,
		RateLimitResetCredits: parseQuotaResetCredits(payload),
	}, nil
}

func parseQuotaResetCredits(payload map[string]any) *codexQuotaResetCredits {
	credits := firstMapping(payload, "rate_limit_reset_credits", "rateLimitResetCredits")
	if credits == nil {
		return nil
	}
	count := coerceInt(firstValue(credits, "available_count", "availableCount"))
	if count == nil {
		return nil
	}
	available := *count
	if available < 0 {
		available = 0
	}
	return &codexQuotaResetCredits{AvailableCount: available}
}

func findCodexWindows(rateLimit map[string]any) (map[string]any, map[string]any) {
	primary := firstMapping(rateLimit, "primary_window", "primaryWindow")
	secondary := firstMapping(rateLimit, "secondary_window", "secondaryWindow")
	var fiveHour map[string]any
	var weekly map[string]any
	for _, candidate := range []map[string]any{primary, secondary} {
		duration := coerceInt(firstValue(candidate, "limit_window_seconds", "limitWindowSeconds"))
		if duration != nil && *duration == quotaWindow5HSeconds && fiveHour == nil {
			fiveHour = candidate
		}
		if duration != nil && *duration == quotaWindow7DSeconds && weekly == nil {
			weekly = candidate
		}
	}
	if fiveHour == nil {
		fiveHour = primary
	}
	if weekly == nil {
		weekly = secondary
	}
	return fiveHour, weekly
}

func buildCodexWindow(id, label string, window map[string]any, limitReached any, allowed any, now time.Time) *codexQuotaWindow {
	if window == nil {
		return nil
	}
	resetAt := extractResetAt(window, now)
	usedPercent := deduceUsedPercent(window, limitReached, allowed, resetAt)
	var remainingPercent *float64
	if usedPercent != nil {
		remaining := clampPercent(100 - *usedPercent)
		remainingPercent = &remaining
	}
	exhaustedHint := boolIs(coerceBool(limitReached), true) || boolIs(coerceBool(allowed), false)
	exhausted := (usedPercent != nil && *usedPercent >= 100) || (usedPercent == nil && exhaustedHint && (resetAt != nil || boolIs(coerceBool(allowed), false)))
	return &codexQuotaWindow{
		ID:                 id,
		Label:              label,
		LimitWindowSeconds: coerceInt(firstValue(window, "limit_window_seconds", "limitWindowSeconds")),
		UsedPercent:        usedPercent,
		RemainingPercent:   remainingPercent,
		ResetAt:            resetAt,
		Exhausted:          exhausted,
	}
}

func extractResetAt(window map[string]any, now time.Time) *time.Time {
	if resetAt := parseAnyTime(firstValue(window, "reset_at", "resetAt")); resetAt != nil {
		return resetAt
	}
	resetAfter := coerceFloat(firstValue(window, "reset_after_seconds", "resetAfterSeconds"))
	if resetAfter == nil || *resetAfter <= 0 {
		return nil
	}
	value := now.Add(time.Duration(*resetAfter * float64(time.Second)))
	return &value
}

func deduceUsedPercent(window map[string]any, limitReached any, allowed any, resetAt *time.Time) *float64 {
	if used := coerceFloat(firstValue(window, "used_percent", "usedPercent")); used != nil {
		value := clampPercent(*used)
		return &value
	}
	exhaustedHint := boolIs(coerceBool(limitReached), true) || boolIs(coerceBool(allowed), false)
	if exhaustedHint && resetAt != nil {
		value := 100.0
		return &value
	}
	return nil
}

func responseErrorDetail(status int, body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		if errorValue, ok := payload["error"].(map[string]any); ok {
			if message := responseValueText(firstValue(errorValue, "message", "type", "code")); message != nil {
				return fmt.Sprintf("HTTP %d: %s", status, *message)
			}
		}
		if detail := responseValueText(firstValue(payload, "detail", "message", "code")); detail != nil {
			return fmt.Sprintf("HTTP %d: %s", status, *detail)
		}
	}
	text := shortenError(string(body), 220)
	if text == "" {
		return fmt.Sprintf("HTTP %d", status)
	}
	return fmt.Sprintf("HTTP %d: %s", status, text)
}

func responseValueText(value any) *string {
	if mapping, ok := value.(map[string]any); ok {
		for _, key := range []string{"message", "type", "code", "detail", "error"} {
			if text := responseValueText(mapping[key]); text != nil {
				return text
			}
		}
		return nil
	}
	if values, ok := value.([]any); ok {
		for _, item := range values {
			if text := responseValueText(item); text != nil {
				return text
			}
		}
		return nil
	}
	return normalizeText(value)
}

func firstMapping(mapping map[string]any, keys ...string) map[string]any {
	if mapping == nil {
		return nil
	}
	for _, key := range keys {
		if value, ok := mapping[key].(map[string]any); ok {
			return value
		}
	}
	return nil
}

func firstValue(mapping map[string]any, keys ...string) any {
	if mapping == nil {
		return nil
	}
	for _, key := range keys {
		if value, ok := mapping[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

func normalizeText(value any) *string {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return nil
	}
	return &text
}

func normalizePlanType(value any) *string {
	text := normalizeText(value)
	if text == nil {
		return nil
	}
	lowered := strings.ToLower(*text)
	lowered = strings.TrimPrefix(lowered, "chatgpt_")
	return &lowered
}

func coerceFloat(value any) *float64 {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case bool:
		return nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil
		}
		return &typed
	case float32:
		value := float64(typed)
		return &value
	case int:
		value := float64(typed)
		return &value
	case int64:
		value := float64(typed)
		return &value
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil
		}
		return &parsed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil
		}
		return &parsed
	default:
		return nil
	}
}

func coerceInt(value any) *int {
	number := coerceFloat(value)
	if number == nil {
		return nil
	}
	valueInt := int(*number)
	return &valueInt
}

func coerceBool(value any) *bool {
	switch typed := value.(type) {
	case bool:
		return &typed
	case int:
		value := typed != 0
		return &value
	case float64:
		value := typed != 0
		return &value
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			value := true
			return &value
		case "0", "false", "no", "off":
			value := false
			return &value
		}
	}
	return nil
}

func parseAnyTime(value any) *time.Time {
	if value == nil {
		return nil
	}
	if number := coerceFloat(value); number != nil {
		ts := *number
		if ts > 1_000_000_000_000 {
			ts /= 1000
		}
		if ts > 0 {
			seconds := int64(ts)
			nanos := int64((ts - float64(seconds)) * 1e9)
			parsed := time.Unix(seconds, nanos).UTC()
			return &parsed
		}
	}
	raw := strings.TrimSpace(fmt.Sprint(value))
	if raw == "" || raw == "<nil>" {
		return nil
	}
	if strings.HasSuffix(raw, "Z") {
		raw = strings.TrimSuffix(raw, "Z") + "+00:00"
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil
	}
	valueTime := parsed.UTC()
	return &valueTime
}

func clampPercent(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func boolIs(value *bool, expected bool) bool {
	return value != nil && *value == expected
}

func generateQuotaRedeemRequestID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	data[6] = (data[6] & 0x0f) | 0x40
	data[8] = (data[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(data)
	return fmt.Sprintf("%s-%s-%s-%s-%s", encoded[0:8], encoded[8:12], encoded[12:16], encoded[16:20], encoded[20:]), nil
}

func shortenError(value string, limit int) string {
	compact := strings.Join(strings.Fields(value), " ")
	if limit > 0 && len(compact) > limit {
		return strings.TrimSpace(compact[:limit-3]) + "..."
	}
	return compact
}

func dedupeInt64(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
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

func firstEnv(primary string, fallbacks ...string) string {
	keys := append([]string{primary}, fallbacks...)
	defaultValue := ""
	if len(fallbacks) > 0 {
		defaultValue = fallbacks[len(fallbacks)-1]
		keys = keys[:len(keys)-1]
	}
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return defaultValue
}

func envIntDefault(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}
