package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

const (
	defaultWHAMUsageURL      = "https://chatgpt.com/backend-api/wham/usage"
	defaultWHAMUserAgent     = "codex_cli_rs/0.125.0 (Debian 13.0.0; x86_64) WindowsTerminal"
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

type codexQuotaSnapshot struct {
	FetchedAt time.Time          `json:"fetched_at"`
	Error     *string            `json:"error"`
	PlanType  *string            `json:"plan_type"`
	Windows   []codexQuotaWindow `json:"windows"`
}

type cachedQuotaSnapshot struct {
	snapshot  *codexQuotaSnapshot
	expiresAt time.Time
}

type adminQuotaService struct {
	client            *http.Client
	usageURL          string
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

func newAdminQuotaService() *adminQuotaService {
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
	return &adminQuotaService{
		client: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
		},
		usageURL:          firstEnv("OAIX_WHAM_USAGE_URL", "WHAM_USAGE_URL", defaultWHAMUsageURL),
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
	quotaByID := map[int64]*codexQuotaSnapshot{}
	var pendingIDs []int64
	if includeQuota && a.quota != nil && len(tokens) > 0 {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		defer cancel()
		quotaByID, pendingIDs = a.quota.collect(ctx, tokens)
	}

	observedCostByID := map[int64]*float64{}
	if len(tokens) > 0 {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		var err error
		observedCostByID, err = a.store.TokenObservedCosts(ctx, tokens)
		if err != nil {
			observedCostByID = map[int64]*float64{}
			if a.logger != nil {
				a.logger.Warn("admin token observed costs load failed", "error", err)
			}
		}
	}

	activeByID := a.activeStreamsByTokenID(tokens)
	cap := a.tokens.ActiveStreamCap()
	items := make([]adminTokenItem, 0, len(tokens))
	for _, token := range tokens {
		if quota := quotaByID[token.ID]; quota != nil && quota.PlanType != nil {
			token.PlanType = quota.PlanType
		}
		items = append(items, adminTokenItem{
			Token:           token,
			ActiveStreams:   activeByID[token.ID],
			ActiveStreamCap: cap,
			ObservedCostUSD: observedCostByID[token.ID],
			Quota:           quotaByID[token.ID],
		})
	}
	return items, pendingIDs
}

func (a *App) activeStreamsByTokenID(tokens []store.Token) map[int64]int64 {
	values := make(map[int64]int64, len(tokens))
	if a.tokens == nil {
		return values
	}
	snapshot := a.tokens.Snapshot()
	for _, token := range tokens {
		if runtimeToken := snapshot.ByID[token.ID]; runtimeToken != nil {
			values[token.ID] = runtimeToken.Active.Load()
		}
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
			out[id] = snapshot
		}
	}
	if syncLimit < len(refresh) {
		pendingIDs = append(pendingIDs, s.scheduleBackground(refresh[syncLimit:])...)
	}
	return out, dedupeInt64(pendingIDs)
}

func quotaEligible(token store.Token) bool {
	return token.IsActive && token.DisabledAt == nil && strings.TrimSpace(token.AccessToken) != ""
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
	if strings.TrimSpace(token.AccessToken) == "" {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, "token has no access token"))
	}
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, ctx.Err().Error()))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.usageURL, nil)
	if err != nil {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token.AccessToken))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	if token.AccountID != nil && strings.TrimSpace(*token.AccountID) != "" {
		req.Header.Set("Chatgpt-Account-Id", strings.TrimSpace(*token.AccountID))
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, err.Error()))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, defaultQuotaBodyMaxBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return s.storeSnapshot(token.ID, quotaErrorSnapshot(now, responseErrorDetail(resp.StatusCode, body)))
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

func (s *adminQuotaService) storeSnapshot(tokenID int64, snapshot *codexQuotaSnapshot) *codexQuotaSnapshot {
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
		Windows:   []codexQuotaWindow{},
	}
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
		FetchedAt: now,
		Error:     nil,
		PlanType:  planType,
		Windows:   windows,
	}, nil
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
			if message := normalizeText(firstValue(errorValue, "message", "type", "code")); message != nil {
				return fmt.Sprintf("HTTP %d: %s", status, *message)
			}
		}
		if detail := normalizeText(firstValue(payload, "detail", "message")); detail != nil {
			return fmt.Sprintf("HTTP %d: %s", status, *detail)
		}
	}
	text := shortenError(string(body), 220)
	if text == "" {
		return fmt.Sprintf("HTTP %d", status)
	}
	return fmt.Sprintf("HTTP %d: %s", status, text)
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
