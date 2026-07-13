package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yym68686/oaix/internal/tokens"
)

const (
	officialModelsURL          = "https://chatgpt.com/backend-api/codex/models"
	officialModelsCacheTTL     = 15 * time.Minute
	officialModelsStaleTTL     = 24 * time.Hour
	officialModelsFetchTimeout = 30 * time.Second
	officialModelsWaitTimeout  = 4 * time.Second
	officialModelsMaxAttempts  = 3
	officialModelsMaxBodyBytes = 4 << 20
	officialModelsMaxEntries   = 256
)

var errNoAvailableModelPlans = errors.New("no available account plans for official models catalog")

type officialModelsCacheEntry struct {
	body        []byte
	contentType string
	etag        string
	plan        string
	plans       string
	fastModels  []string
	modelCount  int
	storedAt    time.Time
	expiresAt   time.Time
	staleUntil  time.Time
}

type officialModelsPlanResult struct {
	entry      officialModelsCacheEntry
	cacheState string
	err        error
}

type officialModelsCatalog struct {
	upstreamURL string
	client      *http.Client
	cacheTTL    time.Duration
	staleTTL    time.Duration
	waitTimeout time.Duration
	maxAttempts int

	mu      sync.RWMutex
	entries map[string]officialModelsCacheEntry
	group   singleflight.Group
}

func newOfficialModelsCatalog() *officialModelsCatalog {
	return &officialModelsCatalog{
		upstreamURL: officialModelsURL,
		client:      &http.Client{Timeout: officialModelsFetchTimeout},
		cacheTTL:    officialModelsCacheTTL,
		staleTTL:    officialModelsStaleTTL,
		waitTimeout: officialModelsWaitTimeout,
		maxAttempts: officialModelsMaxAttempts,
		entries:     make(map[string]officialModelsCacheEntry),
	}
}

func (a *App) writeOfficialModelsCatalog(w http.ResponseWriter, r *http.Request, clientVersion string) (bool, error) {
	if a == nil || a.modelCatalog == nil || a.tokens == nil {
		return false, nil
	}
	auth := authFromContext(r.Context())
	ownerUserID, err := auth.proxyOwner(r.Context(), a.store)
	if err != nil {
		a.logOfficialModelsFailure("resolve owner", 0, clientVersion, err)
		return false, err
	}
	entry, cacheState, err := a.resolveOfficialModelsCatalog(r.Context(), ownerUserID, clientVersion)
	if err != nil {
		a.logOfficialModelsFailure("fetch catalog", ownerUserID, clientVersion, err)
		if errors.Is(err, errNoAvailableModelPlans) {
			return false, nil
		}
		return false, err
	}
	contentType := strings.TrimSpace(entry.contentType)
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	source := "official"
	if cacheState == "stale" {
		source = "official-stale"
	}
	w.Header().Set("X-OAIX-Models-Source", source)
	w.Header().Set("X-OAIX-Models-Cache", cacheState)
	if entry.plan != "" {
		w.Header().Set("X-OAIX-Models-Plan", entry.plan)
	}
	if entry.plans != "" {
		w.Header().Set("X-OAIX-Models-Plans", entry.plans)
	}
	if entry.etag != "" {
		w.Header().Set("ETag", entry.etag)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(entry.body)
	return true, nil
}

func (a *App) resolveOfficialModelsCatalog(ctx context.Context, ownerUserID int64, clientVersion string) (officialModelsCacheEntry, string, error) {
	plans := a.tokens.AvailablePlansForOwner(ownerUserID)
	if len(plans) == 0 {
		if err := a.tokens.RefreshOwner(ctx, ownerUserID); err != nil {
			return officialModelsCacheEntry{}, "", err
		}
		plans = a.tokens.AvailablePlansForOwner(ownerUserID)
	}
	if len(plans) == 0 {
		return officialModelsCacheEntry{}, "", errNoAvailableModelPlans
	}

	results := make(chan officialModelsPlanResult, len(plans))
	for _, plan := range plans {
		plan := plan
		go func() {
			entry, cacheState, err := a.resolveOfficialModelsPlanCatalog(ctx, ownerUserID, plan, clientVersion)
			results <- officialModelsPlanResult{entry: entry, cacheState: cacheState, err: err}
		}()
	}

	var bestFast *officialModelsPlanResult
	var bestNoFast *officialModelsPlanResult
	var lastErr error
	for range plans {
		select {
		case <-ctx.Done():
			return officialModelsCacheEntry{}, "", ctx.Err()
		case result := <-results:
			if result.err != nil {
				lastErr = result.err
				continue
			}
			result.entry.plans = strings.Join(plans, ",")
			if len(result.entry.fastModels) > 0 {
				if bestFast == nil || betterOfficialModelsResult(result, *bestFast) {
					copy := result
					bestFast = &copy
				}
				continue
			}
			if bestNoFast == nil || betterOfficialModelsResult(result, *bestNoFast) {
				copy := result
				bestNoFast = &copy
			}
		}
	}
	if bestFast != nil {
		return bestFast.entry, bestFast.cacheState, nil
	}
	if lastErr != nil {
		return officialModelsCacheEntry{}, "", fmt.Errorf("official model plan scan incomplete: %w", lastErr)
	}
	if bestNoFast == nil {
		return officialModelsCacheEntry{}, "", errors.New("no account plan returned an official models catalog")
	}
	return bestNoFast.entry, bestNoFast.cacheState, nil
}

func betterOfficialModelsResult(candidate, current officialModelsPlanResult) bool {
	if len(candidate.entry.fastModels) != len(current.entry.fastModels) {
		return len(candidate.entry.fastModels) > len(current.entry.fastModels)
	}
	if candidate.entry.modelCount != current.entry.modelCount {
		return candidate.entry.modelCount > current.entry.modelCount
	}
	return candidate.entry.plan < current.entry.plan
}

func (a *App) resolveOfficialModelsPlanCatalog(ctx context.Context, ownerUserID int64, plan, clientVersion string) (officialModelsCacheEntry, string, error) {
	cacheKey := officialModelsCacheKey(ownerUserID, plan, clientVersion)
	now := time.Now().UTC()
	if entry, ok := a.modelCatalog.cached(cacheKey, now, false); ok {
		return entry, "hit", nil
	}

	result := a.modelCatalog.group.DoChan(cacheKey, func() (any, error) {
		if entry, ok := a.modelCatalog.cached(cacheKey, time.Now().UTC(), false); ok {
			return entry, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.Background(), officialModelsFetchTimeout)
		defer cancel()
		entry, err := a.fetchOfficialModelsPlanCatalog(fetchCtx, ownerUserID, plan, clientVersion)
		if err != nil {
			return officialModelsCacheEntry{}, err
		}
		a.modelCatalog.store(cacheKey, entry)
		return entry, nil
	})

	if entry, ok := a.modelCatalog.cached(cacheKey, time.Now().UTC(), false); ok {
		return entry, "hit", nil
	}
	if stale, ok := a.modelCatalog.cached(cacheKey, now, true); ok {
		return stale, "stale", nil
	}

	waitTimeout := a.modelCatalog.waitTimeout
	if waitTimeout <= 0 {
		waitTimeout = officialModelsWaitTimeout
	}
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return officialModelsCacheEntry{}, "", ctx.Err()
	case <-timer.C:
		return officialModelsCacheEntry{}, "", errors.New("official models fetch continues in background")
	case resolved := <-result:
		if resolved.Err != nil {
			return officialModelsCacheEntry{}, "", resolved.Err
		}
		entry, ok := resolved.Val.(officialModelsCacheEntry)
		if !ok {
			return officialModelsCacheEntry{}, "", errors.New("invalid official models cache result")
		}
		return entry, "miss", nil
	}
}

func (a *App) fetchOfficialModelsPlanCatalog(ctx context.Context, ownerUserID int64, plan, clientVersion string) (officialModelsCacheEntry, error) {
	maxAttempts := a.modelCatalog.maxAttempts
	if maxAttempts <= 0 {
		maxAttempts = officialModelsMaxAttempts
	}
	excluded := make(map[int64]struct{}, maxAttempts)
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		claim, err := a.tokens.Claim(ctx, tokens.Intent{
			Endpoint:        "/v1/models",
			OwnerUserID:     ownerUserID,
			ExcludeTokenIDs: excluded,
			RequiredPlan:    plan,
		})
		if err != nil {
			if lastErr != nil {
				return officialModelsCacheEntry{}, lastErr
			}
			return officialModelsCacheEntry{}, err
		}
		tokenID := claim.TokenID()
		excluded[tokenID] = struct{}{}
		entry, fetchErr := a.fetchOfficialModelsWithClaim(ctx, claim, clientVersion)
		claim.Release()
		if fetchErr == nil {
			entry.plan = plan
			a.tokens.SetPlanModelCapabilities(ownerUserID, plan, clientVersion, entry.fastModels, entry.staleUntil)
			return entry, nil
		}
		lastErr = fmt.Errorf("token %d: %w", tokenID, fetchErr)
	}
	if lastErr == nil {
		lastErr = errors.New("no account returned an official models catalog")
	}
	return officialModelsCacheEntry{}, lastErr
}

func (a *App) fetchOfficialModelsWithClaim(ctx context.Context, claim *tokens.Claim, clientVersion string) (officialModelsCacheEntry, error) {
	requestURL, err := url.Parse(a.modelCatalog.upstreamURL)
	if err != nil {
		return officialModelsCacheEntry{}, err
	}
	query := requestURL.Query()
	query.Set("client_version", clientVersion)
	requestURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return officialModelsCacheEntry{}, err
	}
	req.Header.Set("Authorization", "Bearer "+claim.AccessToken())
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Originator", codexOriginator)
	req.Header.Set("User-Agent", codexUserAgent)
	if accountID := claim.AccountID(); accountID != nil && strings.TrimSpace(*accountID) != "" {
		req.Header.Set("ChatGPT-Account-Id", strings.TrimSpace(*accountID))
	}

	resp, err := a.modelCatalog.client.Do(req)
	if err != nil {
		return officialModelsCacheEntry{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, officialModelsMaxBodyBytes+1))
	if err != nil {
		return officialModelsCacheEntry{}, err
	}
	if len(body) > officialModelsMaxBodyBytes {
		return officialModelsCacheEntry{}, errors.New("official models response is too large")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return officialModelsCacheEntry{}, fmt.Errorf("official models status %d", resp.StatusCode)
	}
	fastModels, modelCount, err := officialFastModels(body)
	if err != nil {
		return officialModelsCacheEntry{}, fmt.Errorf("decode official models response: %w", err)
	}
	if modelCount == 0 {
		return officialModelsCacheEntry{}, errors.New("official models response contains no models")
	}

	now := time.Now().UTC()
	cacheTTL := a.modelCatalog.cacheTTL
	if cacheTTL <= 0 {
		cacheTTL = officialModelsCacheTTL
	}
	staleTTL := a.modelCatalog.staleTTL
	if staleTTL < cacheTTL {
		staleTTL = officialModelsStaleTTL
	}
	plan := ""
	if claim.Token != nil && claim.Token.Token.PlanType != nil {
		plan = strings.TrimSpace(*claim.Token.Token.PlanType)
	}
	return officialModelsCacheEntry{
		body:        append([]byte(nil), body...),
		contentType: resp.Header.Get("Content-Type"),
		etag:        resp.Header.Get("ETag"),
		plan:        plan,
		fastModels:  fastModels,
		modelCount:  modelCount,
		storedAt:    now,
		expiresAt:   now.Add(cacheTTL),
		staleUntil:  now.Add(staleTTL),
	}, nil
}

func officialModelsCacheKey(ownerUserID int64, plan, clientVersion string) string {
	return strconv.FormatInt(ownerUserID, 10) + ":" + strings.ToLower(strings.TrimSpace(plan)) + ":" + strings.TrimSpace(clientVersion)
}

func officialFastModels(body []byte) ([]string, int, error) {
	var payload struct {
		Models []json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, 0, err
	}
	fastModels := make([]string, 0)
	for _, rawModel := range payload.Models {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rawModel, &fields); err != nil {
			return nil, 0, err
		}
		var slug string
		if err := json.Unmarshal(fields["slug"], &slug); err != nil || strings.TrimSpace(slug) == "" {
			continue
		}
		if officialModelHasFastTier(fields) {
			fastModels = append(fastModels, strings.TrimSpace(slug))
		}
	}
	return fastModels, len(payload.Models), nil
}

func officialModelHasFastTier(fields map[string]json.RawMessage) bool {
	if raw, present := fields["service_tiers"]; present {
		var tiers []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &tiers); err != nil {
			return false
		}
		for _, tier := range tiers {
			id := strings.ToLower(strings.TrimSpace(tier.ID))
			name := strings.ToLower(strings.TrimSpace(tier.Name))
			if id == "priority" || id == "fast" || name == "fast" {
				return true
			}
		}
		return false
	}

	var legacy []string
	if err := json.Unmarshal(fields["additional_speed_tiers"], &legacy); err != nil {
		return false
	}
	for _, tier := range legacy {
		if normalized := strings.ToLower(strings.TrimSpace(tier)); normalized == "fast" || normalized == "priority" {
			return true
		}
	}
	return false
}

func (c *officialModelsCatalog) cached(key string, now time.Time, allowStale bool) (officialModelsCacheEntry, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return officialModelsCacheEntry{}, false
	}
	deadline := entry.expiresAt
	if allowStale {
		deadline = entry.staleUntil
	}
	if deadline.IsZero() || !now.Before(deadline) {
		return officialModelsCacheEntry{}, false
	}
	entry.body = append([]byte(nil), entry.body...)
	return entry, true
}

func (c *officialModelsCatalog) store(key string, entry officialModelsCacheEntry) {
	entry.body = append([]byte(nil), entry.body...)
	c.mu.Lock()
	_, exists := c.entries[key]
	if !exists && len(c.entries) >= officialModelsMaxEntries {
		oldestKey := ""
		var oldestAt time.Time
		for candidateKey, candidate := range c.entries {
			if oldestKey == "" || candidate.storedAt.Before(oldestAt) {
				oldestKey = candidateKey
				oldestAt = candidate.storedAt
			}
		}
		if oldestKey != "" {
			delete(c.entries, oldestKey)
		}
	}
	c.entries[key] = entry
	c.mu.Unlock()
}

func (a *App) logOfficialModelsFailure(action string, ownerUserID int64, clientVersion string, err error) {
	if a == nil || a.logger == nil {
		return
	}
	level := slog.LevelWarn
	if strings.Contains(err.Error(), "continues in background") {
		level = slog.LevelDebug
	}
	a.logger.Log(context.Background(), level, "official models catalog unavailable",
		"action", action,
		"owner_user_id", ownerUserID,
		"client_version", clientVersion,
		"error", err,
	)
}
