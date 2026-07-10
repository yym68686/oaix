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
	officialModelsMaxEntries   = 64
)

type officialModelsCacheEntry struct {
	body        []byte
	contentType string
	etag        string
	plan        string
	storedAt    time.Time
	expiresAt   time.Time
	staleUntil  time.Time
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

func (a *App) writeOfficialModelsCatalog(w http.ResponseWriter, r *http.Request, clientVersion string) bool {
	if a == nil || a.modelCatalog == nil || a.tokens == nil {
		return false
	}
	auth := authFromContext(r.Context())
	ownerUserID, err := auth.proxyOwner(r.Context(), a.store)
	if err != nil {
		a.logOfficialModelsFailure("resolve owner", 0, clientVersion, err)
		return false
	}
	entry, cacheState, err := a.resolveOfficialModelsCatalog(r.Context(), ownerUserID, clientVersion)
	if err != nil {
		a.logOfficialModelsFailure("fetch catalog", ownerUserID, clientVersion, err)
		return false
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
	if entry.etag != "" {
		w.Header().Set("ETag", entry.etag)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(entry.body)
	return true
}

func (a *App) resolveOfficialModelsCatalog(ctx context.Context, ownerUserID int64, clientVersion string) (officialModelsCacheEntry, string, error) {
	cacheKey := officialModelsCacheKey(ownerUserID, clientVersion)
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
		entry, err := a.fetchOfficialModelsCatalog(fetchCtx, ownerUserID, clientVersion)
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

func (a *App) fetchOfficialModelsCatalog(ctx context.Context, ownerUserID int64, clientVersion string) (officialModelsCacheEntry, error) {
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
	var payload struct {
		Models []json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return officialModelsCacheEntry{}, fmt.Errorf("decode official models response: %w", err)
	}
	if len(payload.Models) == 0 {
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
		storedAt:    now,
		expiresAt:   now.Add(cacheTTL),
		staleUntil:  now.Add(staleTTL),
	}, nil
}

func officialModelsCacheKey(ownerUserID int64, clientVersion string) string {
	return strconv.FormatInt(ownerUserID, 10) + ":" + strings.TrimSpace(clientVersion)
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
