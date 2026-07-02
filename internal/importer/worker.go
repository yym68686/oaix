package importer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

type Validator interface {
	Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error)
}

type ValidatorFunc func(context.Context, store.ImportItem) (ValidatedItem, error)

func (fn ValidatorFunc) Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error) {
	return fn(ctx, item)
}

type ValidatedItem struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	AccountID    string
	Email        string
	PlanType     string
	Action       string
}

type Worker struct {
	MaxConcurrency int
	Validator      Validator
	metrics        WorkerMetrics
}

type WorkerMetrics struct {
	Claimed   int64 `json:"claimed"`
	Validated int64 `json:"validated"`
	Published int64 `json:"published"`
	Failed    int64 `json:"failed"`
}

func (w *Worker) ValidateBatch(ctx context.Context, items []store.ImportItem) []store.ImportItemUpdate {
	if len(items) == 0 {
		return nil
	}
	atomic.AddInt64(&w.metrics.Claimed, int64(len(items)))
	concurrency := w.MaxConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	if concurrency > len(items) {
		concurrency = len(items)
	}
	validator := w.Validator
	if validator == nil {
		validator = AccessTokenValidator{}
	}
	updates := make([]store.ImportItemUpdate, len(items))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				item := items[index]
				started := time.Now()
				validated, err := validator.Validate(ctx, item)
				elapsed := int(time.Since(started).Milliseconds())
				if err != nil {
					atomic.AddInt64(&w.metrics.Failed, 1)
					updates[index] = store.ImportItemUpdate{
						ID:           item.ID,
						Status:       string(ItemFailed),
						ErrorMessage: err.Error(),
						ValidationMS: &elapsed,
					}
					continue
				}
				atomic.AddInt64(&w.metrics.Validated, 1)
				validatedPayload := map[string]any{
					"access_token":  validated.AccessToken,
					"refresh_token": validated.RefreshToken,
					"id_token":      validated.IDToken,
					"account_id":    validated.AccountID,
					"email":         validated.Email,
					"plan_type":     validated.PlanType,
				}
				copyImportControlFields(item.Payload, validatedPayload)
				updates[index] = store.ImportItemUpdate{
					ID:               item.ID,
					Status:           string(ItemValidated),
					ValidatedPayload: validatedPayload,
					Action:           validated.Action,
					ValidationMS:     &elapsed,
				}
			}
		}()
	}
dispatch:
	for index := range items {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- index:
		}
	}
	close(jobs)
	wg.Wait()
	return updates
}

func (w *Worker) Stats() WorkerMetrics {
	return WorkerMetrics{
		Claimed:   atomic.LoadInt64(&w.metrics.Claimed),
		Validated: atomic.LoadInt64(&w.metrics.Validated),
		Published: atomic.LoadInt64(&w.metrics.Published),
		Failed:    atomic.LoadInt64(&w.metrics.Failed),
	}
}

func (w *Worker) RecordPublished(count int) {
	if count > 0 {
		atomic.AddInt64(&w.metrics.Published, int64(count))
	}
}

type AccessTokenValidator struct{}

func (AccessTokenValidator) Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error) {
	select {
	case <-ctx.Done():
		return ValidatedItem{}, ctx.Err()
	default:
	}
	for _, key := range []string{"access_token", "accessToken", "token"} {
		if value, ok := item.Payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return ValidatedItem{AccessToken: strings.TrimSpace(value), Action: "upsert_access_token"}, nil
		}
	}
	return ValidatedItem{}, fmt.Errorf("import item %d does not contain an access token", item.ID)
}

type TokenPayloadValidator struct {
	Access  Validator
	Refresh Validator
}

func (v TokenPayloadValidator) Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error) {
	if hasPayloadString(item.Payload, "access_token", "accessToken", "token") {
		access := v.Access
		if access == nil {
			access = AccessTokenValidator{}
		}
		return access.Validate(ctx, item)
	}
	if hasPayloadString(item.Payload, "refresh_token", "refreshToken") {
		refresh := v.Refresh
		if refresh == nil {
			return ValidatedItem{}, fmt.Errorf("oauth client is not configured")
		}
		return refresh.Validate(ctx, item)
	}
	return AccessTokenValidator{}.Validate(ctx, item)
}

type OAuthRefreshValidator struct {
	Client oauth.Client
}

func (v OAuthRefreshValidator) Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error) {
	if v.Client == nil {
		return ValidatedItem{}, fmt.Errorf("oauth client is not configured")
	}
	refreshToken := ""
	for _, key := range []string{"refresh_token", "refreshToken"} {
		if value, ok := item.Payload[key].(string); ok && strings.TrimSpace(value) != "" {
			refreshToken = strings.TrimSpace(value)
			break
		}
	}
	if refreshToken == "" {
		return ValidatedItem{}, fmt.Errorf("import item %d does not contain a refresh token", item.ID)
	}
	result, err := v.Client.Refresh(ctx, refreshToken)
	if err != nil {
		return ValidatedItem{}, err
	}
	return ValidatedItem{
		AccessToken:  result.AccessToken,
		RefreshToken: firstNonEmpty(result.RefreshToken, refreshToken),
		AccountID:    result.AccountID,
		Email:        result.Email,
		PlanType:     result.PlanType,
		Action:       "oauth_refresh",
	}, nil
}

func hasPayloadString(payload map[string]any, keys ...string) bool {
	for _, key := range keys {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func copyImportControlFields(src map[string]any, dst map[string]any) {
	if len(src) == 0 || dst == nil {
		return
	}
	for _, key := range []string{"_share_enabled", "_share_status", "share_enabled", "shareEnabled", "share_status", "shareStatus"} {
		if value, ok := src[key]; ok {
			dst[key] = value
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
