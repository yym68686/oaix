package importer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
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
	AccessToken   string
	RefreshToken  string
	IDToken       string
	AccountID     string
	Email         string
	PlanType      string
	OAuthClientID string
	Action        string
	Payload       map[string]any
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
					code, excerpt := classifyRefreshImportError(err)
					updates[index] = store.ImportItemUpdate{
						ID:                         item.ID,
						Status:                     string(ItemFailed),
						ErrorMessage:               err.Error(),
						ValidationMS:               &elapsed,
						PublishAttempted:           false,
						PublishSkippedReason:       "validation_failed",
						RefreshErrorCode:           code,
						RefreshErrorMessageExcerpt: excerpt,
					}
					continue
				}
				atomic.AddInt64(&w.metrics.Validated, 1)
				validatedPayload := clonePayload(validated.Payload)
				if len(validatedPayload) == 0 {
					validatedPayload = map[string]any{
						"access_token":  validated.AccessToken,
						"refresh_token": validated.RefreshToken,
						"id_token":      validated.IDToken,
						"account_id":    validated.AccountID,
						"email":         validated.Email,
						"plan_type":     validated.PlanType,
					}
					if validated.OAuthClientID != "" {
						validatedPayload["client_id"] = validated.OAuthClientID
					}
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
	payload := flattenNestedCredentialsPayload(item.Payload)
	for _, key := range []string{"access_token", "accessToken", "token"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
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
	payload := flattenNestedCredentialsPayload(item.Payload)
	item.Payload = payload
	if _, ok := agentidentity.NormalizePayload(payload); ok {
		return AgentIdentityValidator{}.Validate(ctx, item)
	}
	if hasPayloadString(payload, "access_token", "accessToken", "token") {
		access := v.Access
		if access == nil {
			access = AccessTokenValidator{}
		}
		return access.Validate(ctx, item)
	}
	if hasPayloadString(payload, "refresh_token", "refreshToken") {
		refresh := v.Refresh
		if refresh == nil {
			return ValidatedItem{}, fmt.Errorf("oauth client is not configured")
		}
		return refresh.Validate(ctx, item)
	}
	return AccessTokenValidator{}.Validate(ctx, item)
}

type AgentIdentityValidator struct{}

func (AgentIdentityValidator) Validate(ctx context.Context, item store.ImportItem) (ValidatedItem, error) {
	select {
	case <-ctx.Done():
		return ValidatedItem{}, ctx.Err()
	default:
	}
	payload, ok := agentidentity.NormalizePayload(item.Payload)
	if !ok {
		return ValidatedItem{}, fmt.Errorf("import item %d is not an agent identity payload", item.ID)
	}
	if _, _, err := agentidentity.Parse(payload); err != nil {
		return ValidatedItem{}, fmt.Errorf("import item %d: %w", item.ID, err)
	}
	return ValidatedItem{Payload: payload, Action: "upsert_agent_identity"}, nil
}

func flattenNestedCredentialsPayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return payload
	}
	credentials, ok := payload["credentials"].(map[string]any)
	if !ok || len(credentials) == 0 {
		return payload
	}
	flattened := make(map[string]any, len(payload)+len(credentials))
	for key, value := range payload {
		if key != "credentials" {
			flattened[key] = value
		}
	}
	copyStringPayloadField(flattened, credentials, "refresh_token", "refresh_token")
	copyStringPayloadField(flattened, credentials, "refreshToken", "refresh_token")
	if _, hasRefresh := flattened["refresh_token"]; !hasRefresh {
		copyStringPayloadField(flattened, credentials, "access_token", "access_token")
		copyStringPayloadField(flattened, credentials, "accessToken", "access_token")
	}
	copyStringPayloadField(flattened, credentials, "account_id", "account_id")
	copyStringPayloadField(flattened, credentials, "chatgpt_account_id", "chatgpt_account_id")
	copyStringPayloadField(flattened, credentials, "chatgpt_user_id", "chatgpt_user_id")
	copyStringPayloadField(flattened, credentials, "organization_id", "organization_id")
	copyStringPayloadField(flattened, credentials, "email", "email")
	copyStringPayloadField(flattened, credentials, "id_token", "id_token")
	copyStringPayloadField(flattened, credentials, "client_id", "client_id")
	copyStringPayloadField(flattened, credentials, "clientId", "client_id")
	copyStringPayloadField(flattened, credentials, "oauth_client_id", "oauth_client_id")
	copyStringPayloadField(flattened, credentials, "plan_type", "plan_type")
	copyStringPayloadField(flattened, credentials, "type", "type")
	copyStringPayloadField(flattened, credentials, "auth_mode", "auth_mode")
	copyStringPayloadField(flattened, credentials, "authMode", "auth_mode")
	copyStringPayloadField(flattened, credentials, "agent_runtime_id", "agent_runtime_id")
	copyStringPayloadField(flattened, credentials, "agentRuntimeId", "agent_runtime_id")
	copyStringPayloadField(flattened, credentials, "agent_private_key", "agent_private_key")
	copyStringPayloadField(flattened, credentials, "agentPrivateKey", "agent_private_key")
	copyStringPayloadField(flattened, credentials, "task_id", "task_id")
	copyStringPayloadField(flattened, credentials, "taskId", "task_id")
	copyStringPayloadField(flattened, credentials, "workspace_id", "workspace_id")
	copyStringPayloadField(flattened, credentials, "workspaceId", "workspace_id")
	if value, ok := credentials["chatgpt_account_is_fedramp"].(bool); ok {
		flattened["chatgpt_account_is_fedramp"] = value
	}
	return flattened
}

func clonePayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func copyStringPayloadField(dst map[string]any, src map[string]any, srcKey string, dstKey string) {
	if _, exists := dst[dstKey]; exists {
		return
	}
	if value, ok := src[srcKey].(string); ok && strings.TrimSpace(value) != "" {
		dst[dstKey] = strings.TrimSpace(value)
	}
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
	clientID := oauthClientIDFromPayload(item.Payload)
	result, err := refreshOAuthItem(ctx, v.Client, clientID, refreshToken)
	if err != nil {
		return ValidatedItem{}, err
	}
	return ValidatedItem{
		AccessToken:   result.AccessToken,
		RefreshToken:  firstNonEmpty(result.RefreshToken, refreshToken),
		AccountID:     result.AccountID,
		Email:         result.Email,
		PlanType:      result.PlanType,
		OAuthClientID: clientID,
		Action:        "oauth_refresh",
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
	for _, key := range []string{"_share_enabled", "_share_status", "share_enabled", "shareEnabled", "share_status", "shareStatus", "client_id", "clientId", "oauth_client_id"} {
		if value, ok := src[key]; ok {
			dst[key] = value
		}
	}
}

func refreshOAuthItem(ctx context.Context, client oauth.Client, clientID string, refreshToken string) (oauth.RefreshResult, error) {
	if clientID != "" {
		if refresher, ok := client.(oauth.ClientIDRefresher); ok {
			return refresher.RefreshWithClientID(ctx, refreshToken, clientID)
		}
	}
	return client.Refresh(ctx, refreshToken)
}

func oauthClientIDFromPayload(payload map[string]any) string {
	return firstNonEmpty(payloadString(payload, "client_id"), payloadString(payload, "clientId"), payloadString(payload, "oauth_client_id"))
}

func payloadString(payload map[string]any, key string) string {
	if value, ok := payload[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func classifyRefreshImportError(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	message := strings.TrimSpace(err.Error())
	lower := strings.ToLower(message)
	code := "validation_failed"
	switch {
	case strings.Contains(lower, "refresh_token_reused"):
		code = "refresh_token_reused"
	case strings.Contains(lower, "invalid_grant"):
		code = "invalid_grant"
	case strings.Contains(lower, "missing access_token"):
		code = "oauth_response_missing_access_token"
	case strings.Contains(lower, "oauth refresh failed"):
		code = "oauth_refresh_failed"
	case strings.Contains(lower, "does not contain a refresh token"):
		code = "missing_refresh_token"
	case strings.Contains(lower, "does not contain an access token"):
		code = "missing_access_token"
	case strings.Contains(lower, "oauth client is not configured"):
		code = "oauth_client_not_configured"
	}
	if len(message) > 512 {
		message = message[:512]
	}
	return code, message
}
