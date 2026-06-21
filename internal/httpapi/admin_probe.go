package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/cooldown"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
)

const (
	adminTokenProbeInput       = "say test"
	defaultAdminProbeModel     = "gpt-5.5"
	defaultAdminProbeBodyLimit = 2 * 1024 * 1024
)

func (a *App) listTokenQuota(w http.ResponseWriter, r *http.Request) {
	ids, err := parseAdminTokenIDs(r.URL.Query().Get("ids"), 100)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"items": []adminTokenItem{}})
		return
	}
	if queryBool(r, "force_refresh", false) && a.quota != nil {
		a.quota.clear(ids)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	tokens := make([]store.Token, 0, len(ids))
	for _, id := range ids {
		token, err := a.store.GetToken(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err)
			return
		}
		tokens = append(tokens, *token)
	}
	items, pendingIDs := a.adminTokenItems(r.Context(), tokens, true)
	writeJSON(w, http.StatusOK, map[string]any{
		"items":                     items,
		"quota_refresh_pending_ids": pendingIDs,
	})
}

func (a *App) probeToken(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("invalid token id"))
		return
	}
	var payload struct {
		Model string `json:"model"`
	}
	if r.Body != nil {
		defer r.Body.Close()
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
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
	result := a.probeTokenWithAccess(r.Context(), *token, payload.Model)
	writeJSON(w, http.StatusOK, result)
}

func (a *App) probeTokenWithAccess(parent context.Context, token store.Token, requestedModel string) map[string]any {
	model := strings.TrimSpace(requestedModel)
	if model == "" {
		model = firstEnv("ADMIN_TOKEN_PROBE_MODEL", defaultAdminProbeModel)
	}
	baseURL := strings.TrimSpace(a.cfg.Upstream.ResponsesURL)
	if baseURL == "" {
		return tokenProbeResult(token.ID, "inconclusive", 500, "测试未得出结论：CODEX_BASE_URL 为空，当前状态未改变。", "CODEX_BASE_URL is empty", model)
	}
	if strings.TrimSpace(token.RefreshToken) != "" {
		refreshed, result := a.refreshProbeAccessToken(parent, token, model)
		if result != nil {
			return result
		}
		token = refreshed
	}
	accessToken := strings.TrimSpace(token.AccessToken)
	if accessToken == "" {
		return tokenProbeResult(token.ID, "inconclusive", 400, "测试未得出结论：该 key 缺少 access token，当前状态未改变。", "missing access token", model)
	}
	body, _ := json.Marshal(map[string]any{
		"model":        model,
		"stream":       true,
		"store":        false,
		"instructions": "",
		"input": []map[string]any{
			{
				"type": "message",
				"role": "user",
				"content": []map[string]any{
					{"type": "input_text", "text": adminTokenProbeInput},
				},
			},
		},
	})
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(body))
	if err != nil {
		return tokenProbeResult(token.ID, "inconclusive", 500, "测试未得出结论：构造上游请求失败，当前状态未改变。", err.Error(), model)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Openai-Beta", "responses=experimental")
	req.Header.Set("Originator", "codex_cli_rs")
	req.Header.Set("Version", "0.125.0")
	req.Header.Set("User-Agent", "codex_cli_rs/0.125.0")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Session_id", fmt.Sprintf("oaix-admin-probe-%d", token.ID))
	if token.AccountID != nil && strings.TrimSpace(*token.AccountID) != "" {
		req.Header.Set("Chatgpt-Account-Id", strings.TrimSpace(*token.AccountID))
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return tokenProbeResult(token.ID, "inconclusive", 502, "测试未得出结论：上游请求失败，当前状态未改变。", err.Error(), model)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, defaultAdminProbeBodyLimit))
	detail := responseErrorDetail(resp.StatusCode, respBody)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		streamDetail, streamRaw, responseModel := extractProbeStreamResult(parent, respBody)
		if streamDetail != "" {
			if isProbeUsageLimit(resp.StatusCode, streamDetail) {
				now := time.Now().UTC()
				until := cooldown.UsageLimitUntil(http.StatusTooManyRequests, []byte(streamRaw), now, a.cfg.TokenPool.DefaultCooldown)
				if until == nil {
					until = defaultCooldownUntil(now, a.cfg.TokenPool.DefaultCooldown)
				}
				a.markProbeError(token.ID, streamDetail, false, until, false)
				result := tokenProbeResult(token.ID, "cooling", http.StatusTooManyRequests, "测试结果：上游返回额度限制，当前已转为冷却。", streamDetail, model)
				result["cooldown_seconds"] = cooldownSeconds(now, until)
				return result
			}
			if isPermanentProbeDisableError(resp.StatusCode, streamDetail) {
				a.markProbeError(token.ID, streamDetail, true, nil, true)
				return tokenProbeResult(token.ID, "disabled", http.StatusForbidden, "测试失败：使用最新 access token 请求上游后仍返回鉴权/停用错误，当前已标记为禁用。", streamDetail, model)
			}
			return tokenProbeResult(token.ID, "inconclusive", resp.StatusCode, "测试未得出结论：上游流式事件失败，当前状态未改变。", streamDetail, model)
		}
		a.markProbeSuccess(token.ID)
		result := tokenProbeResult(token.ID, "reactivated", resp.StatusCode, "测试成功：使用最新 access token 请求上游成功，当前已标记为可用。", "", model)
		if responseModel == "" {
			responseModel = extractProbeResponseModel(respBody)
		}
		if responseModel != "" {
			result["response_model"] = responseModel
		}
		return result
	}

	if isUsageLimitError(resp.StatusCode, detail) {
		now := time.Now().UTC()
		until := cooldown.UsageLimitUntil(resp.StatusCode, respBody, now, a.cfg.TokenPool.DefaultCooldown)
		if until == nil {
			until = defaultCooldownUntil(now, a.cfg.TokenPool.DefaultCooldown)
		}
		a.markProbeError(token.ID, detail, false, until, false)
		result := tokenProbeResult(token.ID, "cooling", resp.StatusCode, "测试结果：上游返回额度限制，当前已转为冷却。", detail, model)
		result["cooldown_seconds"] = cooldownSeconds(now, until)
		return result
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || isPermanentProbeDisableError(resp.StatusCode, detail) {
		a.markProbeError(token.ID, detail, true, nil, true)
		return tokenProbeResult(token.ID, "disabled", resp.StatusCode, "测试失败：使用最新 access token 请求上游后仍返回鉴权/停用错误，当前已标记为禁用。", detail, model)
	}

	return tokenProbeResult(token.ID, "inconclusive", resp.StatusCode, "测试未得出结论：上游返回非成功状态，当前状态未改变。", detail, model)
}

func (a *App) refreshProbeAccessToken(parent context.Context, token store.Token, model string) (store.Token, map[string]any) {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	client := oauth.NewHTTPClient(a.cfg.Upstream.OAuthTokenURL)
	client.ClientID = a.cfg.Upstream.OAuthClientID
	client.Scope = a.cfg.Upstream.OAuthScope
	result, err := client.Refresh(ctx, strings.TrimSpace(token.RefreshToken))
	if err != nil {
		detail := err.Error()
		status := oauthRefreshErrorStatus(detail)
		if status == 0 {
			status = http.StatusInternalServerError
		}
		if isPermanentlyInvalidRefreshTokenError(status, detail) {
			a.markProbeError(token.ID, detail, true, nil, true)
			return token, tokenProbeResult(token.ID, "disabled", status, "测试失败：refresh token 已失效，当前已标记为禁用。", detail, model)
		}
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			a.markProbeError(token.ID, detail, false, nil, true)
			return token, tokenProbeResult(token.ID, "inconclusive", status, fmt.Sprintf("测试未得出结论：刷新最新 access token 失败（%d），当前状态未改变。", status), detail, model)
		}
		return token, tokenProbeResult(token.ID, "inconclusive", status, fmt.Sprintf("测试未得出结论：刷新最新 access token 时返回 %d，当前状态未改变。", status), detail, model)
	}
	expiresAt := (*time.Time)(nil)
	if result.ExpiresIn > 0 {
		expires := time.Now().UTC().Add(time.Duration(result.ExpiresIn) * time.Second)
		expiresAt = &expires
	}
	if a.store != nil {
		if err := a.store.UpdateTokenSecret(ctx, store.TokenSecretUpdate{
			TokenID:            token.ID,
			AccessToken:        result.AccessToken,
			RefreshToken:       result.RefreshToken,
			IDToken:            result.IDToken,
			ExpiresAt:          expiresAt,
			AccountID:          result.AccountID,
			Email:              result.Email,
			PlanType:           result.PlanType,
			PreserveActivation: true,
		}); err != nil {
			return token, tokenProbeResult(token.ID, "inconclusive", http.StatusServiceUnavailable, "测试未得出结论：保存最新 access token 失败，当前状态未改变。", err.Error(), model)
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

func (a *App) markProbeSuccess(tokenID int64) {
	if a.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.MarkTokenSuccess(ctx, tokenID); err == nil && a.tokens != nil {
		_ = a.tokens.Refresh(ctx)
	}
}

func (a *App) markProbeError(tokenID int64, detail string, deactivate bool, cooldownUntil *time.Time, clearAccess bool) {
	if a.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.store.MarkTokenError(ctx, tokenID, detail, deactivate, cooldownUntil)
	if err == nil && clearAccess {
		err = a.store.ClearTokenAccess(ctx, tokenID)
	}
	if err == nil && a.tokens != nil {
		_ = a.tokens.Refresh(ctx)
	}
}

func tokenProbeResult(id int64, outcome string, statusCode int, message string, detail string, model string) map[string]any {
	result := map[string]any{
		"id":          id,
		"outcome":     outcome,
		"status_code": statusCode,
		"message":     message,
		"probe_model": model,
		"probe_input": adminTokenProbeInput,
	}
	if strings.TrimSpace(detail) == "" {
		result["detail"] = nil
	} else {
		result["detail"] = shortenError(detail, 1000)
	}
	return result
}

func parseAdminTokenIDs(value string, limit int) ([]int64, error) {
	if limit <= 0 {
		limit = 100
	}
	seen := map[int64]struct{}{}
	var ids []int64
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\n' || r == '\t' || r == '\r'
	})
	for _, part := range parts {
		raw := strings.TrimSpace(part)
		if raw == "" {
			continue
		}
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id <= 0 {
			return nil, errors.New("ids must be a comma-separated list of positive integers")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
		if len(ids) >= limit {
			break
		}
	}
	return ids, nil
}

func isUsageLimitError(status int, detail string) bool {
	if status != http.StatusTooManyRequests {
		return false
	}
	lowered := strings.ToLower(detail)
	return strings.Contains(lowered, "usage_limit_reached") || strings.Contains(lowered, "usage limit")
}

func defaultCooldownUntil(now time.Time, duration time.Duration) *time.Time {
	if duration <= 0 {
		duration = 300 * time.Second
	}
	until := now.UTC().Add(duration)
	return &until
}

func cooldownSeconds(now time.Time, until *time.Time) int {
	if until == nil {
		return 0
	}
	seconds := int(until.Sub(now.UTC()).Seconds())
	if seconds < 0 {
		return 0
	}
	return seconds
}

func isProbeUsageLimit(status int, detail string) bool {
	if isUsageLimitError(status, detail) {
		return true
	}
	lowered := strings.ToLower(detail)
	return strings.Contains(lowered, "rate_limit_exceeded") ||
		strings.Contains(lowered, "concurrency limit exceeded") ||
		strings.Contains(lowered, "usage_limit_reached") ||
		strings.Contains(lowered, "usage limit")
}

func isPermanentProbeDisableError(status int, detail string) bool {
	lowered := strings.ToLower(detail)
	if status == http.StatusPaymentRequired && strings.Contains(lowered, "deactivated_workspace") {
		return true
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return true
	}
	for _, marker := range []string{
		"account_deactivated",
		"token_invalidated",
		"deactivated_workspace",
		"authentication token has been invalidated",
		"app_session_terminated",
		"session_terminated",
		"your session has ended",
		"please log in again",
	} {
		if strings.Contains(lowered, marker) {
			return true
		}
	}
	return false
}

func oauthRefreshErrorStatus(detail string) int {
	var status int
	if _, err := fmt.Sscanf(detail, "oauth refresh failed with status %d:", &status); err == nil {
		return status
	}
	return 0
}

func isPermanentlyInvalidRefreshTokenError(status int, detail string) bool {
	if status != 0 && status != http.StatusBadRequest && status != http.StatusUnauthorized && status != http.StatusForbidden {
		return false
	}
	lowered := strings.ToLower(detail)
	for _, marker := range []string{
		"invalid_grant",
		"invalid_refresh_token",
		"refresh_token_expired",
		"refresh_token_not_found",
		"refresh_token_reused",
		"refresh_token_revoked",
		"app_session_terminated",
		"session_terminated",
		"token_expired",
		"token_revoked",
		"already been used to generate a new access token",
		"invalid refresh token",
		"please try signing in again",
		"please log in again",
		"refresh token expired",
		"refresh token is invalid",
		"refresh token not found",
		"refresh token revoked",
		"session has ended",
		"your session has ended",
	} {
		if strings.Contains(lowered, marker) {
			return true
		}
	}
	return false
}

func extractProbeStreamResult(ctx context.Context, body []byte) (string, string, string) {
	if !bytes.Contains(body, []byte("data:")) && !bytes.Contains(body, []byte("event:")) {
		return "", "", ""
	}
	var failure string
	var failureRaw string
	var responseModel string
	_ = sse.NewParser(defaultAdminProbeBodyLimit).Parse(ctx, bytes.NewReader(body), func(event sse.Event) error {
		var payload map[string]any
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			return nil
		}
		eventType := strings.TrimSpace(event.Event)
		if eventType == "" {
			eventType = stringFromAny(firstValue(payload, "type"))
		}
		if eventType == "response.failed" {
			failure = probeFailureDetail(http.StatusOK, event.Data)
			failureRaw = string(event.Data)
			return nil
		}
		if eventType == "response.completed" {
			if model := extractProbeResponseModel(event.Data); model != "" {
				responseModel = model
				return nil
			}
			if response, ok := payload["response"].(map[string]any); ok {
				responseModel = stringFromAny(firstValue(response, "model"))
			}
		}
		return nil
	})
	return failure, failureRaw, responseModel
}

func probeFailureDetail(status int, body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		if response, ok := payload["response"].(map[string]any); ok {
			if errorValue, ok := response["error"].(map[string]any); ok {
				if message := responseValueText(firstValue(errorValue, "message", "type", "code")); message != nil {
					return fmt.Sprintf("HTTP %d: %s", status, *message)
				}
			}
		}
	}
	return responseErrorDetail(status, body)
}

func stringFromAny(value any) string {
	if text := normalizeText(value); text != nil {
		return *text
	}
	return ""
}

func extractProbeResponseModel(body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	if value, ok := payload["model"].(string); ok {
		return strings.TrimSpace(value)
	}
	if response, ok := payload["response"].(map[string]any); ok {
		if value, ok := response["model"].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
