package httpapi

import (
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

	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/store"
)

const (
	adminTokenProbeInput       = "say test"
	defaultAdminProbeModel     = "gpt-5.4-mini"
	defaultAdminProbeBodyLimit = 2 * 1024 * 1024
	adminProbeModelSettingKey  = "admin_token_probe_model"
	userProbeModelSettingKey   = "token_probe_model"
)

var supportedTokenProbeModels = []string{
	"gpt-5.4-mini",
	"gpt-5.4",
	"gpt-5.5",
	"gpt-5.6-sol",
	"gpt-5.6-terra",
	"gpt-5.6-luna",
}

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
	model := a.resolveTokenProbeModel(r.Context(), nil, payload.Model)
	result := a.probeTokenWithAccess(r.Context(), *token, model)
	writeJSON(w, http.StatusOK, result)
}

func (a *App) probeTokenWithAccess(parent context.Context, token store.Token, requestedModel string) map[string]any {
	model := strings.TrimSpace(requestedModel)
	if model == "" {
		model = normalizeConfiguredTokenProbeModel(firstEnv("ADMIN_TOKEN_PROBE_MODEL", ""))
	}
	if model == "" {
		model = defaultAdminProbeModel
	}
	attempt, probedToken := a.executeTokenProbeWithAuth(parent, token, model)
	statusCode := attempt.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusRequestTimeout
	}
	switch attempt.Outcome {
	case tokenProbeCompleted:
		if err := a.markProbeSuccess(token, probedToken); err != nil {
			return tokenProbeResult(token.ID, "inconclusive", http.StatusServiceUnavailable, "测试已完成，但保存恢复状态失败；当前状态未被报告为可用。", err.Error(), model)
		}
		result := tokenProbeResult(token.ID, "reactivated", statusCode, "测试成功：收到完整的 response.completed，当前已标记为可用。", "", model)
		if attempt.ResponseModel != "" {
			result["response_model"] = attempt.ResponseModel
		}
		return result
	case tokenProbeUsageLimited:
		now := time.Now().UTC()
		until := manualProbeUsageLimitCooldown(token.CooldownUntil, attempt.UsageLimit.ResetAt, now, a.cfg.TokenPool.DefaultCooldown)
		if err := a.markProbeUsageLimit(token, probedToken, attempt.Detail, until, model); err != nil {
			return tokenProbeResult(token.ID, "inconclusive", http.StatusServiceUnavailable, "测试确认仍受额度限制，但保存冷却状态失败。", err.Error(), model)
		}
		result := tokenProbeResultWithRawResponse(token.ID, "cooling", statusCode, "测试结果：上游明确返回 usage_limit_reached，当前保持冷却。", attempt.Detail, model, attempt.RawResponse)
		result["cooldown_seconds"] = cooldownSeconds(now, until)
		return result
	case tokenProbeDisabled:
		if err := a.markProbeDisabled(token, probedToken, attempt.Detail, false, statusCode, model); err != nil {
			return tokenProbeResult(token.ID, "inconclusive", http.StatusServiceUnavailable, "测试确认工作区已停用，但保存禁用状态失败。", err.Error(), model)
		}
		return tokenProbeResultWithRawResponse(token.ID, "disabled", statusCode, "测试失败：上游明确返回工作区已停用，当前已标记为禁用。", attempt.Detail, model, attempt.RawResponse)
	case tokenProbeCanceled:
		return tokenProbeResultWithRawResponse(token.ID, "inconclusive", statusCode, "测试在完整终止事件前被取消，当前状态未改变。", attempt.Detail, model, attempt.RawResponse)
	case tokenProbeAuthRejected:
		return tokenProbeResultWithRawResponse(token.ID, "inconclusive", statusCode, "刷新凭据后上游仍拒绝鉴权，当前状态未改变。", attempt.Detail, model, attempt.RawResponse)
	case tokenProbeTransient:
		return tokenProbeResultWithRawResponse(token.ID, "inconclusive", statusCode, "上游暂时不可用或限流，当前状态未改变。", attempt.Detail, model, attempt.RawResponse)
	default:
		return tokenProbeResultWithRawResponse(token.ID, "inconclusive", statusCode, "测试未收到可信的完整成功事件，当前状态未改变。", attempt.Detail, model, attempt.RawResponse)
	}
}

func (a *App) refreshProbeAccessToken(parent context.Context, token store.Token, model string) (store.Token, map[string]any) {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	service := a.quota
	if service == nil {
		service = newAdminQuotaService(a.cfg, a.store, a.logger)
	}
	refreshed, err := service.refreshQuotaToken(ctx, token)
	if err != nil {
		detail := err.Error()
		status := oauthRefreshErrorStatus(detail)
		if status == 0 {
			status = http.StatusInternalServerError
		}
		if isPermanentlyInvalidRefreshTokenError(status, detail) {
			if err := a.markProbeDisabled(token, token, detail, true, status, model); err != nil {
				return token, tokenProbeResult(token.ID, "inconclusive", http.StatusServiceUnavailable, "测试确认 refresh token 已失效，但保存禁用状态失败；当前状态未被报告为禁用。", err.Error(), model)
			}
			return token, tokenProbeResult(token.ID, "disabled", status, "测试失败：refresh token 已失效，当前已标记为禁用。", detail, model)
		}
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			return token, tokenProbeResult(token.ID, "inconclusive", status, fmt.Sprintf("测试未得出结论：刷新最新 access token 失败（%d），当前状态未改变。", status), detail, model)
		}
		return token, tokenProbeResult(token.ID, "inconclusive", status, fmt.Sprintf("测试未得出结论：刷新最新 access token 时返回 %d，当前状态未改变。", status), detail, model)
	}
	return refreshed, nil
}

func (a *App) resolveTokenProbeModel(parent context.Context, ownerUserID *int64, requestedModel string) string {
	if model := strings.TrimSpace(requestedModel); model != "" {
		return model
	}
	if a != nil && a.store != nil {
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		defer cancel()
		if ownerUserID != nil && *ownerUserID > 0 {
			item, err := a.store.GetUserSetting(ctx, *ownerUserID, userProbeModelSettingKey)
			if err == nil {
				if model := tokenProbeModelFromSetting(item.Value); model != "" {
					return model
				}
			}
		} else {
			item, err := a.store.GetSetting(ctx, adminProbeModelSettingKey)
			if err == nil {
				if model := tokenProbeModelFromSetting(item.Value); model != "" {
					return model
				}
			}
		}
	}
	if model := normalizeConfiguredTokenProbeModel(firstEnv("ADMIN_TOKEN_PROBE_MODEL", "")); model != "" {
		return model
	}
	return defaultAdminProbeModel
}

func tokenProbeModelFromSetting(raw json.RawMessage) string {
	var payload struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(raw, &payload); err == nil {
		if model := normalizeConfiguredTokenProbeModel(payload.Model); model != "" {
			return model
		}
	}
	var scalar string
	if err := json.Unmarshal(raw, &scalar); err == nil {
		return normalizeConfiguredTokenProbeModel(scalar)
	}
	return ""
}

func normalizeConfiguredTokenProbeModel(value string) string {
	model := strings.TrimSpace(value)
	if model == "" {
		return ""
	}
	for _, supported := range supportedTokenProbeModels {
		if model == supported {
			return model
		}
	}
	return ""
}

func (a *App) markProbeSuccess(observed store.Token, probed store.Token) error {
	if a.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.MarkManualProbeSuccess(ctx, observed.ID, manualProbeStateFence(observed, probed)); err != nil {
		return err
	}
	if a.tokens != nil {
		if err := a.tokens.Refresh(ctx); err != nil {
			if a.logger != nil {
				a.logger.Warn("token snapshot refresh after manual probe success failed", "token_id", observed.ID, "error", err)
			}
		}
	}
	return nil
}

func (a *App) markProbeUsageLimit(observed store.Token, probed store.Token, detail string, cooldownUntil *time.Time, model string) error {
	if a.store == nil || cooldownUntil == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.MarkManualProbeUsageLimit(ctx, observed.ID, manualProbeStateFence(observed, probed), detail, *cooldownUntil, model); err != nil {
		return err
	}
	if a.tokens != nil {
		if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
			a.logger.Warn("token snapshot refresh after manual usage-limit probe failed", "token_id", observed.ID, "error", err)
		}
	}
	return nil
}

func manualProbeStateFence(observed store.Token, probed store.Token) store.ManualProbeStateFence {
	return store.ManualProbeStateFence{
		IsActive:      observed.IsActive,
		DisabledAt:    observed.DisabledAt,
		CooldownUntil: observed.CooldownUntil,
		LastError:     observed.LastError,
		Credentials:   quotaRecoveryCredentialFence(probed),
	}
}

func (a *App) markProbeDisabled(observed store.Token, probed store.Token, detail string, clearAccess bool, statusCode int, model string) error {
	if a.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.store.MarkManualProbeDisabled(ctx, observed.ID, manualProbeStateFence(observed, probed), detail, clearAccess, statusCode, model)
	if err == nil && a.tokens != nil {
		if refreshErr := a.tokens.Refresh(ctx); refreshErr != nil && a.logger != nil {
			a.logger.Warn("token snapshot refresh after manual probe disable failed", "token_id", observed.ID, "error", refreshErr)
		}
	}
	return err
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

func tokenProbeResultWithRawResponse(id int64, outcome string, statusCode int, message string, detail string, model string, rawResponse string) map[string]any {
	result := tokenProbeResult(id, outcome, statusCode, message, detail, model)
	if rawResponse != "" {
		result["raw_response"] = rawResponse
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

func defaultCooldownUntil(now time.Time, duration time.Duration) *time.Time {
	if duration <= 0 {
		duration = 300 * time.Second
	}
	until := now.UTC().Add(duration)
	return &until
}

func manualProbeUsageLimitCooldown(previous *time.Time, resetAt *time.Time, now time.Time, fallback time.Duration) *time.Time {
	if resetAt != nil && resetAt.After(now) {
		value := resetAt.UTC()
		return &value
	}
	if previous != nil && previous.After(now) {
		value := previous.UTC()
		return &value
	}
	return defaultCooldownUntil(now, fallback)
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

func oauthRefreshErrorStatus(detail string) int {
	return oauth.RefreshErrorStatus(detail)
}

func isPermanentlyInvalidRefreshTokenError(status int, detail string) bool {
	return oauth.IsPermanentlyInvalidRefreshTokenError(status, detail)
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
