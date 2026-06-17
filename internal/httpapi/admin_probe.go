package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

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
	accessToken := strings.TrimSpace(token.AccessToken)
	if accessToken == "" {
		return tokenProbeResult(token.ID, "inconclusive", 400, "测试未得出结论：该 key 缺少 access token，当前状态未改变。", "missing access token", model)
	}
	body, _ := json.Marshal(map[string]any{
		"model":  model,
		"stream": false,
		"store":  false,
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
	req.Header.Set("Accept", "application/json")
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
		a.markProbeSuccess(token.ID)
		result := tokenProbeResult(token.ID, "reactivated", resp.StatusCode, "测试成功：使用当前 access token 请求上游成功，当前已标记为可用。", "", model)
		if responseModel := extractProbeResponseModel(respBody); responseModel != "" {
			result["response_model"] = responseModel
		}
		return result
	}

	if isUsageLimitError(resp.StatusCode, detail) {
		cooldown := a.cfg.TokenPool.DefaultCooldown
		if cooldown <= 0 {
			cooldown = 300 * time.Second
		}
		until := time.Now().UTC().Add(cooldown)
		a.markProbeError(token.ID, detail, false, &until)
		result := tokenProbeResult(token.ID, "cooling", resp.StatusCode, "测试结果：上游返回额度限制，当前已转为冷却。", detail, model)
		result["cooldown_seconds"] = int(cooldown.Seconds())
		return result
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		a.markProbeError(token.ID, detail, true, nil)
		return tokenProbeResult(token.ID, "disabled", resp.StatusCode, "测试失败：上游返回鉴权/权限错误，当前已标记为禁用。", detail, model)
	}

	return tokenProbeResult(token.ID, "inconclusive", resp.StatusCode, "测试未得出结论：上游返回非成功状态，当前状态未改变。", detail, model)
}

func (a *App) markProbeSuccess(tokenID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.MarkTokenSuccess(ctx, tokenID); err == nil && a.tokens != nil {
		_ = a.tokens.Refresh(ctx)
	}
}

func (a *App) markProbeError(tokenID int64, detail string, deactivate bool, cooldownUntil *time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.store.MarkTokenError(ctx, tokenID, detail, deactivate, cooldownUntil); err == nil && a.tokens != nil {
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

func extractProbeResponseModel(body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	if value, ok := payload["model"].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}
