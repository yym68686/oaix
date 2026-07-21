package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/agentidentity"
	"github.com/yym68686/oaix/internal/cooldown"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
)

type tokenProbeOutcome string

const (
	tokenProbeCompleted    tokenProbeOutcome = "completed"
	tokenProbeUsageLimited tokenProbeOutcome = "usage_limited"
	tokenProbeAuthRejected tokenProbeOutcome = "auth_rejected"
	tokenProbeDisabled     tokenProbeOutcome = "disabled"
	tokenProbeTransient    tokenProbeOutcome = "transient"
	tokenProbeCanceled     tokenProbeOutcome = "canceled"
	tokenProbeInconclusive tokenProbeOutcome = "inconclusive"
	probeRequestTimeout                      = 60 * time.Second
)

var errProbeTerminal = errors.New("probe terminal event received")

type tokenProbeAttempt struct {
	Outcome           tokenProbeOutcome
	StatusCode        int
	Detail            string
	RawResponse       string
	ResponseModel     string
	UsageLimit        cooldown.UsageLimit
	ErrorCode         string
	Stage             string
	UpstreamAttempted bool
}

func localTokenProbeFailure(status int, stage string, errorCode string, detail string) tokenProbeAttempt {
	return tokenProbeAttempt{
		Outcome:    tokenProbeInconclusive,
		StatusCode: status,
		Detail:     detail,
		ErrorCode:  errorCode,
		Stage:      stage,
	}
}

func (a *App) executeTokenProbe(parent context.Context, token store.Token, model string) tokenProbeAttempt {
	baseURL := strings.TrimSpace(a.cfg.Upstream.ResponsesURL)
	if baseURL == "" {
		return localTokenProbeFailure(http.StatusInternalServerError, probeStageLocalPreflight, "upstream_url_missing", "CODEX_BASE_URL is empty")
	}
	body, err := json.Marshal(map[string]any{
		"model":        model,
		"stream":       true,
		"store":        false,
		"instructions": "",
		"input": []map[string]any{{
			"type": "message",
			"role": "user",
			"content": []map[string]any{{
				"type": "input_text",
				"text": adminTokenProbeInput,
			}},
		}},
	})
	if err != nil {
		return localTokenProbeFailure(http.StatusInternalServerError, probeStageLocalPreflight, "request_encode_failed", "failed to encode probe request")
	}
	ctx, cancel := context.WithTimeout(parent, probeRequestTimeout)
	defer cancel()
	authorization, authorizationFailure, ok := a.prepareTokenProbeAuthorization(ctx, token)
	if !ok {
		return authorizationFailure
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(body))
	if err != nil {
		return localTokenProbeFailure(http.StatusInternalServerError, probeStageLocalPreflight, "request_build_failed", "failed to construct upstream request")
	}
	req.Header.Set("Authorization", authorization.Value)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Openai-Beta", "responses=experimental")
	req.Header.Set("Originator", codexOriginator)
	req.Header.Set("User-Agent", codexUserAgent)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Session_id", fmt.Sprintf("oaix-admin-probe-%d", token.ID))
	if authorization.AccountID != "" {
		req.Header.Set("Chatgpt-Account-Id", authorization.AccountID)
	}
	if authorization.FedRAMP {
		req.Header.Set("X-OpenAI-FedRAMP", "true")
	}
	resp, err := a.tokenProbeRequestDoer().Do(ctx, req)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return tokenProbeAttempt{
				Outcome: tokenProbeCanceled, Detail: "probe request canceled before a terminal response",
				ErrorCode: "request_canceled", Stage: probeStageUpstreamTransport, UpstreamAttempted: true,
			}
		}
		return tokenProbeAttempt{
			Outcome: tokenProbeTransient, StatusCode: http.StatusBadGateway, Detail: "upstream probe request failed",
			ErrorCode: "upstream_transport_error", Stage: probeStageUpstreamTransport, UpstreamAttempted: true,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		attempt := readProbeHTTPFailure(ctx, resp)
		attempt.Stage = probeStageUpstreamResponse
		attempt.UpstreamAttempted = true
		return redactAgentIdentityProbeAttempt(attempt, authorization.AgentIdentity)
	}
	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	if contentType != "" && !strings.HasPrefix(contentType, "text/event-stream") {
		body, readErr := readProbeBody(ctx, resp.Body)
		if readErr != nil {
			attempt := probeReadErrorAttempt(ctx, resp.StatusCode, readErr)
			attempt.RawResponse = string(body)
			return redactAgentIdentityProbeAttempt(attempt, authorization.AgentIdentity)
		}
		return redactAgentIdentityProbeAttempt(tokenProbeAttempt{
			Outcome:     tokenProbeInconclusive,
			StatusCode:  resp.StatusCode,
			Detail:      fmt.Sprintf("upstream probe returned unexpected content type %q", shortenError(contentType, 120)),
			RawResponse: string(body),
			ErrorCode:   "unexpected_content_type", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
		}, authorization.AgentIdentity)
	}
	attempt := readStrictProbeStream(ctx, resp.StatusCode, resp.Body, time.Now().UTC())
	attempt.Stage = probeStageUpstreamResponse
	attempt.UpstreamAttempted = true
	return redactAgentIdentityProbeAttempt(attempt, authorization.AgentIdentity)
}

func readProbeHTTPFailure(ctx context.Context, resp *http.Response) tokenProbeAttempt {
	body, err := readProbeBody(ctx, resp.Body)
	if err != nil {
		attempt := probeReadErrorAttempt(ctx, resp.StatusCode, err)
		attempt.RawResponse = string(body)
		return attempt
	}
	detail := responseErrorDetail(resp.StatusCode, body)
	usage := cooldown.ParseUsageLimit(resp.StatusCode, body, time.Now().UTC())
	errorCode := probeUpstreamErrorCode(body)
	if errorCode == "" {
		errorCode = "upstream_http_error"
	}
	result := tokenProbeAttempt{StatusCode: resp.StatusCode, Detail: detail, RawResponse: string(body), ErrorCode: errorCode}
	if usage.Matched && usage.ExplicitKind {
		result.Outcome = tokenProbeUsageLimited
		result.UsageLimit = usage
		return result
	}
	if quotaResponseShouldDisable(resp.StatusCode, body) {
		result.Outcome = tokenProbeDisabled
		return result
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		result.Outcome = tokenProbeAuthRejected
		return result
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		result.Outcome = tokenProbeTransient
		return result
	}
	result.Outcome = tokenProbeInconclusive
	return result
}

func readProbeBody(ctx context.Context, reader io.Reader) ([]byte, error) {
	limited := &io.LimitedReader{R: reader, N: defaultAdminProbeBodyLimit + 1}
	body, err := io.ReadAll(limited)
	if err != nil {
		return body, err
	}
	if err := ctx.Err(); err != nil {
		return body, err
	}
	if len(body) > defaultAdminProbeBodyLimit {
		return body, fmt.Errorf("probe response exceeds %d bytes", defaultAdminProbeBodyLimit)
	}
	return body, nil
}

func probeReadErrorAttempt(ctx context.Context, status int, err error) tokenProbeAttempt {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return tokenProbeAttempt{
			Outcome: tokenProbeCanceled, StatusCode: status, Detail: "probe response canceled before a terminal event",
			ErrorCode: "response_canceled", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
		}
	}
	return tokenProbeAttempt{
		Outcome: tokenProbeInconclusive, StatusCode: status, Detail: shortenError(err.Error(), 220),
		ErrorCode: "response_read_error", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
	}
}

func readStrictProbeStream(ctx context.Context, status int, reader io.Reader, now time.Time) tokenProbeAttempt {
	limited := &io.LimitedReader{R: reader, N: defaultAdminProbeBodyLimit + 1}
	result := tokenProbeAttempt{
		Outcome: tokenProbeInconclusive, StatusCode: status, Detail: "event stream ended before response.completed",
		ErrorCode: "incomplete_event_stream", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
	}
	var rawResponse bytes.Buffer
	eventCount := 0
	err := sse.NewParser(defaultAdminProbeBodyLimit).Parse(ctx, io.TeeReader(limited, &rawResponse), func(event sse.Event) error {
		if len(event.Data) == 0 {
			return nil
		}
		if string(bytes.TrimSpace(event.Data)) == "[DONE]" {
			return nil
		}
		eventCount++
		var payload map[string]any
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			return fmt.Errorf("malformed probe event JSON: %w", err)
		}
		eventType := strings.TrimSpace(event.Event)
		payloadType := stringFromAny(firstValue(payload, "type"))
		if eventType == "" {
			eventType = payloadType
		} else if payloadType != "" && payloadType != eventType {
			return fmt.Errorf("probe event type mismatch")
		}
		switch eventType {
		case "response.completed":
			response, ok := payload["response"].(map[string]any)
			if !ok {
				return fmt.Errorf("response.completed is missing response object")
			}
			if responseStatus := strings.TrimSpace(stringFromAny(firstValue(response, "status"))); responseStatus != "completed" {
				return fmt.Errorf("response.completed has non-completed status %q", responseStatus)
			}
			result = tokenProbeAttempt{
				Outcome:           tokenProbeCompleted,
				StatusCode:        status,
				ResponseModel:     strings.TrimSpace(stringFromAny(firstValue(response, "model"))),
				Stage:             probeStageUpstreamResponse,
				UpstreamAttempted: true,
			}
			return errProbeTerminal
		case "response.failed", "error":
			usage := cooldown.ParseUsageLimit(http.StatusTooManyRequests, event.Data, now)
			if usage.Matched && usage.ExplicitKind {
				result = tokenProbeAttempt{
					Outcome:    tokenProbeUsageLimited,
					StatusCode: http.StatusTooManyRequests,
					Detail:     probeFailureDetail(http.StatusTooManyRequests, event.Data),
					UsageLimit: usage,
					ErrorCode:  "usage_limit_reached", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
				}
			} else {
				errorCode := probeUpstreamErrorCode(event.Data)
				if errorCode == "" {
					errorCode = "upstream_stream_error"
				}
				result = tokenProbeAttempt{
					Outcome:    tokenProbeInconclusive,
					StatusCode: status,
					Detail:     probeFailureDetail(status, event.Data),
					ErrorCode:  errorCode, Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
				}
			}
			return errProbeTerminal
		}
		return nil
	})
	result.RawResponse = rawResponse.String()
	if errors.Is(err, errProbeTerminal) {
		return result
	}
	if err != nil {
		attempt := probeReadErrorAttempt(ctx, status, err)
		attempt.RawResponse = rawResponse.String()
		return attempt
	}
	if limited.N <= 0 {
		return tokenProbeAttempt{
			Outcome:     tokenProbeInconclusive,
			StatusCode:  status,
			Detail:      fmt.Sprintf("probe response exceeds %d bytes", defaultAdminProbeBodyLimit),
			RawResponse: rawResponse.String(),
			ErrorCode:   "response_too_large", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
		}
	}
	if eventCount == 0 {
		return tokenProbeAttempt{
			Outcome:     tokenProbeInconclusive,
			StatusCode:  status,
			Detail:      "event stream contained no response events",
			RawResponse: rawResponse.String(),
			ErrorCode:   "empty_event_stream", Stage: probeStageUpstreamResponse, UpstreamAttempted: true,
		}
	}
	return result
}

func (a *App) executeTokenProbeWithAuth(parent context.Context, token store.Token, model string) (tokenProbeAttempt, store.Token) {
	attempt := a.executeTokenProbe(parent, token, model)
	needsOAuthCredentialRefresh := attempt.Outcome == tokenProbeAuthRejected ||
		(attempt.Outcome == tokenProbeInconclusive && attempt.ErrorCode == "missing_access_token" && !token.IsAgentIdentity())
	if !needsOAuthCredentialRefresh {
		return attempt, token
	}
	if token.IsAgentIdentity() {
		if !agentidentity.IsInvalidTaskResponse(attempt.StatusCode, []byte(attempt.RawResponse)) {
			attempt.Outcome = tokenProbeDisabled
			attempt.ErrorCode = "agent_identity_auth_rejected"
			return attempt, token
		}
		credentials, err := a.loadAgentIdentityProbeCredentials(parent, token)
		if err != nil {
			attempt.Outcome = tokenProbeInconclusive
			attempt.Detail = "agent identity task recovery could not load credentials"
			attempt.ErrorCode = "agent_identity_credentials_unavailable"
			attempt.Stage = probeStageCredentialPreparation
			return attempt, token
		}
		recovered, err := a.recoverAgentIdentityProbeTask(parent, token, credentials.TaskID)
		if err != nil {
			if a.logger != nil {
				a.logger.Warn("manual token probe agent identity task recovery failed", "token_id", token.ID, "error", err)
			}
			attempt.Outcome = tokenProbeInconclusive
			attempt.Detail = "agent identity task recovery failed"
			attempt.ErrorCode = "agent_identity_task_recovery_failed"
			attempt.Stage = probeStageCredentialPreparation
			return attempt, token
		}
		token.AgentIdentity = &recovered
		retry := a.executeTokenProbe(parent, token, model)
		if retry.Outcome == tokenProbeAuthRejected {
			if agentidentity.IsInvalidTaskResponse(retry.StatusCode, []byte(retry.RawResponse)) {
				retry.Outcome = tokenProbeInconclusive
				retry.Detail = "upstream rejected the recovered agent identity task"
				retry.ErrorCode = "agent_identity_task_rejected_after_recovery"
			} else {
				retry.Outcome = tokenProbeDisabled
				retry.ErrorCode = "agent_identity_auth_rejected"
			}
		}
		return retry, token
	}

	if a.store != nil {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		latest, err := a.store.GetToken(ctx, token.ID)
		cancel()
		if err == nil && latest != nil {
			if strings.TrimSpace(latest.AccessToken) != "" && strings.TrimSpace(latest.AccessToken) != strings.TrimSpace(token.AccessToken) {
				token = *latest
				attempt = a.executeTokenProbe(parent, token, model)
				if attempt.Outcome != tokenProbeAuthRejected {
					return attempt, token
				}
			} else {
				token = *latest
			}
		}
	}
	if token.IsAccessTokenOnly() {
		attempt.Outcome = tokenProbeInconclusive
		attempt.Detail = "access-token-only credential was rejected; token state was preserved"
		attempt.ErrorCode = "oauth_refresh_unavailable"
		return attempt, token
	}

	if !token.HasRefreshableOAuthToken() {
		if attempt.Outcome == tokenProbeAuthRejected {
			attempt.ErrorCode = "oauth_refresh_unavailable"
		}
		return attempt, token
	}
	service := a.quota
	if service == nil {
		service = newAdminQuotaService(a.cfg, a.store, a.logger)
	}
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	refreshed, err := service.refreshQuotaToken(ctx, token)
	cancel()
	if err != nil {
		status := oauthRefreshErrorStatus(err.Error())
		if status == 0 {
			status = http.StatusBadGateway
		}
		return tokenProbeAttempt{
			Outcome:     tokenProbeInconclusive,
			StatusCode:  status,
			Detail:      "OAuth refresh did not produce usable credentials; token state was preserved",
			RawResponse: attempt.RawResponse,
			ErrorCode:   "oauth_refresh_failed", Stage: probeStageCredentialPreparation, UpstreamAttempted: attempt.UpstreamAttempted,
		}, token
	}
	return a.executeTokenProbe(parent, refreshed, model), refreshed
}

func probeUpstreamErrorCode(body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	for _, candidate := range []any{payload["error"], payload["response"]} {
		mapping, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		if nested, ok := mapping["error"].(map[string]any); ok {
			mapping = nested
		}
		if value := strings.TrimSpace(stringFromAny(firstValue(mapping, "code", "type"))); value != "" {
			return shortenError(value, 120)
		}
	}
	return strings.TrimSpace(stringFromAny(firstValue(payload, "code", "type")))
}
