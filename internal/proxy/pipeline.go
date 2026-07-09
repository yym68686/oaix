package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yym68686/oaix/internal/affinity"
	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/cooldown"
	"github.com/yym68686/oaix/internal/logs"
	"github.com/yym68686/oaix/internal/oauth"
	"github.com/yym68686/oaix/internal/protocol/openai"
	"github.com/yym68686/oaix/internal/protocol/sse"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
	"github.com/yym68686/oaix/internal/transport"
)

const (
	maxRejectedEncryptedContentRetries = 64
	authFailureRefreshTimeout          = 15 * time.Second
	authFailureCooldown                = 5 * time.Second
)

type Pipeline struct {
	cfg            config.Config
	logger         *slog.Logger
	tokens         *tokens.Manager
	transport      *transport.Client
	logs           *logs.Writer
	store          tokenStateStore
	affinity       affinity.Store
	oauthClient    oauth.Client
	commitFailures atomic.Int64
}

type tokenStateStore interface {
	MarkTokenSuccess(ctx context.Context, tokenID int64) error
	MarkTokenError(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error
	MarkTokenErrorWithContext(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time, eventCtx store.TokenStateEventContext) error
	InsertGatewayRequestAttempt(ctx context.Context, item store.GatewayRequestAttempt) (int64, error)
}

type tokenSecretUpdater interface {
	UpdateTokenSecret(ctx context.Context, update store.TokenSecretUpdate) error
}

type tokenOAuthClientIDStore interface {
	TokenOAuthClientID(ctx context.Context, tokenID int64) (string, error)
}

type RequestIntent struct {
	Endpoint            string
	Model               string
	OwnerUserID         int64
	APIKeyID            *int64
	SelectionMode       string
	CallerOwnerUserID   int64
	ExcludeOwnerUserID  int64
	TargetTokenID       int64
	Stream              bool
	Compact             bool
	Method              string
	UpstreamSuffix      string
	UpstreamEndpoint    string
	UpstreamContentType string
	UpstreamAccept      string
	ResponseModelAlias  string
	ImageResponseFormat string
	ImageStreamPrefix   string
}

type Attempt struct {
	Index       int
	RequestID   string
	Intent      RequestIntent
	Claim       *tokens.Claim
	Body        []byte
	StartedAt   time.Time
	RetryCause  Outcome
	UpstreamURL string
	Method      string
	PromptCache *PromptCacheContext
	StreamState StreamAttemptState
}

type AttemptResult struct {
	Status       int
	Retry        bool
	Committed    bool
	StreamState  StreamAttemptState
	Usage        *UsageMetrics
	ResponseID   string
	FirstTokenAt *time.Time
	ErrorBody    []byte
}

type StreamAttemptState struct {
	DownstreamStarted bool
	KeepaliveSent     bool
}

type Outcome string

const (
	OutcomeSuccess                Outcome = "success"
	OutcomeClientCanceled         Outcome = "client_canceled"
	OutcomeUpstream429Cooldown    Outcome = "upstream_429_cooldown"
	OutcomeUpstream401Invalid     Outcome = "upstream_401_invalid"
	OutcomeUpstream402Deactivated Outcome = "upstream_402_deactivated_workspace"
	OutcomeUpstream403Invalid     Outcome = "upstream_403_invalid"
	OutcomeUpstream4xx            Outcome = "upstream_4xx"
	OutcomeUpstream5xx            Outcome = "upstream_5xx"
	OutcomeTransportError         Outcome = "transport_error"
	OutcomeNoToken                Outcome = "no_token"
)

func New(cfg config.Config, logger *slog.Logger, tokenManager *tokens.Manager, client *transport.Client, writer *logs.Writer, stateStore tokenStateStore, affinityStore affinity.Store) *Pipeline {
	return &Pipeline{
		cfg:       cfg,
		logger:    logger,
		tokens:    tokenManager,
		transport: client,
		logs:      writer,
		store:     stateStore,
		affinity:  affinityStore,
	}
}

func (p *Pipeline) SetOAuthClient(client oauth.Client) {
	if p == nil {
		return
	}
	p.oauthClient = client
}

func (p *Pipeline) TransportStats() transport.Stats {
	return p.transport.Stats()
}

func (p *Pipeline) StateCommitFailures() int64 {
	return p.commitFailures.Load()
}

func (p *Pipeline) CloseIdleConnections() {
	if p == nil || p.transport == nil {
		return
	}
	p.transport.CloseIdleConnections()
}

func (p *Pipeline) Proxy(w http.ResponseWriter, r *http.Request, intent RequestIntent) {
	started := time.Now().UTC()
	requestID := requestID(r)
	w.Header().Set("X-Request-ID", requestID)
	bodyBytes, bodyStatus, bodyMessage, err := readProxyRequestBody(r, p.cfg.Upstream.MaxRequestBodyBytes)
	if err != nil {
		writeJSONError(w, bodyStatus, bodyMessage)
		return
	}
	bodyBytes, _ = sanitizeReasoningContentBody(bodyBytes)
	intent = normalizeIntent(intent, bodyBytes)
	var status int
	bodyBytes, intent, status, err = prepareUpstreamPayload(r, bodyBytes, intent)
	if err != nil {
		writeErrorResponse(w, intent.Stream, status, err.Error())
		return
	}
	promptCacheContext, upstreamBody := buildPromptCacheContext(r.Header, intent, bodyBytes, p.cfg.PromptCache)
	bodyBytes = upstreamBody
	if promptCacheContext != nil && intent.Model == "" {
		intent.Model = promptCacheContext.Model
	}
	timing := map[string]any{"request_body_bytes": len(bodyBytes)}
	if promptCacheContext != nil {
		timing["prompt_cache_key_hash"] = promptCacheContext.PromptCacheKeyHash
		timing["prompt_cache_source"] = promptCacheContext.Source
	}
	if strings.TrimSpace(intent.SelectionMode) != "" {
		timing["selection_mode"] = strings.TrimSpace(intent.SelectionMode)
	}
	if intent.CallerOwnerUserID > 0 {
		timing["caller_owner_user_id"] = intent.CallerOwnerUserID
	}
	if intent.ExcludeOwnerUserID > 0 {
		timing["exclude_owner_user_id"] = intent.ExcludeOwnerUserID
	}
	if intent.TargetTokenID > 0 {
		timing["target_token_id"] = intent.TargetTokenID
	}
	p.logs.Submit(r.Context(), store.RequestLog{
		RequestID:                     requestID,
		OwnerUserID:                   ptrInt64(intent.OwnerUserID),
		APIKeyID:                      intent.APIKeyID,
		SelectionMode:                 nullable(strings.TrimSpace(intent.SelectionMode)),
		CallerOwnerUserID:             ptrPositiveInt64(intent.CallerOwnerUserID),
		Endpoint:                      intent.Endpoint,
		Model:                         nullable(intent.Model),
		ModelName:                     nullable(intent.Model),
		IsStream:                      intent.Stream,
		StartedAt:                     started,
		ClientIP:                      nullable(clientIP(r)),
		UserAgent:                     nullable(r.UserAgent()),
		TimingSpans:                   timing,
		AttemptCount:                  0,
		RequestPayloadHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.RequestPayloadHash }),
		UpstreamPayloadHash:           promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.UpstreamPayloadHash }),
		PromptTemplateHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptTemplateHash }),
		PromptDynamicHash:             promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptDynamicHash }),
		PromptCacheSource:             promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.Source }),
		PromptCacheKeyHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheKeyHash }),
		PromptCacheRetentionRequested: promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheRetentionRequested }),
		PromptCacheRetentionSent:      promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheRetentionSent }),
		SessionIDHash:                 promptString(promptCacheContext, func(c *PromptCacheContext) string { return shortHash(c.SessionID, 64) }),
		SessionIDSource:               promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.SessionIDSource }),
		PreviousResponseIDHash:        promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PreviousResponseIDHash }),
		PromptCacheTrace:              promptTrace(promptCacheContext),
	}, false)

	var excluded = make(map[int64]struct{})
	var lastErr error
	var finalStatus = http.StatusBadGateway
	var selectedTokenID *int64
	var selectedTokenOwnerID *int64
	var accountID *string
	var lastAffinityResult tokens.PromptAffinityResult
	var lastUsage *UsageMetrics
	var lastResponseID string
	var lastFirstTokenAt *time.Time
	var streamState StreamAttemptState
	var refreshedAuthTokens = make(map[int64]struct{})
	for attempt := 1; attempt <= p.cfg.Upstream.MaxRetries; attempt++ {
		selectStarted := time.Now()
		tokenIntent := tokens.Intent{
			Endpoint:           intent.Endpoint,
			Model:              intent.Model,
			OwnerUserID:        intent.OwnerUserID,
			SelectionMode:      intent.SelectionMode,
			ExcludeOwnerUserID: intent.ExcludeOwnerUserID,
			TargetTokenID:      intent.TargetTokenID,
			ExcludeTokenIDs:    excluded,
		}
		if promptCacheContext != nil {
			tokenIntent.PromptCacheKeyHash = promptCacheContext.PromptCacheKeyHash
		}
		claim, affinityResult, err := p.claimToken(r.Context(), tokenIntent, promptCacheContext)
		lastAffinityResult = affinityResult
		if affinityResult.Result != "" {
			timing["cache_affinity_result"] = affinityResult.Result
		}
		if affinityResult.LaneIndex != nil {
			timing["cache_affinity_lane_index"] = *affinityResult.LaneIndex
		}
		if affinityResult.LaneCount > 0 {
			timing["cache_affinity_lane_count"] = affinityResult.LaneCount
		}
		timing["token_select_ms"] = int(time.Since(selectStarted).Milliseconds())
		p.recordClaimTiming(timing, claim, err)
		if err != nil {
			finalStatus = http.StatusServiceUnavailable
			lastErr = err
			p.recordNoTokenGatewayAttempt(requestID, intent, attempt, selectStarted.UTC(), err)
			break
		}
		selectedTokenID = ptrInt64(claim.TokenID())
		if claim.Token != nil {
			selectedTokenOwnerID = ptrInt64(claim.Token.Token.OwnerUserID)
			if isMarketplaceIntent(intent) {
				timing["marketplace_price_bps"] = claimMarketplacePriceBPS(claim)
				timing["marketplace_price_source"] = claimMarketplacePriceSource(claim)
				if claim.MarketplacePriceLocked {
					timing["marketplace_price_locked"] = true
					if claim.MarketplacePriceLockedAt != nil {
						timing["marketplace_price_locked_at"] = claim.MarketplacePriceLockedAt.UTC().Format(time.RFC3339Nano)
					}
					if claim.MarketplacePriceContractKey != "" {
						timing["marketplace_price_contract_key"] = claim.MarketplacePriceContractKey
					}
					if claim.MarketplacePriceLockStatus != "" {
						timing["marketplace_price_lock_status"] = claim.MarketplacePriceLockStatus
					}
				}
			}
		}
		accountID = claim.AccountID()
		attemptStarted := time.Now()
		attemptSpec := Attempt{
			Index:       attempt,
			RequestID:   requestID,
			Intent:      intent,
			Claim:       claim,
			Body:        bodyBytes,
			StartedAt:   attemptStarted.UTC(),
			RetryCause:  classify(finalStatus, lastErr),
			PromptCache: promptCacheContext,
			StreamState: streamState,
		}
		result, err := p.doAttempt(w, r, attemptSpec)
		encryptedContentRetryStarted := time.Now()
		encryptedContentRetryCount := 0
		for encryptedContentRetryCount < maxRejectedEncryptedContentRetries && !result.Committed && !result.StreamState.DownstreamStarted {
			marker, ok := rejectedEncryptedContentMarker(intent, result.Status, err, result.ErrorBody)
			if !ok {
				break
			}
			sanitizedBody, removed, changed := sanitizeRejectedEncryptedContentBody(bodyBytes, marker)
			if !changed {
				break
			}
			retryStarted := time.Now()
			retrySpec := attemptSpec
			retrySpec.Body = sanitizedBody
			retrySpec.RetryCause = classify(result.Status, err)
			bodyBytes = sanitizedBody
			encryptedContentRetryCount++
			timing["rejected_encrypted_content_retry"] = true
			timing["rejected_encrypted_content_retry_count"] = encryptedContentRetryCount
			timing["rejected_encrypted_content_removed_count"] = encryptedContentRetryCount
			timing["rejected_encrypted_content_last_removed_index"] = removed.Index
			timing["rejected_encrypted_content_last_removed_type"] = removed.Type
			result, err = p.doAttempt(w, r, retrySpec)
			timing["rejected_encrypted_content_retry_last_ms"] = int(time.Since(retryStarted).Milliseconds())
			timing["rejected_encrypted_content_retry_total_ms"] = int(time.Since(encryptedContentRetryStarted).Milliseconds())
		}
		if encryptedContentRetryCount >= maxRejectedEncryptedContentRetries {
			timing["rejected_encrypted_content_retry_limit_reached"] = true
		}
		if result.StreamState.DownstreamStarted {
			streamState.DownstreamStarted = true
		}
		if result.StreamState.KeepaliveSent {
			streamState.KeepaliveSent = true
		}
		status := result.Status
		retry := result.Retry
		committed := result.Committed
		if result.Usage != nil {
			lastUsage = result.Usage
		}
		if result.ResponseID != "" {
			lastResponseID = result.ResponseID
		}
		if result.FirstTokenAt != nil {
			lastFirstTokenAt = result.FirstTokenAt
		}
		timing["upstream_attempt_ms"] = int(time.Since(attemptStarted).Milliseconds())
		timing["upstream_attempt_count"] = attempt
		claim.Release()
		p.recordClaimReleaseTiming(timing, claim)
		finalStatus = status
		if committed {
			success := err == nil && status >= 200 && status < 400
			p.recordGatewayAttempt(context.Background(), attemptSpec, result, err, classify(status, err), retry, false, nil)
			var msg *string
			if err != nil {
				text := err.Error()
				msg = &text
			}
			p.finalLog(r.Context(), requestID, intent, started, finalStatus, success, attempt, selectedTokenID, selectedTokenOwnerID, accountID, msg, timing, promptCacheContext, lastAffinityResult, lastUsage, lastResponseID, lastFirstTokenAt)
			if success {
				p.commitSuccess(claim.TokenID())
				p.recordPromptCacheSuccess(promptCacheContext, claim, lastResponseID)
			}
			return
		}
		if err == nil && status >= 200 && status < 400 {
			p.recordGatewayAttempt(context.Background(), attemptSpec, result, err, OutcomeSuccess, retry, false, nil)
			p.commitSuccess(claim.TokenID())
			p.recordPromptCacheSuccess(promptCacheContext, claim, lastResponseID)
			p.finalLog(r.Context(), requestID, intent, started, finalStatus, true, attempt, selectedTokenID, selectedTokenOwnerID, accountID, nil, timing, promptCacheContext, lastAffinityResult, lastUsage, lastResponseID, lastFirstTokenAt)
			return
		}
		lastErr = err
		action := classify(status, err)
		if action == OutcomeClientCanceled {
			p.recordGatewayAttempt(context.Background(), attemptSpec, result, err, action, retry, false, nil)
			message := "client canceled"
			if err != nil {
				message = err.Error()
			}
			p.finalLog(context.Background(), requestID, intent, started, 499, false, attempt, selectedTokenID, selectedTokenOwnerID, accountID, &message, timing, promptCacheContext, lastAffinityResult, lastUsage, lastResponseID, lastFirstTokenAt)
			return
		}
		if isDeactivatedWorkspaceFailure(status, result.ErrorBody, err) {
			message := "terminal upstream status 402: deactivated_workspace"
			lastErr = errors.New(message)
			attemptID := p.recordGatewayAttempt(context.Background(), attemptSpec, result, lastErr, OutcomeUpstream402Deactivated, retry, true, nil)
			p.commitTokenError(claim.TokenID(), selectedTokenOwnerID, message, true, nil, p.tokenStateEventContext(requestID, intent, status, OutcomeUpstream402Deactivated, attemptID))
			p.tokens.RemovePromptAffinityToken(p.affinity, claim.TokenID())
			excluded[claim.TokenID()] = struct{}{}
			if attempt < p.cfg.Upstream.MaxRetries {
				continue
			}
			break
		}
		var (
			deactivate     bool
			cooldownUntil  *time.Time
			commitMessage  string
			commitRequired bool
			retrySameToken bool
		)
		if action == OutcomeUpstream401Invalid || action == OutcomeUpstream403Invalid {
			_, alreadyRefreshed := refreshedAuthTokens[claim.TokenID()]
			refreshResult := p.refreshAccessTokenAfterAuthFailure(r.Context(), claim, alreadyRefreshed)
			switch {
			case refreshResult.Refreshed:
				refreshedAuthTokens[claim.TokenID()] = struct{}{}
				retrySameToken = true
				timing["oauth_refresh_on_auth_failure"] = true
			case refreshResult.Inconclusive:
				cooldownUntil = authFailureCooldownUntil()
				commitRequired = true
				commitMessage = refreshResult.Message
				if commitMessage == "" {
					commitMessage = fmt.Sprintf("non-terminal upstream status %d after oauth refresh check", status)
				}
			default:
				deactivate = true
				commitRequired = true
				commitMessage = refreshResult.Message
				if commitMessage == "" {
					commitMessage = fmt.Sprintf("terminal upstream status %d", status)
				}
			}
		} else if action == OutcomeUpstream429Cooldown {
			now := time.Now().UTC()
			cooldownUntil = cooldown.UsageLimitUntil(status, result.ErrorBody, now, p.cfg.TokenPool.DefaultCooldown)
			commitMessage = "upstream 429 cooldown"
			if cooldownUntil == nil {
				fallback := p.cfg.TokenPool.DefaultCooldown
				if fallback <= 0 {
					fallback = 300 * time.Second
				}
				until := now.Add(fallback)
				cooldownUntil = &until
			} else {
				commitMessage = "upstream usage limit cooldown"
			}
			commitRequired = true
		} else if retry && (action == OutcomeUpstream5xx || action == OutcomeTransportError) {
			cooldown := time.Now().UTC().Add(5 * time.Second)
			cooldownUntil = &cooldown
			commitMessage = fmt.Sprintf("retryable upstream failure: %v", err)
			commitRequired = true
		}
		attemptID := p.recordGatewayAttempt(context.Background(), attemptSpec, result, err, action, retry, deactivate, cooldownUntil)
		if commitRequired {
			p.commitTokenError(claim.TokenID(), selectedTokenOwnerID, commitMessage, deactivate, cooldownUntil, p.tokenStateEventContext(requestID, intent, status, action, attemptID))
			if deactivate {
				p.tokens.RemovePromptAffinityToken(p.affinity, claim.TokenID())
			}
		}
		if retrySameToken {
			if attempt < p.cfg.Upstream.MaxRetries {
				continue
			}
			break
		}
		excluded[claim.TokenID()] = struct{}{}
		if !retry || attempt == p.cfg.Upstream.MaxRetries {
			break
		}
	}
	if errors.Is(lastErr, tokens.ErrNoToken) {
		writeFinalErrorResponse(w, intent.Stream, streamState.DownstreamStarted, http.StatusServiceUnavailable, "no available token")
		p.finalLog(r.Context(), requestID, intent, started, http.StatusServiceUnavailable, false, len(excluded), selectedTokenID, selectedTokenOwnerID, accountID, stringPtr("no available token"), timing, promptCacheContext, lastAffinityResult, lastUsage, lastResponseID, lastFirstTokenAt)
		return
	}
	message := "upstream request failed"
	if lastErr != nil {
		message = lastErr.Error()
	}
	writeFinalErrorResponse(w, intent.Stream, streamState.DownstreamStarted, finalStatus, message)
	p.finalLog(r.Context(), requestID, intent, started, finalStatus, false, len(excluded), selectedTokenID, selectedTokenOwnerID, accountID, &message, timing, promptCacheContext, lastAffinityResult, lastUsage, lastResponseID, lastFirstTokenAt)
}

func (p *Pipeline) claimToken(ctx context.Context, intent tokens.Intent, promptCacheContext *PromptCacheContext) (*tokens.Claim, tokens.PromptAffinityResult, error) {
	if promptCacheContext == nil {
		claim, err := p.tokens.Claim(ctx, intent)
		return claim, tokens.PromptAffinityResult{Result: claimReason(claim)}, err
	}
	return p.tokens.ClaimPromptAffinity(ctx, p.affinity, intent, tokens.PromptAffinityOptions{
		AffinityKey:           promptCacheContext.AffinityKey,
		PreviousResponseID:    promptCacheContext.PreviousResponseID,
		MaxLanesPerKey:        p.cfg.PromptCache.MaxLanesPerKey,
		PrimaryWait:           p.cfg.PromptCache.PrimaryWait,
		LaneWait:              p.cfg.PromptCache.LaneWait,
		PreviousOwnerWait:     p.cfg.PromptCache.PreviousOwnerWait,
		PreviousStrict:        p.cfg.PromptCache.PreviousStrict,
		GlobalFallbackEnabled: p.cfg.PromptCache.GlobalFallbackEnabled,
		LaneTTL:               p.cfg.PromptCache.LaneTTL,
		ResponseTTL:           p.cfg.PromptCache.ResponseTTL,
	})
}

type authFailureRefreshResult struct {
	Refreshed    bool
	Inconclusive bool
	Message      string
}

func (p *Pipeline) refreshAccessTokenAfterAuthFailure(parent context.Context, claim *tokens.Claim, alreadyRefreshed bool) authFailureRefreshResult {
	if alreadyRefreshed {
		return authFailureRefreshResult{
			Inconclusive: true,
			Message:      "non-terminal upstream auth failure after oauth access refresh",
		}
	}
	if p == nil || p.oauthClient == nil || claim == nil || claim.Token == nil {
		return authFailureRefreshResult{}
	}
	token := claim.Token.Token
	refreshToken := strings.TrimSpace(token.RefreshToken)
	if refreshToken == "" {
		return authFailureRefreshResult{}
	}
	updater, ok := p.store.(tokenSecretUpdater)
	if !ok || updater == nil {
		return authFailureRefreshResult{}
	}

	ctx, cancel := context.WithTimeout(parent, authFailureRefreshTimeout)
	defer cancel()
	result, err := p.refreshOAuthToken(ctx, token.ID, refreshToken)
	if err != nil {
		detail := err.Error()
		status := oauth.RefreshErrorStatus(detail)
		if oauth.IsPermanentlyInvalidRefreshTokenError(status, detail) {
			return authFailureRefreshResult{
				Message: "terminal upstream auth failure: refresh token invalid",
			}
		}
		return authFailureRefreshResult{
			Inconclusive: true,
			Message:      "non-terminal upstream auth failure: oauth refresh inconclusive: " + truncateDetail(detail, 240),
		}
	}

	expiresAt := (*time.Time)(nil)
	if result.ExpiresIn > 0 {
		expires := time.Now().UTC().Add(time.Duration(result.ExpiresIn) * time.Second)
		expiresAt = &expires
	}
	if err := updater.UpdateTokenSecret(ctx, store.TokenSecretUpdate{
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
		return authFailureRefreshResult{
			Inconclusive: true,
			Message:      "non-terminal upstream auth failure: oauth refresh persist failed: " + truncateDetail(err.Error(), 240),
		}
	}
	p.refreshTokenSnapshotsAfterSecretUpdate(ctx, token.OwnerUserID)
	if p.logger != nil {
		p.logger.Info("oauth access token refreshed after upstream auth failure", "token_id", token.ID, "owner_user_id", token.OwnerUserID)
	}
	return authFailureRefreshResult{Refreshed: true}
}

func (p *Pipeline) refreshOAuthToken(ctx context.Context, tokenID int64, refreshToken string) (oauth.RefreshResult, error) {
	clientID := ""
	if source, ok := p.store.(tokenOAuthClientIDStore); ok && source != nil {
		if value, err := source.TokenOAuthClientID(ctx, tokenID); err == nil {
			clientID = strings.TrimSpace(value)
		} else if p.logger != nil {
			p.logger.Warn("token oauth client id lookup failed; using default client id", "token_id", tokenID, "error", err)
		}
	}
	if clientID != "" {
		if refresher, ok := p.oauthClient.(oauth.ClientIDRefresher); ok {
			return refresher.RefreshWithClientID(ctx, refreshToken, clientID)
		}
	}
	return p.oauthClient.Refresh(ctx, refreshToken)
}

func (p *Pipeline) refreshTokenSnapshotsAfterSecretUpdate(ctx context.Context, ownerUserID int64) {
	if p == nil || p.tokens == nil {
		return
	}
	if err := p.tokens.Refresh(ctx); err != nil && p.logger != nil {
		p.logger.Warn("token snapshot refresh after oauth access refresh failed", "owner_user_id", ownerUserID, "error", err)
	}
	if ownerUserID > 0 {
		if err := p.tokens.RefreshOwner(ctx, ownerUserID); err != nil && p.logger != nil {
			p.logger.Warn("owner token snapshot refresh after oauth access refresh failed", "owner_user_id", ownerUserID, "error", err)
		}
	}
}

func authFailureCooldownUntil() *time.Time {
	until := time.Now().UTC().Add(authFailureCooldown)
	return &until
}

func truncateDetail(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

func (p *Pipeline) recordClaimTiming(timing map[string]any, claim *tokens.Claim, err error) {
	if timing == nil {
		return
	}
	if err != nil {
		timing["token_claim_error"] = err.Error()
	}
	if claim == nil {
		return
	}
	telemetry := claim.Telemetry
	timing["token_claim_id"] = telemetry.ClaimID
	timing["token_claim_reason"] = telemetry.Reason
	timing["token_claim_snapshot_scope"] = telemetry.SnapshotScope
	timing["token_claim_snapshot_version"] = telemetry.SnapshotVersion
	timing["token_claim_snapshot_age_ms"] = telemetry.SnapshotAgeMs
	timing["token_claim_ready_tokens"] = telemetry.SnapshotReadyTokens
	timing["token_claim_active_cap"] = telemetry.ActiveCap
	timing["token_claim_active_before"] = telemetry.ActiveBefore
	timing["token_claim_active_after"] = telemetry.ActiveAfter
	timing["token_claim_candidate_count"] = telemetry.CandidateCount
	timing["token_claim_selected_at"] = telemetry.SelectedAt
}

func (p *Pipeline) recordClaimReleaseTiming(timing map[string]any, claim *tokens.Claim) {
	if timing == nil || claim == nil {
		return
	}
	telemetry := claim.Telemetry
	if telemetry.ReleasedAt != nil {
		timing["token_claim_released_at"] = telemetry.ReleasedAt
	}
	if telemetry.HeldMs != nil {
		timing["token_claim_held_ms"] = *telemetry.HeldMs
	}
	if telemetry.ActiveAfterRelease != nil {
		timing["token_claim_active_after_release"] = *telemetry.ActiveAfterRelease
	}
}

func (p *Pipeline) doAttempt(w http.ResponseWriter, r *http.Request, attempt Attempt) (AttemptResult, error) {
	upstreamURL, err := p.upstreamURL(attempt.Intent)
	if err != nil {
		return AttemptResult{Status: http.StatusBadGateway}, err
	}
	attempt.UpstreamURL = upstreamURL
	method := attempt.Intent.Method
	if method == "" {
		method = http.MethodPost
	}
	attempt.Method = method
	req, err := http.NewRequestWithContext(r.Context(), method, upstreamURL, bytes.NewReader(attempt.Body))
	if err != nil {
		return AttemptResult{Status: http.StatusBadGateway}, err
	}
	copyProxyHeaders(req.Header, r.Header)
	req.Header.Set("X-Request-ID", attempt.RequestID)
	req.Header.Set("Authorization", "Bearer "+attempt.Claim.AccessToken())
	req.Header.Set("Content-Type", contentType(firstNonEmpty(attempt.Intent.UpstreamContentType, r.Header.Get("Content-Type"))))
	if attempt.Intent.UpstreamAccept != "" {
		req.Header.Set("Accept", attempt.Intent.UpstreamAccept)
	}
	if attempt.PromptCache != nil && attempt.PromptCache.SessionID != "" {
		req.Header.Set("Session_id", attempt.PromptCache.SessionID)
	}
	resp, err := p.transport.Do(r.Context(), req)
	if err != nil {
		return AttemptResult{Status: http.StatusBadGateway, Retry: true}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, raw := readUpstreamError(resp.Body)
		retry := resp.StatusCode == http.StatusUnauthorized ||
			resp.StatusCode == http.StatusForbidden ||
			resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode >= 500
		return AttemptResult{Status: resp.StatusCode, Retry: retry, ErrorBody: raw}, fmt.Errorf("%s", detail)
	}
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Set("X-OAIX-Request-ID", attempt.RequestID)
	w.Header().Set("X-OAIX-Token-ID", fmt.Sprint(attempt.Claim.TokenID()))
	if attempt.Claim != nil && attempt.Claim.Token != nil {
		w.Header().Set("X-OAIX-Token-Owner-User-ID", fmt.Sprint(attempt.Claim.Token.Token.OwnerUserID))
		if isMarketplaceIntent(attempt.Intent) {
			w.Header().Set("X-OAIX-Selection-Mode", "marketplace")
			w.Header().Set("X-OAIX-Marketplace-Price-BPS", fmt.Sprint(claimMarketplacePriceBPS(attempt.Claim)))
			w.Header().Set("X-OAIX-Marketplace-Price-Source", claimMarketplacePriceSource(attempt.Claim))
			w.Header().Set("X-OAIX-Marketplace-Price-Locked", strconv.FormatBool(attempt.Claim.MarketplacePriceLocked))
			if attempt.Claim.MarketplacePriceLockedAt != nil {
				w.Header().Set("X-OAIX-Marketplace-Price-Locked-At", attempt.Claim.MarketplacePriceLockedAt.UTC().Format(time.RFC3339Nano))
			}
			if attempt.Claim.MarketplacePriceContractKey != "" {
				w.Header().Set("X-OAIX-Marketplace-Contract-Key", attempt.Claim.MarketplacePriceContractKey)
			}
		}
	}
	if attempt.Intent.ImageResponseFormat != "" {
		if attempt.Intent.Stream {
			return p.streamImageResponse(w, resp, attempt)
		}
		return p.writeImageJSONResponse(w, resp, attempt)
	}
	if (isSSE(resp.Header.Get("Content-Type")) || attempt.Intent.UpstreamAccept == "text/event-stream") && !attempt.Intent.Stream {
		return p.writeResponsesJSONFromSSE(w, resp, attempt)
	}
	if isSSE(resp.Header.Get("Content-Type")) || attempt.Intent.Stream {
		return p.streamResponsesWithPreflight(w, resp, attempt)
	}
	w.WriteHeader(resp.StatusCode)
	if attempt.PromptCache != nil {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, p.cfg.Upstream.NonStreamMaxResponseBytes))
		if readErr != nil {
			return AttemptResult{Status: resp.StatusCode, Committed: true}, readErr
		}
		usage, responseID := extractResponseMetrics(body, attempt.Intent.Model)
		_, copyErr := w.Write(body)
		result := AttemptResult{Status: resp.StatusCode, Committed: true, Usage: usage, ResponseID: responseID}
		if copyErr != nil {
			return result, copyErr
		}
		return result, nil
	}
	_, copyErr := io.Copy(w, resp.Body)
	result := AttemptResult{Status: resp.StatusCode, Committed: true}
	if copyErr != nil {
		return result, copyErr
	}
	return result, nil
}

func isMarketplaceIntent(intent RequestIntent) bool {
	switch strings.ToLower(strings.TrimSpace(intent.SelectionMode)) {
	case "marketplace", "marketplace-priced":
		return true
	default:
		return false
	}
}

func claimMarketplacePriceBPS(claim *tokens.Claim) int {
	if claim != nil && claim.MarketplacePriceLocked {
		value := claim.MarketplacePriceBPS
		if value < 0 {
			return 0
		}
		if value > store.MaxMarketplacePriceBPS {
			return store.MaxMarketplacePriceBPS
		}
		return value
	}
	if claim == nil || claim.Token == nil || claim.Token.Token.MarketplacePriceBPS == nil {
		return store.DefaultMarketplacePriceBPS
	}
	value := *claim.Token.Token.MarketplacePriceBPS
	if value < 0 {
		return 0
	}
	if value > store.MaxMarketplacePriceBPS {
		return store.MaxMarketplacePriceBPS
	}
	return value
}

func claimMarketplacePriceSource(claim *tokens.Claim) string {
	if claim != nil && claim.MarketplacePriceLocked && strings.TrimSpace(claim.MarketplacePriceSource) != "" {
		return strings.TrimSpace(claim.MarketplacePriceSource)
	}
	if claim == nil || claim.Token == nil {
		return "owner_default"
	}
	source := strings.TrimSpace(claim.Token.Token.MarketplacePriceSource)
	if source == "" {
		return "owner_default"
	}
	return source
}

func (p *Pipeline) upstreamURL(intent RequestIntent) (string, error) {
	endpoint := intent.Endpoint
	if intent.UpstreamEndpoint != "" {
		endpoint = intent.UpstreamEndpoint
	}
	if endpoint == "/v1/chat/completions" && strings.TrimSpace(p.cfg.Upstream.ChatCompletionsURL) != "" {
		return p.cfg.Upstream.ChatCompletionsURL, nil
	}
	base := strings.TrimSpace(p.cfg.Upstream.ResponsesURL)
	if base == "" {
		return "", errors.New("CODEX_BASE_URL is empty")
	}
	if intent.Compact && !strings.HasSuffix(base, "/compact") {
		return strings.TrimRight(base, "/") + "/compact", nil
	}
	if intent.UpstreamSuffix != "" {
		return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(intent.UpstreamSuffix, "/"), nil
	}
	if endpoint == "/v1/chat/completions" {
		parsed, err := url.Parse(base)
		if err != nil {
			return "", err
		}
		parsed.Path = strings.TrimRight(strings.TrimSuffix(parsed.Path, "/responses"), "/") + "/chat/completions"
		return parsed.String(), nil
	}
	return base, nil
}

func classify(status int, err error) Outcome {
	if err != nil && errors.Is(err, context.Canceled) {
		return OutcomeClientCanceled
	}
	switch status {
	case http.StatusTooManyRequests:
		return OutcomeUpstream429Cooldown
	case http.StatusUnauthorized:
		return OutcomeUpstream401Invalid
	case http.StatusForbidden:
		return OutcomeUpstream403Invalid
	}
	if status >= 500 {
		return OutcomeUpstream5xx
	}
	if status >= 400 {
		return OutcomeUpstream4xx
	}
	if err != nil {
		return OutcomeTransportError
	}
	return OutcomeSuccess
}

func isDeactivatedWorkspaceFailure(status int, raw []byte, err error) bool {
	if status != http.StatusPaymentRequired {
		return false
	}
	if bytes.Contains(bytes.ToLower(raw), []byte("deactivated_workspace")) {
		return true
	}
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "deactivated_workspace")
}

func (p *Pipeline) commitSuccess(tokenID int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.withCommitRetry(ctx, func(ctx context.Context) error {
			return p.store.MarkTokenSuccess(ctx, tokenID)
		}); err != nil {
			p.commitFailures.Add(1)
			if p.logger != nil {
				p.logger.Warn("mark token success failed", "token_id", tokenID, "error", err)
			}
		}
	}()
}

func (p *Pipeline) recordNoTokenGatewayAttempt(requestID string, intent RequestIntent, attemptIndex int, started time.Time, err error) *int64 {
	finished := time.Now().UTC()
	duration := int(finished.Sub(started).Milliseconds())
	item := store.GatewayRequestAttempt{
		RequestID:           requestID,
		AttemptIndex:        attemptIndex,
		OwnerUserID:         intent.OwnerUserID,
		SelectionMode:       nullable(strings.TrimSpace(intent.SelectionMode)),
		CallerOwnerUserID:   ptrPositiveInt64(intent.CallerOwnerUserID),
		ExcludeOwnerUserID:  ptrPositiveInt64(intent.ExcludeOwnerUserID),
		Endpoint:            intent.Endpoint,
		Model:               nullable(intent.Model),
		StartedAt:           started,
		FinishedAt:          &finished,
		DurationMs:          &duration,
		StatusCode:          ptrInt(http.StatusServiceUnavailable),
		Success:             boolPtr(false),
		Retry:               boolPtr(false),
		Outcome:             string(OutcomeNoToken),
		ErrorCode:           nullable(string(OutcomeNoToken)),
		ErrorMessageExcerpt: errorExcerpt(err),
	}
	return p.insertGatewayAttempt(item)
}

func (p *Pipeline) recordGatewayAttempt(ctx context.Context, attempt Attempt, result AttemptResult, err error, outcome Outcome, retry bool, deactivated bool, cooldownUntil *time.Time) *int64 {
	finished := time.Now().UTC()
	started := attempt.StartedAt
	if started.IsZero() {
		started = finished
	}
	duration := int(finished.Sub(started).Milliseconds())
	success := err == nil && result.Status >= 200 && result.Status < 400
	item := store.GatewayRequestAttempt{
		RequestID:           attempt.RequestID,
		AttemptIndex:        attempt.Index,
		OwnerUserID:         attempt.Intent.OwnerUserID,
		SelectionMode:       nullable(strings.TrimSpace(attempt.Intent.SelectionMode)),
		CallerOwnerUserID:   ptrPositiveInt64(attempt.Intent.CallerOwnerUserID),
		ExcludeOwnerUserID:  ptrPositiveInt64(attempt.Intent.ExcludeOwnerUserID),
		Endpoint:            attempt.Intent.Endpoint,
		Model:               nullable(attempt.Intent.Model),
		StartedAt:           started,
		FinishedAt:          &finished,
		DurationMs:          &duration,
		StatusCode:          ptrInt(result.Status),
		Success:             &success,
		Retry:               &retry,
		Outcome:             string(outcome),
		Deactivated:         deactivated,
		CooldownUntil:       cooldownUntil,
		ErrorCode:           attemptErrorCode(result.Status, result.ErrorBody, err, outcome),
		ErrorMessageExcerpt: errorExcerpt(err),
		ErrorBodyHash:       errorBodyHash(result.ErrorBody),
	}
	if attempt.Claim != nil {
		tokenID := attempt.Claim.TokenID()
		item.TokenID = &tokenID
		telemetry := attempt.Claim.Telemetry
		if telemetry.OwnerUserID > 0 {
			item.TokenOwnerUserID = &telemetry.OwnerUserID
		}
		item.ClaimID = int64PtrFromUint64(telemetry.ClaimID)
		item.CandidateCount = ptrInt(telemetry.CandidateCount)
		item.ReadyTokens = ptrInt(telemetry.SnapshotReadyTokens)
		if telemetry.SnapshotVersion > 0 {
			item.SnapshotVersion = &telemetry.SnapshotVersion
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return p.insertGatewayAttempt(item)
}

func (p *Pipeline) insertGatewayAttempt(item store.GatewayRequestAttempt) *int64 {
	if p == nil || p.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	id, err := p.store.InsertGatewayRequestAttempt(ctx, item)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("gateway attempt log write failed", "request_id", item.RequestID, "attempt", item.AttemptIndex, "error", err)
		}
		return nil
	}
	return &id
}

func (p *Pipeline) tokenStateEventContext(requestID string, intent RequestIntent, status int, outcome Outcome, attemptID *int64) store.TokenStateEventContext {
	return store.TokenStateEventContext{
		RequestID:               requestID,
		GatewayRequestAttemptID: attemptID,
		Endpoint:                intent.Endpoint,
		Model:                   intent.Model,
		StatusCode:              ptrInt(status),
		SelectionMode:           strings.TrimSpace(intent.SelectionMode),
		CallerOwnerUserID:       ptrPositiveInt64(intent.CallerOwnerUserID),
		Metadata: map[string]any{
			"outcome":               string(outcome),
			"exclude_owner_user_id": intent.ExcludeOwnerUserID,
			"target_token_id":       intent.TargetTokenID,
		},
	}
}

func (p *Pipeline) commitTokenError(tokenID int64, tokenOwnerUserID *int64, message string, deactivate bool, cooldownUntil *time.Time, eventCtx store.TokenStateEventContext) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := p.withCommitRetry(ctx, func(ctx context.Context) error {
			return p.store.MarkTokenErrorWithContext(ctx, tokenID, message, deactivate, cooldownUntil, eventCtx)
		}); err != nil {
			p.commitFailures.Add(1)
			if p.logger != nil {
				p.logger.Error("mark token error failed", "token_id", tokenID, "error", err)
			}
			return
		}
		if deactivate && p.tokens != nil {
			if err := p.tokens.Refresh(ctx); err != nil && p.logger != nil {
				p.logger.Warn("token snapshot refresh after deactivate failed", "token_id", tokenID, "error", err)
			}
			if tokenOwnerUserID != nil {
				if err := p.tokens.RefreshOwner(ctx, *tokenOwnerUserID); err != nil && p.logger != nil {
					p.logger.Warn("owner token snapshot refresh after deactivate failed", "token_id", tokenID, "owner_user_id", *tokenOwnerUserID, "error", err)
				}
			}
		}
	}()
}

func boolPtr(value bool) *bool {
	return &value
}

func int64PtrFromUint64(value uint64) *int64 {
	if value == 0 || value > uint64(^uint64(0)>>1) {
		return nil
	}
	out := int64(value)
	return &out
}

func errorExcerpt(err error) *string {
	if err == nil {
		return nil
	}
	text := strings.TrimSpace(err.Error())
	if text == "" {
		return nil
	}
	if len(text) > 512 {
		text = text[:512]
	}
	return &text
}

func errorBodyHash(raw []byte) *string {
	if len(raw) == 0 {
		return nil
	}
	sum := sha256.Sum256(raw)
	value := hex.EncodeToString(sum[:])
	return &value
}

func attemptErrorCode(status int, raw []byte, err error, outcome Outcome) *string {
	if status >= 200 && status < 400 && err == nil {
		return nil
	}
	if code := errorCodeFromBody(raw); code != "" {
		return &code
	}
	if outcome != "" && outcome != OutcomeSuccess {
		value := string(outcome)
		return &value
	}
	return nil
}

func errorCodeFromBody(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	if code := errorCodeFromMap(payload); code != "" {
		return code
	}
	if nested, ok := payload["error"].(map[string]any); ok {
		return errorCodeFromMap(nested)
	}
	return ""
}

func errorCodeFromMap(payload map[string]any) string {
	for _, key := range []string{"code", "type", "error_code"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (p *Pipeline) withCommitRetry(ctx context.Context, fn func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := fn(ctx); err != nil {
			lastErr = err
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		return nil
	}
	return lastErr
}

func (p *Pipeline) finalLog(ctx context.Context, requestID string, intent RequestIntent, started time.Time, status int, success bool, attempts int, tokenID *int64, tokenOwnerUserID *int64, accountID *string, errMsg *string, timing map[string]any, promptCacheContext *PromptCacheContext, affinityResult tokens.PromptAffinityResult, usage *UsageMetrics, upstreamResponseID string, firstTokenAt *time.Time) {
	finished := time.Now().UTC()
	duration := int(finished.Sub(started).Milliseconds())
	if timing == nil {
		timing = map[string]any{}
	}
	timing["total_ms"] = duration
	trace := updatePromptTrace(promptCacheContext, affinityResult, usage, upstreamResponseID, status)
	p.logs.Submit(ctx, store.RequestLog{
		RequestID:                     requestID,
		OwnerUserID:                   ptrInt64(intent.OwnerUserID),
		APIKeyID:                      intent.APIKeyID,
		TokenOwnerUserID:              tokenOwnerUserID,
		SelectionMode:                 nullable(strings.TrimSpace(intent.SelectionMode)),
		CallerOwnerUserID:             ptrPositiveInt64(intent.CallerOwnerUserID),
		Endpoint:                      intent.Endpoint,
		Model:                         nullable(intent.Model),
		ModelName:                     nullable(intent.Model),
		IsStream:                      intent.Stream,
		StatusCode:                    ptrInt(status),
		Success:                       &success,
		AttemptCount:                  attempts,
		TokenID:                       tokenID,
		AccountID:                     accountID,
		StartedAt:                     started,
		FinishedAt:                    &finished,
		FirstTokenAt:                  firstTokenAt,
		TTFTMs:                        ttftMillis(started, firstTokenAt),
		DurationMs:                    &duration,
		TimingSpans:                   timing,
		InputTokens:                   usageInt(usage, func(u *UsageMetrics) int { return u.InputTokens }),
		CacheWriteInputTokens:         usageInt(usage, func(u *UsageMetrics) int { return u.CacheWriteInputTokens }),
		CacheWriteTokensSource:        usageString(usage, func(u *UsageMetrics) string { return u.CacheWriteTokensSource }),
		CachedInputTokens:             usageInt(usage, func(u *UsageMetrics) int { return u.CachedInputTokens }),
		OutputTokens:                  usageInt(usage, func(u *UsageMetrics) int { return u.OutputTokens }),
		TotalTokens:                   usageInt(usage, func(u *UsageMetrics) int { return u.TotalTokens }),
		EstimatedCostUSD:              usageFloat(usage, func(u *UsageMetrics) *float64 { return u.EstimatedCostUSD }),
		RequestPayloadHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.RequestPayloadHash }),
		UpstreamPayloadHash:           promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.UpstreamPayloadHash }),
		PromptTemplateHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptTemplateHash }),
		PromptDynamicHash:             promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptDynamicHash }),
		PromptCacheSource:             promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.Source }),
		PromptCacheKeyHash:            promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheKeyHash }),
		PromptCacheRetentionRequested: promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheRetentionRequested }),
		PromptCacheRetentionSent:      promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PromptCacheRetentionSent }),
		SessionIDHash:                 promptString(promptCacheContext, func(c *PromptCacheContext) string { return shortHash(c.SessionID, 64) }),
		SessionIDSource:               promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.SessionIDSource }),
		PreviousResponseIDHash:        promptString(promptCacheContext, func(c *PromptCacheContext) string { return c.PreviousResponseIDHash }),
		UpstreamResponseID:            nullable(upstreamResponseID),
		CacheHitRatio:                 usageFloatValue(usage, func(u *UsageMetrics) *float64 { return u.CacheHitRatio }),
		CacheAffinityResult:           nullable(affinityResult.Result),
		CacheAffinityLaneIndex:        affinityResult.LaneIndex,
		PromptCacheTrace:              trace,
		ErrorMessage:                  errMsg,
	}, true)
}

func (p *Pipeline) recordPromptCacheSuccess(promptCacheContext *PromptCacheContext, claim *tokens.Claim, responseID string) {
	if promptCacheContext == nil {
		return
	}
	if responseID != "" {
		p.tokens.BindPromptResponseOwner(p.affinity, responseID, claim, p.cfg.PromptCache.ResponseTTL)
	}
}

func copyProxyHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		if lower == "host" || lower == "authorization" || lower == "content-length" || lower == "content-encoding" || lower == "accept-encoding" || strings.HasPrefix(lower, "x-oaix-") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		if lower == "content-length" || lower == "connection" {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyAndFlush(w http.ResponseWriter, reader io.Reader, observer *usageObserver) (int64, error) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	var written int64
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if observer != nil {
				observer.Observe(buf[:n])
			}
			m, writeErr := w.Write(buf[:n])
			written += int64(m)
			if flusher != nil {
				flusher.Flush()
			}
			if writeErr != nil {
				return written, writeErr
			}
			if m != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	if status <= 0 {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(openai.NewError(message, "gateway_error", "oaix_gateway_error"))
}

func writeErrorResponse(w http.ResponseWriter, stream bool, status int, message string) {
	if stream {
		writeSSEError(w, status, message)
		return
	}
	writeJSONError(w, status, message)
}

func writeFinalErrorResponse(w http.ResponseWriter, stream bool, downstreamStarted bool, status int, message string) {
	if stream && downstreamStarted {
		writeSSEErrorEvent(w, status, message)
		return
	}
	writeErrorResponse(w, stream, status, message)
}

func writeSSEError(w http.ResponseWriter, status int, message string) {
	if status <= 0 {
		status = http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(status)
	writeSSEErrorEvent(w, status, message)
}

func writeSSEErrorEvent(w http.ResponseWriter, status int, message string) {
	if status <= 0 {
		status = http.StatusBadGateway
	}
	payload, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    "gateway_error",
			"code":    "oaix_gateway_error",
			"status":  status,
		},
	})
	_, _ = w.Write(sse.Encode("error", payload))
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func requestID(r *http.Request) string {
	for _, key := range []string{"X-Request-ID", "X-OAIX-Request-ID", "OpenAI-Request-ID"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			return value
		}
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s:%s", time.Now().UnixNano(), r.RemoteAddr, r.URL.Path)))
	return "oaix-" + hex.EncodeToString(sum[:8])
}

func normalizeIntent(intent RequestIntent, body []byte) RequestIntent {
	switch intent.Endpoint {
	case "/v1/responses":
		if req, err := openai.DecodeResponsesRequest(body); err == nil {
			if intent.Model == "" {
				intent.Model = req.Model
			}
			if !intent.Stream {
				intent.Stream = req.Stream
			}
		}
	case "/v1/chat/completions":
		if req, err := openai.DecodeChatCompletionRequest(body); err == nil {
			if intent.Model == "" {
				intent.Model = req.Model
			}
			if !intent.Stream {
				intent.Stream = req.Stream
			}
		}
	case "/v1/images/generations":
		if req, err := openai.DecodeImageGenerationRequest(body); err == nil && intent.Model == "" {
			intent.Model = req.Model
		}
	default:
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err == nil {
			if intent.Model == "" {
				if model, ok := payload["model"].(string); ok {
					intent.Model = model
				}
			}
			if !intent.Stream {
				stream, _ := payload["stream"].(bool)
				intent.Stream = stream
			}
		}
	}
	return intent
}

func clientIP(r *http.Request) string {
	for _, key := range []string{"X-Forwarded-For", "X-Real-IP"} {
		if value := strings.TrimSpace(r.Header.Get(key)); value != "" {
			if first, _, ok := strings.Cut(value, ","); ok {
				return strings.TrimSpace(first)
			}
			return value
		}
	}
	return r.RemoteAddr
}

func contentType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/json"
	}
	return value
}

func isSSE(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/event-stream")
}

func nullable(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func ptrInt(value int) *int {
	return &value
}

func ptrInt64(value int64) *int64 {
	return &value
}

func ptrPositiveInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
}

func stringPtr(value string) *string {
	return &value
}
