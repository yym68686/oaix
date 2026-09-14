package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/recovery"
	"github.com/yym68686/oaix/internal/store"
)

var recoveryWorkspaceUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Start401Recovery keeps long OAuth operations off the proxy path. Candidates
// come from durable 401 state events, so restart also recovers existing failures.
func (a *App) Start401Recovery(ctx context.Context) {
	if !a.cfg.Recovery401.Enabled || a.store == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				scanCtx, scanCancel := context.WithTimeout(ctx, 5*time.Second)
				ids, err := a.store.ListRecovery401Candidates(scanCtx)
				scanCancel()
				if err != nil {
					if a.logger != nil {
						a.logger.Warn("401 recovery scan failed", "error", err)
					}
					continue
				}
				for _, id := range ids {
					if ctx.Err() != nil {
						return
					}
					a.run401Recovery(ctx, id)
				}
			}
		}
	}()
}

func recoveryAccountName(candidate store.Recovery401Candidate) (string, error) {
	token := candidate.Token
	if token.IsAgentIdentity() || token.IsCodexPersonalAccessToken() || stringPtr(token.PlanType) != recovery.EligiblePlan {
		return "", errors.New("subscription_ineligible")
	}
	email := strings.TrimSpace(stringPtr(token.Email))
	accountID := strings.TrimSpace(stringPtr(token.AccountID))
	if email == "" || !strings.Contains(email, "@") || !recoveryWorkspaceUUID.MatchString(accountID) {
		return "", errors.New("missing_identity")
	}
	if name := strings.TrimSpace(candidate.Name); name != "" {
		parts := strings.SplitN(name, "----", 2)
		if len(parts) == 2 && strings.EqualFold(strings.TrimSpace(parts[0]), email) && strings.TrimSpace(parts[1]) != "" {
			return email + "----" + strings.TrimSpace(parts[1]), nil
		}
	}
	// 5xteam's exported names use the final six workspace UUID characters.
	return email + "----myWorkspace-" + strings.ToLower(accountID[len(accountID)-6:]), nil
}

func (a *App) run401Recovery(parent context.Context, id int64) {
	ctx, cancel := context.WithTimeout(parent, 6*time.Minute)
	defer cancel()
	lease, acquired, err := a.store.TryQuotaRecoveryCheckLease(ctx, id)
	if err != nil || !acquired || lease == nil {
		if err != nil && a.logger != nil {
			a.logger.Warn("401 recovery lease failed", "token_id", id, "error", err)
		}
		return
	}
	defer lease.Release()
	candidate, err := a.store.BeginRecovery401(ctx, id)
	if err != nil || candidate == nil {
		if err != nil && a.logger != nil {
			a.logger.Warn("401 recovery claim failed", "token_id", id, "error", err)
		}
		return
	}
	outcome := "failed"
	reason := "recovery_failed"
	defer func() {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.store.RecordRecovery401Result(saveCtx, *candidate, outcome, reason); err != nil && a.logger != nil {
			a.logger.Warn("401 recovery result persistence failed", "token_id", id, "error", err)
		}
		if a.logger != nil {
			a.logger.Info("401 recovery completed", "token_id", id, "owner_user_id", candidate.Token.OwnerUserID, "outcome", outcome, "reason", reason)
		}
	}()
	name, err := recoveryAccountName(*candidate)
	if err != nil {
		reason = err.Error()
		return
	}
	client := recovery.New(a.cfg.Recovery401.BaseURL)
	result, err := client.Recover(ctx, name)
	if err != nil {
		reason = recoveryErrorCode(err)
		return
	}
	token := candidate.Token
	if err := recovery.Validate(result, stringPtr(token.Email), stringPtr(token.AccountID)); err != nil {
		reason = recoveryErrorCode(err)
		return
	}
	// Do not publish the recovered secret before the upstream completes a model
	// response. The existing probe applies the token's configured egress route.
	probeToken := token
	probeToken.AccessToken = result.AccessToken
	probeToken.RefreshToken = result.RefreshToken
	attempt := a.executeTokenProbe(ctx, probeToken, store.QuotaRecoveryModel)
	if attempt.Outcome != tokenProbeCompleted {
		reason = fmt.Sprintf("probe_%s_http_%d", attempt.Outcome, attempt.StatusCode)
		return
	}
	err = a.store.CommitRecovery401(ctx, *candidate, store.TokenSecretUpdate{
		TokenID: id, AccessToken: result.AccessToken, RefreshToken: result.RefreshToken, IDToken: result.IDToken, ExpiresAt: &result.ExpiresAt,
	})
	if err != nil {
		reason = "state_conflict_or_persistence_failed"
		return
	}
	outcome = "recovered"
	reason = "response.completed"
	if a.quota != nil {
		a.quota.clear([]int64{id})
	}
	if a.tokens != nil {
		if err := a.tokens.Refresh(ctx); err != nil && a.logger != nil {
			a.logger.Warn("401 recovery snapshot refresh failed", "token_id", id, "error", err)
		}
		if err := a.tokens.RefreshOwner(ctx, token.OwnerUserID); err != nil && a.logger != nil {
			a.logger.Warn("401 recovery owner snapshot refresh failed", "token_id", id, "error", err)
		}
	}
}

func recoveryErrorCode(err error) string {
	var apiErr *recovery.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "transport_error"
}

// recordQuota401 persists eligible quota endpoint failures with credential and
// state fences; generic quota polling must not revive or disable stale secrets.
func (s *adminQuotaService) recordQuota401(token store.Token, status int, body []byte) bool {
	if status != http.StatusUnauthorized || s.app == nil || !s.app.cfg.Recovery401.Enabled || stringPtr(token.PlanType) != recovery.EligiblePlan || token.IsAgentIdentity() {
		return false
	}
	if !token.IsActive {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.store != nil {
		err := s.store.MarkManualProbeDisabled(ctx, token.ID, manualProbeStateFence(token, token), responseErrorDetail(status, body), false, status, "")
		if err != nil && !errors.Is(err, store.ErrTokenStateChanged) && s.logger != nil {
			s.logger.Warn("quota 401 state persistence failed", "token_id", token.ID, "error", err)
		}
	}
	return true
}
