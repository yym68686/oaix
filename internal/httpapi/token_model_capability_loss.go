package httpapi

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/yym68686/oaix/internal/proxy"
	"github.com/yym68686/oaix/internal/store"
)

func (a *App) HandleTokenModelCapabilityLoss(ctx context.Context, loss proxy.TokenModelCapabilityLoss) error {
	if a == nil || a.store == nil {
		return errors.New("token model capability loss store is not configured")
	}
	if loss.TokenID <= 0 || strings.TrimSpace(loss.Model) == "" || loss.ValidUntil.IsZero() {
		return errors.New("invalid token model capability loss")
	}
	key := strconv.FormatInt(loss.TokenID, 10) + ":" + strings.ToLower(strings.TrimSpace(loss.Model))
	_, err, _ := a.capabilityQuotaRefresh.Do(key, func() (any, error) {
		var handlingErrors []error
		if err := a.store.MarkTokenModelCapabilityLoss(
			ctx,
			loss.TokenID,
			loss.OwnerUserID,
			loss.Model,
			loss.Detail,
			loss.ValidUntil,
		); err != nil {
			handlingErrors = append(handlingErrors, fmt.Errorf("persist token model capability loss: %w", err))
		} else if a.tokens != nil {
			a.tokens.ConfirmTokenModelCapabilityLoss(loss.TokenID, loss.Model)
		}

		scope := store.AllResources()
		if loss.OwnerUserID > 0 {
			scope = store.OwnerResources(loss.OwnerUserID)
		}
		token, err := a.store.GetTokenScoped(ctx, scope, loss.TokenID)
		if err != nil {
			handlingErrors = append(handlingErrors, fmt.Errorf("load token for authoritative quota refresh: %w", err))
			return nil, errors.Join(handlingErrors...)
		}
		if a.quota == nil {
			handlingErrors = append(handlingErrors, errors.New("quota service is not configured"))
			return nil, errors.Join(handlingErrors...)
		}
		snapshot, saveResult, err := a.quota.fetchAuthoritativeSnapshot(ctx, *token)
		if saveResult.PlanChanged && a.tokens != nil {
			a.tokens.ClearTokenModelCapabilityLoss(loss.TokenID, loss.Model)
		}
		if err != nil {
			handlingErrors = append(handlingErrors, err)
			return nil, errors.Join(handlingErrors...)
		}

		if a.tokens != nil {
			if err := a.tokens.Refresh(ctx); err != nil {
				handlingErrors = append(handlingErrors, fmt.Errorf("refresh global token manager: %w", err))
			}
			ownerUserID := token.OwnerUserID
			if saveResult.OwnerUserID > 0 {
				ownerUserID = saveResult.OwnerUserID
			}
			if ownerUserID > 0 {
				if err := a.tokens.RefreshOwner(ctx, ownerUserID); err != nil {
					handlingErrors = append(handlingErrors, fmt.Errorf("refresh owner token manager: %w", err))
				}
			}
		}
		if a.logger != nil {
			plan := ""
			if snapshot != nil && snapshot.PlanType != nil {
				plan = strings.TrimSpace(*snapshot.PlanType)
			}
			a.logger.Info(
				"authoritative quota refreshed after token model capability loss",
				"token_id", loss.TokenID,
				"owner_user_id", token.OwnerUserID,
				"model", loss.Model,
				"plan_type", plan,
				"plan_changed", saveResult.PlanChanged,
				"request_id", loss.RequestID,
			)
		}
		return nil, errors.Join(handlingErrors...)
	})
	return err
}
