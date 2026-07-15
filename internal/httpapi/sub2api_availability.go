package httpapi

import (
	"context"
	"fmt"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) syncSub2APIAvailabilityAsync(token *store.Token, reason string) {
	if a == nil || a.store == nil || token == nil || token.ID <= 0 {
		return
	}
	tokenID := token.ID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		current, err := a.store.GetToken(ctx, tokenID)
		if err != nil || current == nil || !current.IsActive || current.DisabledAt != nil || (current.CooldownUntil != nil && current.CooldownUntil.After(time.Now().UTC())) {
			return
		}
		restored, err := a.sub2APISyncer().RestoreTokenAvailability(ctx, *current)
		if err != nil {
			if a.logger != nil {
				a.logger.Warn("sub2api availability restore failed", "token_id", current.ID, "owner_user_id", current.OwnerUserID, "reason", reason, "restored", restored, "error_type", fmt.Sprintf("%T", err))
			}
			return
		}
		if restored > 0 && a.logger != nil {
			a.logger.Info("sub2api availability restored", "token_id", current.ID, "owner_user_id", current.OwnerUserID, "reason", reason, "restored", restored)
		}
	}()
}

func (a *App) syncSub2APIReadyTransitions(ctx context.Context, ready []store.Token) {
	if a == nil || len(ready) == 0 {
		return
	}
	for i := range ready {
		a.syncSub2APIAvailabilityAsync(&ready[i], "token_ready_snapshot")
	}
}
