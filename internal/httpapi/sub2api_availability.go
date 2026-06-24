package httpapi

import (
	"context"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

func (a *App) syncSub2APIAvailabilityAsync(token *store.Token, reason string) {
	if a == nil || a.store == nil || token == nil || token.ID <= 0 {
		return
	}
	tokenCopy := *token
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		restored, err := a.sub2APISyncer().RestoreTokenAvailability(ctx, tokenCopy)
		if err != nil {
			if a.logger != nil {
				a.logger.Warn("sub2api availability restore failed", "token_id", tokenCopy.ID, "owner_user_id", tokenCopy.OwnerUserID, "reason", reason, "restored", restored, "error", err)
			}
			return
		}
		if restored > 0 && a.logger != nil {
			a.logger.Info("sub2api availability restored", "token_id", tokenCopy.ID, "owner_user_id", tokenCopy.OwnerUserID, "reason", reason, "restored", restored)
		}
	}()
}
