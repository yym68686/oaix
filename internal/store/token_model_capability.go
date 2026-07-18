package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const tokenModelCapabilityScopeSuffix = ":model-compatibility"

type TokenModelCapabilityLoss struct {
	TokenID     int64
	OwnerUserID int64
	Model       string
	ValidUntil  time.Time
	UpdatedAt   time.Time
}

func TokenModelCapabilityScope(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return ""
	}
	scope := model + tokenModelCapabilityScopeSuffix
	if len(scope) > 128 {
		return ""
	}
	return scope
}

func tokenModelFromCapabilityScope(scope string) (string, bool) {
	scope = strings.ToLower(strings.TrimSpace(scope))
	if !strings.HasSuffix(scope, tokenModelCapabilityScopeSuffix) {
		return "", false
	}
	model := strings.TrimSpace(strings.TrimSuffix(scope, tokenModelCapabilityScopeSuffix))
	return model, model != ""
}

func (s *Store) MarkTokenModelCapabilityLoss(ctx context.Context, tokenID, ownerUserID int64, model, message string, validUntil time.Time) error {
	if tokenID <= 0 {
		return fmt.Errorf("token id is required")
	}
	scope := TokenModelCapabilityScope(model)
	if scope == "" {
		return fmt.Errorf("model capability scope is required")
	}
	if validUntil.IsZero() {
		return fmt.Errorf("model capability loss expiry is required")
	}
	args := []any{tokenID, scope, validUntil.UTC(), truncate(message, 4000)}
	ownerWhere := "true"
	if ownerUserID > 0 {
		args = append(args, ownerUserID)
		ownerWhere = fmt.Sprintf("owner_user_id = $%d", len(args))
	}
	tag, err := s.pool.Exec(ctx, `
		insert into codex_token_scoped_cooldowns(token_id, owner_user_id, scope, cooldown_until, last_error, created_at, updated_at)
		select id, owner_user_id, $2, $3, nullif($4, ''), now(), now()
		from codex_tokens
		where id = $1
		  and merged_into_token_id is null
		  and `+ownerWhere+`
		on conflict (token_id, scope) do update
		set owner_user_id = excluded.owner_user_id,
		    cooldown_until = greatest(codex_token_scoped_cooldowns.cooldown_until, excluded.cooldown_until),
		    last_error = excluded.last_error,
		    updated_at = now()
	`, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ListTokenModelCapabilityLosses(ctx context.Context, scope ResourceScope) ([]TokenModelCapabilityLoss, error) {
	args := []any{time.Now().UTC(), tokenModelCapabilityScopeSuffix}
	ownerWhere := scope.ownerFilter("c.owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select c.token_id, coalesce(c.owner_user_id, 0), c.scope, c.cooldown_until, c.updated_at
		from codex_token_scoped_cooldowns c
		join codex_tokens t on t.id = c.token_id and t.merged_into_token_id is null
		where c.cooldown_until > $1
		  and right(c.scope, length($2)) = $2
		  and `+ownerWhere+`
		order by c.token_id, c.scope
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	losses := make([]TokenModelCapabilityLoss, 0)
	for rows.Next() {
		var item TokenModelCapabilityLoss
		var storedScope string
		if err := rows.Scan(&item.TokenID, &item.OwnerUserID, &storedScope, &item.ValidUntil, &item.UpdatedAt); err != nil {
			return nil, err
		}
		model, ok := tokenModelFromCapabilityScope(storedScope)
		if !ok {
			continue
		}
		item.Model = model
		losses = append(losses, item)
	}
	return losses, rows.Err()
}

func deleteTokenModelCapabilityLossesTx(ctx context.Context, tx pgx.Tx, tokenID int64) error {
	_, err := tx.Exec(ctx, `
		delete from codex_token_scoped_cooldowns
		where token_id = $1
		  and right(scope, length($2)) = $2
	`, tokenID, tokenModelCapabilityScopeSuffix)
	return err
}
