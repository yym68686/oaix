package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Token struct {
	ID            int64      `json:"id"`
	Email         *string    `json:"email,omitempty"`
	AccountID     *string    `json:"account_id,omitempty"`
	AccessToken   string     `json:"-"`
	RefreshToken  string     `json:"-"`
	PlanType      *string    `json:"plan_type,omitempty"`
	Remark        *string    `json:"remark,omitempty"`
	SourceFile    *string    `json:"source_file,omitempty"`
	IsActive      bool       `json:"is_active"`
	CooldownUntil *time.Time `json:"cooldown_until,omitempty"`
	DisabledAt    *time.Time `json:"disabled_at,omitempty"`
	LastUsedAt    *time.Time `json:"last_used_at,omitempty"`
	LastError     *string    `json:"last_error,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type TokenCounts struct {
	Total     int `json:"total"`
	Available int `json:"available"`
	Cooling   int `json:"cooling"`
	Disabled  int `json:"disabled"`
}

type ImportResult struct {
	Created int     `json:"created"`
	Updated int     `json:"updated"`
	Skipped int     `json:"skipped"`
	Failed  int     `json:"failed"`
	Tokens  []Token `json:"tokens"`
}

type TokenListOptions struct {
	Limit  int
	Offset int
	Query  string
	Status string
	Sort   string
}

type RefreshTokenIdentity struct {
	TokenID int64  `json:"token_id"`
	Hash    string `json:"hash"`
	Created bool   `json:"created"`
}

type TokenSecretUpdate struct {
	TokenID      int64
	AccessToken  string
	RefreshToken string
	IDToken      string
	ExpiresAt    *time.Time
	AccountID    string
	Email        string
	PlanType     string
}

func (s *Store) ListAvailableTokens(ctx context.Context) ([]Token, error) {
	rows, err := s.pool.Query(ctx, `
		select id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where is_active = true
		  and merged_into_token_id is null
		  and access_token is not null
		  and access_token <> ''
		  and (cooldown_until is null or cooldown_until <= now())
		  and disabled_at is null
		order by coalesce(last_used_at, 'epoch'::timestamptz) asc, id asc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokens(rows)
}

func (s *Store) ListTokens(ctx context.Context, opts TokenListOptions) ([]Token, int, error) {
	limit := opts.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}
	var filters []string
	var args []any
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	filters = append(filters, "merged_into_token_id is null")
	if q := strings.TrimSpace(opts.Query); q != "" {
		placeholder := arg("%" + q + "%")
		filters = append(filters, fmt.Sprintf("(email ilike %s or account_id ilike %s or remark ilike %s)", placeholder, placeholder, placeholder))
	}
	switch strings.ToLower(strings.TrimSpace(opts.Status)) {
	case "available", "active":
		filters = append(filters, "is_active = true and disabled_at is null and (cooldown_until is null or cooldown_until <= now())")
	case "cooling", "cooldown":
		filters = append(filters, "cooldown_until > now()")
	case "disabled", "inactive":
		filters = append(filters, "(is_active = false or disabled_at is not null)")
	}
	where := strings.Join(filters, " and ")
	if where == "" {
		where = "true"
	}
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from codex_tokens where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	orderBy := "id desc"
	switch strings.ToLower(strings.TrimSpace(opts.Sort)) {
	case "oldest":
		orderBy = "id asc"
	case "last_used":
		orderBy = "last_used_at desc nulls last, id desc"
	case "available":
		orderBy = "is_active desc, cooldown_until asc nulls first, last_used_at asc nulls first"
	}
	args = append(args, limit, offset)
	query := fmt.Sprintf(`
		select id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where %s
		order by %s
		limit $%d offset $%d
	`, where, orderBy, len(args)-1, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items, err := scanTokens(rows)
	if err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (s *Store) TokenCounts(ctx context.Context) (TokenCounts, error) {
	var counts TokenCounts
	err := s.pool.QueryRow(ctx, `
		select
			count(*)::int as total,
			count(*) filter (
				where is_active = true
				  and disabled_at is null
				  and access_token is not null
				  and access_token <> ''
				  and (cooldown_until is null or cooldown_until <= now())
			)::int as available,
			count(*) filter (where cooldown_until > now())::int as cooling,
			count(*) filter (where is_active = false or disabled_at is not null)::int as disabled
		from codex_tokens
		where merged_into_token_id is null
	`).Scan(&counts.Total, &counts.Available, &counts.Cooling, &counts.Disabled)
	return counts, err
}

func (s *Store) UpsertAccessTokens(ctx context.Context, accessTokens []string, source string) (ImportResult, error) {
	result := ImportResult{}
	if len(accessTokens) == 0 {
		return result, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	for _, raw := range accessTokens {
		accessToken := strings.TrimSpace(raw)
		if accessToken == "" {
			result.Skipped++
			continue
		}
		accountID, email, planType := parseAccessTokenClaims(accessToken)
		refreshToken := "access:" + hashString(accessToken)
		var existingID int64
		err := tx.QueryRow(ctx, `select id from codex_tokens where refresh_token = $1 and merged_into_token_id is null limit 1`, refreshToken).Scan(&existingID)
		if err != nil && err != pgx.ErrNoRows {
			return result, err
		}
		if existingID > 0 {
			row := tx.QueryRow(ctx, `
				update codex_tokens
				set access_token = $2,
				    account_id = coalesce($3, account_id),
				    email = coalesce($4, email),
				    plan_type = coalesce($5, plan_type),
				    source_file = coalesce($6, source_file),
				    is_active = true,
				    disabled_at = null,
				    last_error = null,
				    updated_at = now()
				where id = $1
				returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
				          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
			`, existingID, accessToken, accountID, email, planType, nullableString(source))
			token, scanErr := scanToken(row)
			if scanErr != nil {
				return result, scanErr
			}
			result.Updated++
			result.Tokens = append(result.Tokens, token)
			continue
		}
		payload := map[string]any{"source": "access_token_import"}
		row := tx.QueryRow(ctx, `
			insert into codex_tokens (
				email, account_id, access_token, refresh_token, raw_payload, plan_type, source_file,
				is_active, created_at, updated_at
			)
			values ($1, $2, $3, $4, $5, $6, $7, true, now(), now())
			returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
			          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		`, email, accountID, accessToken, refreshToken, jsonBytes(payload), planType, nullableString(source))
		token, scanErr := scanToken(row)
		if scanErr != nil {
			return result, scanErr
		}
		result.Created++
		result.Tokens = append(result.Tokens, token)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) TouchTokens(ctx context.Context, tokenIDs []int64, usedAt time.Time) error {
	if len(tokenIDs) == 0 {
		return nil
	}
	unique := make(map[int64]struct{}, len(tokenIDs))
	values := make([]string, 0, len(tokenIDs))
	args := []any{usedAt}
	for _, id := range tokenIDs {
		if id <= 0 {
			continue
		}
		if _, ok := unique[id]; ok {
			continue
		}
		unique[id] = struct{}{}
		args = append(args, id)
		values = append(values, fmt.Sprintf("($%d::integer, $1::timestamptz)", len(args)))
	}
	if len(values) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		update codex_tokens as t
		set last_used_at = v.used_at, updated_at = now()
		from (values `+strings.Join(values, ",")+`) as v(id, used_at)
		where t.id = v.id
	`, args...)
	return err
}

func (s *Store) MarkTokenSuccess(ctx context.Context, tokenID int64) error {
	_, err := s.pool.Exec(ctx, `
		update codex_tokens
		set last_used_at = now(), last_error = null, updated_at = now()
		where id = $1
	`, tokenID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		insert into token_runtime_state(token_id, active_streams, failure_streak, last_success_at, updated_at)
		values ($1, 0, 0, now(), now())
		on conflict (token_id) do update
		set failure_streak = 0, last_success_at = now(), updated_at = now()
	`, tokenID)
	return err
}

func (s *Store) MarkTokenError(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	disabledAtExpr := "disabled_at"
	isActiveExpr := "is_active"
	if deactivate {
		disabledAtExpr = "now()"
		isActiveExpr = "false"
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`
		update codex_tokens
		set last_error = $2,
		    is_active = %s,
		    disabled_at = %s,
		    cooldown_until = coalesce($3, cooldown_until),
		    updated_at = now()
		where id = $1
	`, isActiveExpr, disabledAtExpr), tokenID, truncate(message, 4000), cooldownUntil)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		insert into token_runtime_state(token_id, cooldown_until, disabled_reason, failure_streak, last_failure_at, updated_at)
		values ($1, $2, $3, 1, now(), now())
		on conflict (token_id) do update
		set cooldown_until = coalesce(excluded.cooldown_until, token_runtime_state.cooldown_until),
		    disabled_reason = coalesce(excluded.disabled_reason, token_runtime_state.disabled_reason),
		    failure_streak = token_runtime_state.failure_streak + 1,
		    last_failure_at = now(),
		    updated_at = now()
	`, tokenID, cooldownUntil, truncate(message, 4000))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		insert into token_state_events(token_id, event_type, reason, cooldown_until)
		values ($1, $2, $3, $4)
	`, tokenID, map[bool]string{true: "disabled", false: "error"}[deactivate], truncate(message, 4000), cooldownUntil)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) SetTokenActive(ctx context.Context, tokenID int64, active bool, clearCooldown bool) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set is_active = $2,
		    disabled_at = case when $2 then null else now() end,
		    cooldown_until = case when $3 then null else cooldown_until end,
		    updated_at = now()
		where id = $1
		returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
	`, tokenID, active, clearCooldown || !active)
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) GetToken(ctx context.Context, tokenID int64) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		select id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where id = $1 and merged_into_token_id is null
	`, tokenID)
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) UpdateTokenRemark(ctx context.Context, tokenID int64, remark string) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set remark = nullif($2, ''), updated_at = now()
		where id = $1 and merged_into_token_id is null
		returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
	`, tokenID, truncate(remark, 2000))
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) DeleteToken(ctx context.Context, tokenID int64) error {
	tag, err := s.pool.Exec(ctx, `delete from codex_tokens where id = $1 or merged_into_token_id = $1`, tokenID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) RecordRefreshTokenIdentity(ctx context.Context, tokenID int64, refreshToken string) (RefreshTokenIdentity, error) {
	identity := RefreshTokenIdentity{TokenID: tokenID, Hash: hashString(refreshToken)}
	tag, err := s.pool.Exec(ctx, `
		insert into token_refresh_history(token_id, refresh_token_hash, seen_at)
		values ($1, $2, now())
		on conflict (token_id, refresh_token_hash) do nothing
	`, tokenID, identity.Hash)
	if err != nil {
		return RefreshTokenIdentity{}, err
	}
	identity.Created = tag.RowsAffected() > 0
	return identity, nil
}

func (s *Store) UpdateTokenSecret(ctx context.Context, update TokenSecretUpdate) error {
	if update.TokenID <= 0 {
		return fmt.Errorf("token id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		update codex_tokens
		set access_token = coalesce(nullif($2, ''), access_token),
		    refresh_token = coalesce(nullif($3, ''), refresh_token),
		    id_token = coalesce(nullif($4, ''), id_token),
		    expired = coalesce($5, expired),
		    account_id = coalesce(nullif($6, ''), account_id),
		    email = coalesce(nullif($7, ''), email),
		    plan_type = coalesce(nullif($8, ''), plan_type),
		    last_refresh = now(),
		    last_error = null,
		    is_active = true,
		    disabled_at = null,
		    updated_at = now()
		where id = $1 and merged_into_token_id is null
	`, update.TokenID, update.AccessToken, update.RefreshToken, update.IDToken, update.ExpiresAt, update.AccountID, update.Email, update.PlanType)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		insert into token_secrets(token_id, access_token, refresh_token, id_token, updated_at)
		values ($1, nullif($2, ''), nullif($3, ''), nullif($4, ''), now())
		on conflict (token_id) do update
		set access_token = coalesce(excluded.access_token, token_secrets.access_token),
		    refresh_token = coalesce(excluded.refresh_token, token_secrets.refresh_token),
		    id_token = coalesce(excluded.id_token, token_secrets.id_token),
		    updated_at = now()
	`, update.TokenID, update.AccessToken, update.RefreshToken, update.IDToken)
	if err != nil {
		return err
	}
	if strings.TrimSpace(update.RefreshToken) != "" {
		_, err = tx.Exec(ctx, `
			insert into token_refresh_history(token_id, refresh_token_hash, seen_at)
			values ($1, $2, now())
			on conflict (token_id, refresh_token_hash) do nothing
		`, update.TokenID, hashString(update.RefreshToken))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func scanTokens(rows pgx.Rows) ([]Token, error) {
	var tokens []Token
	for rows.Next() {
		token, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanToken(row rowScanner) (Token, error) {
	var token Token
	err := row.Scan(
		&token.ID,
		&token.Email,
		&token.AccountID,
		&token.AccessToken,
		&token.RefreshToken,
		&token.PlanType,
		&token.Remark,
		&token.SourceFile,
		&token.IsActive,
		&token.CooldownUntil,
		&token.DisabledAt,
		&token.LastUsedAt,
		&token.LastError,
		&token.CreatedAt,
		&token.UpdatedAt,
	)
	return token, err
}

func parseAccessTokenClaims(token string) (*string, *string, *string) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, nil, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, nil, nil
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, nil, nil
	}
	var accountID, email, planType *string
	for _, key := range []string{"account_id", "sub", "https://api.openai.com/auth/account_id"} {
		if value := stringClaim(claims, key); value != nil {
			accountID = value
			break
		}
	}
	for _, key := range []string{"email", "https://api.openai.com/auth/email"} {
		if value := stringClaim(claims, key); value != nil {
			email = value
			break
		}
	}
	for _, key := range []string{"plan_type", "plan", "https://api.openai.com/auth/plan_type"} {
		if value := stringClaim(claims, key); value != nil {
			planType = value
			break
		}
	}
	return accountID, email, planType
}

func stringClaim(claims map[string]any, key string) *string {
	value, ok := claims[key]
	if !ok {
		return nil
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return nil
	}
	return &text
}

func nullableString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func truncate(value string, max int) string {
	value = strings.TrimSpace(value)
	if max > 0 && len(value) > max {
		return value[:max]
	}
	return value
}
