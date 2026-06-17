package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	planFree    = "free"
	planPlus    = "plus"
	planTeam    = "team"
	planPro     = "pro"
	planUnknown = "unknown"
)

var primaryPlanOrder = []string{planFree, planPlus, planTeam, planPro}

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

type TokenObservedCost struct {
	ID              int64    `json:"id"`
	ObservedCostUSD *float64 `json:"observed_cost_usd"`
}

type TokenCounts struct {
	Total     int `json:"total"`
	Available int `json:"available"`
	Cooling   int `json:"cooling"`
	Disabled  int `json:"disabled"`
}

type TokenPlanCount struct {
	Plan  string `json:"plan"`
	Label string `json:"label"`
	Count int    `json:"count"`
}

type ImportResult struct {
	Created int                `json:"created"`
	Updated int                `json:"updated"`
	Skipped int                `json:"skipped"`
	Failed  int                `json:"failed"`
	Tokens  []Token            `json:"tokens"`
	Items   []ImportResultItem `json:"items,omitempty"`
}

type ImportResultItem struct {
	Index        int    `json:"index"`
	TokenID      *int64 `json:"token_id,omitempty"`
	Action       string `json:"action,omitempty"`
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type TokenListOptions struct {
	Limit  int
	Offset int
	Query  string
	Status string
	Plan   string
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
	where, args := tokenListWhere(opts, true)
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

func tokenListWhere(opts TokenListOptions, includePlan bool) (string, []any) {
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
	if includePlan {
		if plan := normalizePlanFilter(opts.Plan); plan != "" && plan != "all" {
			placeholder := arg(plan)
			if plan == planUnknown {
				filters = append(filters, fmt.Sprintf("(plan_type is null or btrim(plan_type) = '' or lower(btrim(plan_type)) = %s)", placeholder))
			} else {
				filters = append(filters, fmt.Sprintf("lower(btrim(plan_type)) = %s", placeholder))
			}
		}
	}
	where := strings.Join(filters, " and ")
	if where == "" {
		where = "true"
	}
	return where, args
}

func (s *Store) ListTokensByIDs(ctx context.Context, tokenIDs []int64) ([]Token, error) {
	ids := postgresIntIDs(tokenIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		select id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where id = any($1::integer[])
		  and merged_into_token_id is null
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokens(rows)
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

func (s *Store) TokenPlanCounts(ctx context.Context, opts TokenListOptions) ([]TokenPlanCount, error) {
	where, args := tokenListWhere(opts, false)
	rows, err := s.pool.Query(ctx, `
		with normalized as (
			select case
				when plan_type is null or btrim(plan_type) = '' then 'unknown'
				else lower(btrim(plan_type))
			end as plan
			from codex_tokens
			where `+where+`
		)
		select plan, count(*)::int
		from normalized
		group by plan
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	countByPlan := map[string]int{}
	for rows.Next() {
		var plan string
		var count int
		if err := rows.Scan(&plan, &count); err != nil {
			return nil, err
		}
		plan = normalizePlanFilter(plan)
		if plan == "" {
			plan = planUnknown
		}
		countByPlan[plan] += count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return buildPlanCounts(countByPlan), nil
}

func buildPlanCounts(countByPlan map[string]int) []TokenPlanCount {
	counts := make([]TokenPlanCount, 0, len(countByPlan)+len(primaryPlanOrder)+1)
	seen := map[string]struct{}{}
	appendPlan := func(plan string) {
		plan = normalizePlanFilter(plan)
		if plan == "" {
			plan = planUnknown
		}
		if _, ok := seen[plan]; ok {
			return
		}
		seen[plan] = struct{}{}
		counts = append(counts, TokenPlanCount{
			Plan:  plan,
			Label: planLabel(plan),
			Count: countByPlan[plan],
		})
	}
	for _, plan := range primaryPlanOrder {
		appendPlan(plan)
	}
	extras := make([]string, 0, len(countByPlan))
	for plan := range countByPlan {
		if _, ok := seen[plan]; ok || plan == planUnknown {
			continue
		}
		extras = append(extras, plan)
	}
	sort.Slice(extras, func(i, j int) bool {
		left, right := extras[i], extras[j]
		if countByPlan[left] != countByPlan[right] {
			return countByPlan[left] > countByPlan[right]
		}
		return left < right
	})
	for _, plan := range extras {
		appendPlan(plan)
	}
	appendPlan(planUnknown)
	return counts
}

func normalizePlanFilter(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func planLabel(plan string) string {
	switch normalizePlanFilter(plan) {
	case planFree:
		return "Free"
	case planPlus:
		return "Plus"
	case planTeam:
		return "Team"
	case planPro:
		return "Pro"
	case planUnknown, "":
		return "Unknown"
	default:
		return plan
	}
}

func (s *Store) UpsertAccessTokens(ctx context.Context, accessTokens []string, source string) (ImportResult, error) {
	payloads := make([]map[string]any, 0, len(accessTokens))
	for _, accessToken := range accessTokens {
		payloads = append(payloads, map[string]any{"access_token": accessToken})
	}
	return s.UpsertTokenPayloads(ctx, payloads, source)
}

func (s *Store) UpsertTokenPayloads(ctx context.Context, payloads []map[string]any, source string) (ImportResult, error) {
	result := ImportResult{}
	if len(payloads) == 0 {
		return result, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	for index, rawPayload := range payloads {
		resultIndex := importPayloadIndex(rawPayload, index)
		previousRefreshToken := stringFromPayload(rawPayload, "_previous_refresh_token")
		payload := normalizeTokenPayload(rawPayload)
		accessToken := stringFromPayload(payload, "access_token", "accessToken")
		refreshToken := stringFromPayload(payload, "refresh_token", "refreshToken")
		lastRefresh := timeFromPayload(payload, "last_refresh")
		if refreshToken == "" && accessToken != "" {
			refreshToken = "access:" + hashString(accessToken)
			payload["refresh_token"] = refreshToken
			payload["token_source"] = "access_token"
		}
		if refreshToken == "" {
			result.Failed++
			result.Items = append(result.Items, ImportResultItem{
				Index:        resultIndex,
				Action:       "failed",
				ErrorMessage: "token payload missing refresh_token or access_token",
				Status:       "failed",
			})
			continue
		}
		accountID, email, planType := parseAccessTokenClaims(accessToken)
		if value := stringFromPayload(payload, "account_id", "chatgpt_account_id"); value != "" {
			accountID = &value
		}
		if value := stringFromPayload(payload, "email"); value != "" {
			email = &value
		}
		if value := stringFromPayload(payload, "plan_type", "planType", "chatgpt_plan_type"); value != "" {
			planType = &value
		}
		idToken := nullableString(stringFromPayload(payload, "id_token", "idToken"))
		tokenType := stringFromPayload(payload, "type")
		if tokenType == "" {
			tokenType = "codex"
		}
		isActive := payloadBool(payload["is_active"], true)
		lastError := nullableString(stringFromPayload(payload, "last_error"))
		if isActive {
			lastError = nil
		}
		if sourceValue := strings.TrimSpace(source); sourceValue != "" {
			payload["source"] = sourceValue
		}
		if _, ok := payload["source"]; !ok && accessToken != "" {
			payload["source"] = "access_token_import"
		}

		var existingID int64
		err := tx.QueryRow(ctx, `
					select id
					from codex_tokens
					where merged_into_token_id is null
					  and refresh_token = any($1::text[])
					order by case when $2 <> '' and refresh_token = $2 then 0 else 1 end, id desc
					limit 1
				`, postgresTextArray(refreshToken, previousRefreshToken), strings.TrimSpace(previousRefreshToken)).Scan(&existingID)
		if err != nil && err != pgx.ErrNoRows {
			return result, err
		}
		if existingID > 0 {
			row := tx.QueryRow(ctx, `
				update codex_tokens
				set access_token = coalesce(nullif($2, ''), access_token),
				    account_id = coalesce($3, account_id),
					    email = coalesce($4, email),
					    plan_type = coalesce($5, plan_type),
					    source_file = coalesce($6, source_file),
					    id_token = coalesce($7, id_token),
					    type = coalesce(nullif($8, ''), type, 'codex'),
					    raw_payload = coalesce($9, raw_payload),
					    is_active = $10,
					    disabled_at = case when $10 then null else coalesce(disabled_at, now()) end,
					    last_error = $11,
					    refresh_token = coalesce(nullif($12, ''), refresh_token),
					    last_refresh = coalesce($13, last_refresh),
					    updated_at = now()
					where id = $1
					returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
					          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
				`, existingID, accessToken, accountID, email, planType, nullableString(source), idToken, tokenType, jsonBytes(payload), isActive, lastError, refreshToken, lastRefresh)
			token, scanErr := scanToken(row)
			if scanErr != nil {
				return result, scanErr
			}
			if err := recordTokenSecretAndRefreshHistory(ctx, tx, token.ID, accessToken, refreshToken, idToken); err != nil {
				return result, err
			}
			action := "updated"
			result.Updated++
			result.Tokens = append(result.Tokens, token)
			tokenID := token.ID
			result.Items = append(result.Items, ImportResultItem{
				Index:   resultIndex,
				TokenID: &tokenID,
				Action:  action,
				Status:  "published",
			})
			continue
		}

		row := tx.QueryRow(ctx, `
				insert into codex_tokens (
					email, account_id, id_token, access_token, refresh_token, raw_payload, plan_type, source_file,
					type, is_active, disabled_at, last_error, last_refresh, created_at, updated_at
				)
				values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, case when $10 then null else now() end, $11, $12, now(), now())
				returning id, email, account_id, access_token, refresh_token, plan_type, remark, source_file,
				          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
			`, email, accountID, idToken, accessToken, refreshToken, jsonBytes(payload), planType, nullableString(source), tokenType, isActive, lastError, lastRefresh)
		token, scanErr := scanToken(row)
		if scanErr != nil {
			return result, scanErr
		}
		if err := recordTokenSecretAndRefreshHistory(ctx, tx, token.ID, accessToken, refreshToken, idToken); err != nil {
			return result, err
		}
		result.Created++
		result.Tokens = append(result.Tokens, token)
		tokenID := token.ID
		result.Items = append(result.Items, ImportResultItem{
			Index:   resultIndex,
			TokenID: &tokenID,
			Action:  "created",
			Status:  "published",
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func recordTokenSecretAndRefreshHistory(ctx context.Context, tx pgx.Tx, tokenID int64, accessToken string, refreshToken string, idToken *string) error {
	if strings.TrimSpace(refreshToken) != "" {
		if _, err := tx.Exec(ctx, `
			insert into token_refresh_history(token_id, refresh_token_hash, seen_at)
			values ($1, $2, now())
			on conflict (token_id, refresh_token_hash) do nothing
		`, tokenID, hashString(refreshToken)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(accessToken) == "" && strings.TrimSpace(refreshToken) == "" && idToken == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into token_secrets(token_id, access_token, refresh_token, id_token, updated_at)
		values ($1, nullif($2, ''), nullif($3, ''), $4, now())
		on conflict (token_id) do update
		set access_token = coalesce(excluded.access_token, token_secrets.access_token),
		    refresh_token = coalesce(excluded.refresh_token, token_secrets.refresh_token),
		    id_token = coalesce(excluded.id_token, token_secrets.id_token),
		    updated_at = now()
	`, tokenID, accessToken, refreshToken, idToken)
	return err
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
	var accessToken sql.NullString
	var refreshToken sql.NullString
	err := row.Scan(
		&token.ID,
		&token.Email,
		&token.AccountID,
		&accessToken,
		&refreshToken,
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
	if accessToken.Valid {
		token.AccessToken = accessToken.String
	}
	if refreshToken.Valid {
		token.RefreshToken = refreshToken.String
	}
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

func timeFromPayload(payload map[string]any, keys ...string) *time.Time {
	for _, key := range keys {
		switch value := payload[key].(type) {
		case time.Time:
			if !value.IsZero() {
				normalized := value.UTC()
				return &normalized
			}
		case string:
			text := strings.TrimSpace(value)
			if text == "" {
				continue
			}
			if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
				normalized := parsed.UTC()
				return &normalized
			}
			if parsed, err := time.Parse(time.RFC3339, text); err == nil {
				normalized := parsed.UTC()
				return &normalized
			}
		}
	}
	return nil
}

func normalizeTokenPayload(payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+2)
	for key, value := range payload {
		if key == "_import_index" || key == "_previous_refresh_token" {
			continue
		}
		out[key] = value
	}
	if out["access_token"] == nil {
		if value := stringFromPayload(out, "accessToken"); value != "" {
			out["access_token"] = value
		}
	}
	if out["refresh_token"] == nil {
		if value := stringFromPayload(out, "refreshToken"); value != "" {
			out["refresh_token"] = value
		}
	}
	if out["access_token"] == nil && out["refresh_token"] == nil {
		if value := stringFromPayload(out, "token"); value != "" {
			if looksLikeAccessToken(value) {
				out["access_token"] = value
			} else {
				out["refresh_token"] = value
			}
		}
	}
	return out
}

func postgresTextArray(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func importPayloadIndex(payload map[string]any, fallback int) int {
	switch value := payload["_import_index"].(type) {
	case int:
		if value >= 0 {
			return value
		}
	case int64:
		if value >= 0 {
			return int(value)
		}
	case float64:
		index := int(value)
		if value == float64(index) && index >= 0 {
			return index
		}
	case json.Number:
		if parsed, err := value.Int64(); err == nil && parsed >= 0 {
			return int(parsed)
		}
	}
	return fallback
}

func stringFromPayload(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func payloadBool(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on", "active", "enabled":
			return true
		case "0", "false", "no", "off", "inactive", "disabled":
			return false
		}
	case float64:
		return typed != 0
	case int:
		return typed != 0
	}
	return fallback
}

func looksLikeAccessToken(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "eyJ")
}

func truncate(value string, max int) string {
	value = strings.TrimSpace(value)
	if max > 0 && len(value) > max {
		return value[:max]
	}
	return value
}

func postgresIntIDs(ids []int64) []int32 {
	out := make([]int32, 0, len(ids))
	seen := map[int32]struct{}{}
	for _, id := range ids {
		if id <= 0 || id > 2147483647 {
			continue
		}
		value := int32(id)
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
