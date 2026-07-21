package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/yym68686/oaix/internal/agentidentity"
)

const (
	planFree    = "free"
	planPlus    = "plus"
	planTeam    = "team"
	planPro     = "pro"
	planUnknown = "unknown"

	DefaultMarketplacePriceBPS = 250
	MaxMarketplacePriceBPS     = 250
	MarketplacePriceStepBPS    = 10

	AccessTokenOnlyRefreshTokenPrefix       = "access:"
	legacyAccessTokenOnlyRefreshTokenPrefix = "__oaix_access_token_only__:"

	transientRetryCooldownPrefix        = "retryable upstream failure:"
	transientRetryCooldownDisplayWindow = time.Minute
)

var primaryPlanOrder = []string{planFree, planPlus, planTeam, planPro}

type Token struct {
	ID                        int64                      `json:"id"`
	OwnerUserID               int64                      `json:"owner_user_id"`
	Email                     *string                    `json:"email,omitempty"`
	AccountID                 *string                    `json:"account_id,omitempty"`
	AccessToken               string                     `json:"-"`
	RefreshToken              string                     `json:"-"`
	AgentIdentity             *agentidentity.Credentials `json:"-"`
	PlanType                  *string                    `json:"plan_type,omitempty"`
	Remark                    *string                    `json:"remark,omitempty"`
	SourceFile                *string                    `json:"source_file,omitempty"`
	IsActive                  bool                       `json:"is_active"`
	CooldownUntil             *time.Time                 `json:"cooldown_until,omitempty"`
	DisabledAt                *time.Time                 `json:"disabled_at,omitempty"`
	ShareEnabled              bool                       `json:"share_enabled"`
	ShareStatus               string                     `json:"share_status,omitempty"`
	ShareReason               *string                    `json:"share_disabled_reason,omitempty"`
	ShareEnabledAt            *time.Time                 `json:"share_enabled_at,omitempty"`
	ShareDisabledAt           *time.Time                 `json:"share_disabled_at,omitempty"`
	MarketplacePriceBPS       *int                       `json:"marketplace_price_bps,omitempty"`
	MarketplacePriceUpdatedAt *time.Time                 `json:"marketplace_price_updated_at,omitempty"`
	MarketplacePriceSource    string                     `json:"marketplace_price_source,omitempty"`
	LastUsedAt                *time.Time                 `json:"last_used_at,omitempty"`
	LastError                 *string                    `json:"last_error,omitempty"`
	CreatedAt                 time.Time                  `json:"created_at"`
	UpdatedAt                 time.Time                  `json:"updated_at"`
}

func (t Token) IsAgentIdentity() bool {
	return t.AgentIdentity != nil || strings.HasPrefix(strings.TrimSpace(t.RefreshToken), agentidentity.SyntheticRefreshPrefix)
}

func (t Token) IsAccessTokenOnly() bool {
	return IsAccessTokenOnlyRefreshToken(t.RefreshToken)
}

func (t Token) HasRefreshableOAuthToken() bool {
	return !t.IsAgentIdentity() && !t.IsAccessTokenOnly() && strings.TrimSpace(t.RefreshToken) != ""
}

func IsAccessTokenOnlyRefreshToken(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, AccessTokenOnlyRefreshTokenPrefix) ||
		strings.HasPrefix(value, legacyAccessTokenOnlyRefreshTokenPrefix)
}

func accessTokenOnlyRefreshToken(accessToken string) string {
	return AccessTokenOnlyRefreshTokenPrefix + hashString(strings.TrimSpace(accessToken))
}

type TokenObservedCost struct {
	ID                      int64      `json:"id"`
	ObservedCostUSD         *float64   `json:"observed_cost_usd"`
	LocalObservedCostUSD    *float64   `json:"local_observed_cost_usd"`
	Sub2APIObservedCostUSD  *float64   `json:"sub2api_observed_cost_usd"`
	CombinedObservedCostUSD *float64   `json:"combined_observed_cost_usd"`
	Sub2APIUsageSyncedAt    *time.Time `json:"sub2api_usage_synced_at,omitempty"`
	Sub2APIUsageStale       bool       `json:"sub2api_usage_stale"`
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

type TokenErrorCount struct {
	ErrorType string `json:"error_type"`
	Count     int    `json:"count"`
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
	Index                  int        `json:"index"`
	TokenID                *int64     `json:"token_id,omitempty"`
	Action                 string     `json:"action,omitempty"`
	Status                 string     `json:"status"`
	ErrorMessage           string     `json:"error_message,omitempty"`
	MatchedExistingTokenID *int64     `json:"matched_existing_token_id,omitempty"`
	Reactivated            bool       `json:"reactivated,omitempty"`
	PreviousIsActive       *bool      `json:"previous_is_active,omitempty"`
	NextIsActive           *bool      `json:"next_is_active,omitempty"`
	PreviousDisabledAt     *time.Time `json:"previous_disabled_at,omitempty"`
	NextDisabledAt         *time.Time `json:"next_disabled_at,omitempty"`
	RefreshErrorCode       string     `json:"refresh_error_code,omitempty"`
	RefreshErrorMessage    string     `json:"refresh_error_message_excerpt,omitempty"`
}

type TokenListOptions struct {
	Limit          int
	Offset         int
	Cursor         string
	OwnerUserID    int64
	Query          string
	Status         string
	Plan           string
	Sort           string
	ImportJobID    int64
	Source         string
	SourceFile     string
	CreatedFrom    *time.Time
	CreatedTo      *time.Time
	LastUsedFrom   *time.Time
	LastUsedTo     *time.Time
	HasError       *bool
	ErrorCode      string
	CooldownReason string
	QuotaState     string
	StatusAsOf     *time.Time
}

type TokenSharingCounts struct {
	Shared  int `json:"shared"`
	Private int `json:"private"`
}

type OwnerTokenPoolSummary struct {
	Counts        TokenCounts
	SharingCounts TokenSharingCounts
	PlanCounts    []TokenPlanCount
}

type TokenMarketplacePricingSummary struct {
	DefaultPriceBPS int  `json:"default_price_bps"`
	MinPriceBPS     *int `json:"min_price_bps,omitempty"`
	MaxPriceBPS     *int `json:"max_price_bps,omitempty"`
	AvgPriceBPS     *int `json:"avg_price_bps,omitempty"`
	PricedCount     int  `json:"priced_count"`
	Total           int  `json:"total"`
}

type TokenMarketplaceAvailablePriceSummary struct {
	DefaultPriceBPS     int    `json:"default_price_bps"`
	PriceStepBPS        int    `json:"price_step_bps"`
	LowestPriceBPS      *int   `json:"lowest_price_bps,omitempty"`
	AvailableTokenCount int    `json:"available_token_count"`
	LowestTokenCount    int    `json:"lowest_token_count"`
	ExcludedOwnerUserID *int64 `json:"excluded_owner_user_id,omitempty"`
}

type TokenMarketplaceAvailablePricingSummary struct {
	All        TokenMarketplaceAvailablePriceSummary `json:"all"`
	Competitor TokenMarketplaceAvailablePriceSummary `json:"competitor"`
}

type RefreshTokenIdentity struct {
	TokenID int64  `json:"token_id"`
	Hash    string `json:"hash"`
	Created bool   `json:"created"`
}

type TokenSecretUpdate struct {
	TokenID                int64
	AccessToken            string
	RefreshToken           string
	IDToken                string
	ExpiresAt              *time.Time
	AccountID              string
	Email                  string
	PlanType               string
	PreserveActivation     bool
	RequireCredentialMatch bool
	ExpectedAccessToken    string
	ExpectedRefreshToken   string
}

var ErrTokenCredentialsChanged = errors.New("token credentials changed concurrently")
var ErrTokenStateChanged = errors.New("token state changed concurrently")
var ErrAgentIdentityTaskChanged = errors.New("agent identity task changed concurrently")

func (s *Store) ListAvailableTokens(ctx context.Context) ([]Token, error) {
	return s.ListAvailableTokensScoped(ctx, AllResources())
}

func (s *Store) ListAvailableTokensScoped(ctx context.Context, scope ResourceScope) ([]Token, error) {
	args := []any{}
	ownerWhere := scope.ownerFilter("t.owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select t.id, coalesce(t.owner_user_id, 0), t.email, t.account_id, t.access_token, t.refresh_token, t.plan_type, t.remark, t.source_file,
		       t.is_active, t.cooldown_until, t.disabled_at,
		       t.share_enabled, t.share_status, t.share_disabled_reason, t.share_enabled_at, t.share_disabled_at,
		       t.marketplace_price_bps, t.marketplace_price_updated_at, t.marketplace_price_source,
		       t.last_used_at, t.last_error, t.created_at, t.updated_at,
		       ai.agent_runtime_id, ai.agent_private_key, ai.task_id, ai.chatgpt_user_id,
		       ai.workspace_id, ai.chatgpt_account_is_fedramp
		from codex_tokens t
		left join token_agent_identities ai on ai.token_id = t.id
		where t.is_active = true
		  and t.merged_into_token_id is null
		  and `+ownerWhere+`
		  and (coalesce(t.access_token, '') <> '' or ai.token_id is not null)
		  and (t.cooldown_until is null or t.cooldown_until <= now())
		  and t.disabled_at is null
		order by coalesce(t.last_used_at, 'epoch'::timestamptz) asc, t.id asc
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRuntimeTokens(rows)
}

func (s *Store) ListTokens(ctx context.Context, opts TokenListOptions) ([]Token, int, error) {
	return s.ListTokensScoped(ctx, AllResources(), opts)
}

func (s *Store) ListTokensScoped(ctx context.Context, scope ResourceScope, opts TokenListOptions) ([]Token, int, error) {
	limit := opts.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := opts.Offset
	if offset < 0 {
		offset = 0
	}
	where, args := tokenListWhereScoped(opts, true, scope)
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from codex_tokens where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	orderBy := "id desc"
	switch strings.ToLower(strings.TrimSpace(opts.Sort)) {
	case "oldest":
		orderBy = "id asc"
	case "created_at":
		orderBy = "id asc"
	case "-created_at", "latest", "newest":
		orderBy = "id desc"
	case "last_used":
		orderBy = "last_used_at desc nulls last, id desc"
	case "-last_used_at":
		orderBy = "last_used_at desc nulls last, id desc"
	case "available":
		orderBy = "is_active desc, cooldown_until asc nulls first, last_used_at asc nulls first"
	case "status":
		orderBy = "is_active desc, disabled_at asc nulls first, cooldown_until asc nulls first, id desc"
	}
	args = append(args, limit, offset)
	query := fmt.Sprintf(`
		select id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at,
		       share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		       marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		       last_used_at, last_error, created_at, updated_at
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
	items, err := scanAvailableTokens(rows)
	if err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func tokenListWhere(opts TokenListOptions, includePlan bool) (string, []any) {
	return tokenListWhereScoped(opts, includePlan, AllResources())
}

func TokenListStatusAsOf(opts TokenListOptions) time.Time {
	if opts.StatusAsOf != nil && !opts.StatusAsOf.IsZero() {
		return opts.StatusAsOf.UTC()
	}
	return time.Now().UTC()
}

func TokenHasTransientRetryBackoff(token Token, asOf time.Time) bool {
	if token.CooldownUntil == nil {
		return false
	}
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	} else {
		asOf = asOf.UTC()
	}
	if !token.CooldownUntil.After(asOf) || token.CooldownUntil.Sub(asOf) > transientRetryCooldownDisplayWindow {
		return false
	}
	if token.LastError == nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(*token.LastError), transientRetryCooldownPrefix)
}

func tokenTransientRetryBackoffWhere(asOf string) string {
	return fmt.Sprintf("(coalesce(last_error, '') like '%s%%' and cooldown_until <= %s + interval '1 minute')", transientRetryCooldownPrefix, asOf)
}

func tokenDisplayAvailableWhere(asOf string) string {
	return fmt.Sprintf("(cooldown_until is null or cooldown_until <= %s or %s)", asOf, tokenTransientRetryBackoffWhere(asOf))
}

func tokenDisplayCoolingWhere(asOf string) string {
	return fmt.Sprintf("(cooldown_until > %s and not %s)", asOf, tokenTransientRetryBackoffWhere(asOf))
}

func tokenCredentialReadyWhere() string {
	return "(coalesce(access_token, '') <> '' or exists (select 1 from token_agent_identities where token_id = codex_tokens.id))"
}

func tokenListWhereScoped(opts TokenListOptions, includePlan bool, scope ResourceScope) (string, []any) {
	var filters []string
	var args []any
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	filters = append(filters, "merged_into_token_id is null")
	filters = append(filters, scope.ownerFilter("owner_user_id", &args))
	if opts.OwnerUserID > 0 {
		placeholder := arg(opts.OwnerUserID)
		filters = append(filters, fmt.Sprintf("owner_user_id = %s", placeholder))
	}
	if q := strings.TrimSpace(opts.Query); q != "" {
		placeholder := arg("%" + q + "%")
		filters = append(filters, fmt.Sprintf("(email ilike %s or account_id ilike %s or remark ilike %s)", placeholder, placeholder, placeholder))
	}
	switch strings.ToLower(strings.TrimSpace(opts.Status)) {
	case "available", "active":
		asOf := arg(TokenListStatusAsOf(opts))
		filters = append(filters, fmt.Sprintf("is_active = true and disabled_at is null and %s", tokenDisplayAvailableWhere(asOf)))
	case "cooling", "cooldown":
		asOf := arg(TokenListStatusAsOf(opts))
		filters = append(filters, fmt.Sprintf("is_active = true and disabled_at is null and %s", tokenDisplayCoolingWhere(asOf)))
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
	if opts.ImportJobID > 0 {
		placeholder := arg(opts.ImportJobID)
		filters = append(filters, fmt.Sprintf(`exists (
			select 1
			from token_import_items import_item
			where import_item.token_id = codex_tokens.id
			  and import_item.job_id = %s
		)`, placeholder))
	}
	if source := strings.TrimSpace(opts.Source); source != "" {
		placeholder := arg(source)
		filters = append(filters, fmt.Sprintf("coalesce(raw_payload->>'source', '') = %s", placeholder))
	}
	if sourceFile := strings.TrimSpace(opts.SourceFile); sourceFile != "" {
		placeholder := arg("%" + sourceFile + "%")
		filters = append(filters, fmt.Sprintf("coalesce(source_file, '') ilike %s", placeholder))
	}
	if opts.CreatedFrom != nil {
		filters = append(filters, fmt.Sprintf("created_at >= %s", arg(*opts.CreatedFrom)))
	}
	if opts.CreatedTo != nil {
		filters = append(filters, fmt.Sprintf("created_at <= %s", arg(*opts.CreatedTo)))
	}
	if opts.LastUsedFrom != nil {
		filters = append(filters, fmt.Sprintf("last_used_at >= %s", arg(*opts.LastUsedFrom)))
	}
	if opts.LastUsedTo != nil {
		filters = append(filters, fmt.Sprintf("last_used_at <= %s", arg(*opts.LastUsedTo)))
	}
	if opts.HasError != nil {
		if *opts.HasError {
			filters = append(filters, "last_error is not null and btrim(last_error) <> ''")
		} else {
			filters = append(filters, "(last_error is null or btrim(last_error) = '')")
		}
	}
	if errorCode := strings.TrimSpace(opts.ErrorCode); errorCode != "" {
		placeholder := arg("%" + errorCode + "%")
		filters = append(filters, fmt.Sprintf("coalesce(last_error, '') ilike %s", placeholder))
	}
	if reason := strings.TrimSpace(opts.CooldownReason); reason != "" {
		placeholder := arg("%" + reason + "%")
		filters = append(filters, fmt.Sprintf("coalesce(last_error, '') ilike %s", placeholder))
	}
	where := strings.Join(filters, " and ")
	if where == "" {
		where = "true"
	}
	return where, args
}

func (s *Store) ListTokensByIDs(ctx context.Context, tokenIDs []int64) ([]Token, error) {
	return s.ListTokensByIDsScoped(ctx, AllResources(), tokenIDs)
}

func (s *Store) ListTokensByIDsScoped(ctx context.Context, scope ResourceScope, tokenIDs []int64) ([]Token, error) {
	ids := postgresIntIDs(tokenIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	args := []any{ids}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at,
		       share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		       marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		       last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where id = any($1::integer[])
		  and merged_into_token_id is null
		  and `+ownerWhere, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanAvailableTokens(rows)
}

func (s *Store) TokenCounts(ctx context.Context) (TokenCounts, error) {
	return s.TokenCountsScoped(ctx, AllResources())
}

func (s *Store) TokenCountsScoped(ctx context.Context, scope ResourceScope) (TokenCounts, error) {
	return s.TokenCountsScopedAt(ctx, scope, time.Now().UTC())
}

func (s *Store) TokenCountsScopedAt(ctx context.Context, scope ResourceScope, asOf time.Time) (TokenCounts, error) {
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	var counts TokenCounts
	args := []any{asOf.UTC()}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	err := s.pool.QueryRow(ctx, `
		select
			count(*)::int as total,
			count(*) filter (
				where is_active = true
				  and disabled_at is null
				  and `+tokenCredentialReadyWhere()+`
				  and `+tokenDisplayAvailableWhere("$1")+`
			)::int as available,
			count(*) filter (where is_active = true and disabled_at is null and `+tokenDisplayCoolingWhere("$1")+`)::int as cooling,
			count(*) filter (where is_active = false or disabled_at is not null)::int as disabled
		from codex_tokens
		where merged_into_token_id is null
		  and `+ownerWhere, args...).Scan(&counts.Total, &counts.Available, &counts.Cooling, &counts.Disabled)
	return counts, err
}

func (s *Store) TokenSharingCountsScoped(ctx context.Context, scope ResourceScope) (TokenSharingCounts, error) {
	var counts TokenSharingCounts
	args := []any{}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	err := s.pool.QueryRow(ctx, `
		select
			count(*) filter (
				where share_enabled = true
				  and lower(coalesce(share_status, '')) = 'active'
			)::int as shared,
			count(*) filter (
				where share_enabled = false
				   or lower(coalesce(share_status, '')) <> 'active'
			)::int as private
		from codex_tokens
		where merged_into_token_id is null
		  and `+ownerWhere, args...).Scan(&counts.Shared, &counts.Private)
	return counts, err
}

func (s *Store) TokenPoolSummariesByOwner(ctx context.Context, ownerIDs []int64, asOf time.Time) (map[int64]OwnerTokenPoolSummary, error) {
	ids := uniquePositiveInt64s(ownerIDs)
	out := make(map[int64]OwnerTokenPoolSummary, len(ids))
	planCountsByOwner := make(map[int64]map[string]int, len(ids))
	for _, ownerID := range ids {
		out[ownerID] = OwnerTokenPoolSummary{}
		planCountsByOwner[ownerID] = map[string]int{}
	}
	if len(ids) == 0 {
		return out, nil
	}
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}

	rows, err := s.pool.Query(ctx, `
		select
			owner_user_id,
			case
				when plan_type is null or btrim(plan_type) = '' then 'unknown'
				else lower(btrim(plan_type))
			end as plan,
			count(*)::int as total,
			count(*) filter (
				where is_active = true
				  and disabled_at is null
				  and `+tokenCredentialReadyWhere()+`
				  and `+tokenDisplayAvailableWhere("$1")+`
			)::int as available,
			count(*) filter (where is_active = true and disabled_at is null and `+tokenDisplayCoolingWhere("$1")+`)::int as cooling,
			count(*) filter (where is_active = false or disabled_at is not null)::int as disabled,
			count(*) filter (where share_enabled = true and lower(coalesce(share_status, '')) = 'active')::int as shared,
			count(*) filter (where share_enabled = false or lower(coalesce(share_status, '')) <> 'active')::int as private
		from codex_tokens
		where merged_into_token_id is null
		  and owner_user_id = any($2::bigint[])
		group by owner_user_id, 2
	`, asOf.UTC(), ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ownerID int64
		var plan string
		var counts TokenCounts
		var sharingCounts TokenSharingCounts
		if err := rows.Scan(
			&ownerID, &plan,
			&counts.Total, &counts.Available, &counts.Cooling, &counts.Disabled,
			&sharingCounts.Shared, &sharingCounts.Private,
		); err != nil {
			return nil, err
		}
		summary := out[ownerID]
		summary.Counts.Total += counts.Total
		summary.Counts.Available += counts.Available
		summary.Counts.Cooling += counts.Cooling
		summary.Counts.Disabled += counts.Disabled
		summary.SharingCounts.Shared += sharingCounts.Shared
		summary.SharingCounts.Private += sharingCounts.Private
		out[ownerID] = summary

		plan = normalizePlanFilter(plan)
		if plan == "" {
			plan = planUnknown
		}
		planCountsByOwner[ownerID][plan] += counts.Total
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, ownerID := range ids {
		summary := out[ownerID]
		summary.PlanCounts = buildPlanCounts(planCountsByOwner[ownerID])
		out[ownerID] = summary
	}
	return out, nil
}

func (s *Store) TokenMarketplacePricingSummaryScoped(ctx context.Context, scope ResourceScope) (TokenMarketplacePricingSummary, error) {
	summary := TokenMarketplacePricingSummary{DefaultPriceBPS: DefaultMarketplacePriceBPS}
	args := []any{DefaultMarketplacePriceBPS}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	var minPrice sql.NullInt64
	var maxPrice sql.NullInt64
	var avgPrice sql.NullFloat64
	err := s.pool.QueryRow(ctx, `
		select
			count(*)::int as total,
			count(marketplace_price_bps)::int as priced_count,
			min(coalesce(marketplace_price_bps, $1))::bigint as min_price_bps,
			max(coalesce(marketplace_price_bps, $1))::bigint as max_price_bps,
			avg(coalesce(marketplace_price_bps, $1))::float8 as avg_price_bps
		from codex_tokens
		where merged_into_token_id is null
		  and `+ownerWhere, args...).Scan(&summary.Total, &summary.PricedCount, &minPrice, &maxPrice, &avgPrice)
	if err != nil {
		return summary, err
	}
	if minPrice.Valid {
		value := int(minPrice.Int64)
		summary.MinPriceBPS = &value
	}
	if maxPrice.Valid {
		value := int(maxPrice.Int64)
		summary.MaxPriceBPS = &value
	}
	if avgPrice.Valid {
		value := int(avgPrice.Float64 + 0.5)
		summary.AvgPriceBPS = &value
	}
	return summary, nil
}

func (s *Store) TokenMarketplaceAvailablePricingSummary(ctx context.Context, excludeOwnerUserID int64) (TokenMarketplaceAvailablePricingSummary, error) {
	all, err := s.tokenMarketplaceAvailablePriceFloor(ctx, nil)
	if err != nil {
		return TokenMarketplaceAvailablePricingSummary{}, err
	}
	var exclude *int64
	if excludeOwnerUserID > 0 {
		exclude = &excludeOwnerUserID
	}
	competitor, err := s.tokenMarketplaceAvailablePriceFloor(ctx, exclude)
	if err != nil {
		return TokenMarketplaceAvailablePricingSummary{}, err
	}
	return TokenMarketplaceAvailablePricingSummary{All: all, Competitor: competitor}, nil
}

func (s *Store) tokenMarketplaceAvailablePriceFloor(ctx context.Context, excludeOwnerUserID *int64) (TokenMarketplaceAvailablePriceSummary, error) {
	summary := TokenMarketplaceAvailablePriceSummary{
		DefaultPriceBPS: DefaultMarketplacePriceBPS,
		PriceStepBPS:    MarketplacePriceStepBPS,
	}
	args := []any{DefaultMarketplacePriceBPS}
	ownerWhere := "true"
	if excludeOwnerUserID != nil && *excludeOwnerUserID > 0 {
		args = append(args, *excludeOwnerUserID)
		ownerWhere = fmt.Sprintf("owner_user_id is distinct from $%d", len(args))
		summary.ExcludedOwnerUserID = excludeOwnerUserID
	}
	var lowest sql.NullInt64
	err := s.pool.QueryRow(ctx, `
		with available as (
			select coalesce(marketplace_price_bps, $1)::int as price_bps
			from codex_tokens
			where merged_into_token_id is null
			  and share_enabled = true
			  and share_status = 'active'
			  and is_active = true
			  and disabled_at is null
			  and `+tokenCredentialReadyWhere()+`
			  and (cooldown_until is null or cooldown_until <= now())
			  and `+ownerWhere+`
		),
		lowest as (
			select min(price_bps)::bigint as price_bps
			from available
		)
		select
			count(*)::int,
			(select price_bps from lowest),
			count(*) filter (where price_bps = (select price_bps from lowest))::int
		from available
	`, args...).Scan(&summary.AvailableTokenCount, &lowest, &summary.LowestTokenCount)
	if err != nil {
		return summary, err
	}
	if lowest.Valid {
		value := int(lowest.Int64)
		summary.LowestPriceBPS = &value
	}
	return summary, nil
}

func (s *Store) TokenPlanCounts(ctx context.Context, opts TokenListOptions) ([]TokenPlanCount, error) {
	return s.TokenPlanCountsScoped(ctx, AllResources(), opts)
}

func (s *Store) TokenPlanCountsScoped(ctx context.Context, scope ResourceScope, opts TokenListOptions) ([]TokenPlanCount, error) {
	where, args := tokenListWhereScoped(opts, false, scope)
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

func (s *Store) TokenErrorCountsScoped(ctx context.Context, scope ResourceScope, limit int) ([]TokenErrorCount, error) {
	if limit <= 0 || limit > 50 {
		limit = 10
	}
	args := []any{}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, `
		select
			case
				when lower(last_error) like '%deactivated_workspace%' then 'deactivated_workspace'
				when lower(last_error) like '%invalid_grant%' then 'invalid_grant'
				when lower(last_error) like '%unauthorized%' or lower(last_error) like '%401%' then 'unauthorized'
				when lower(last_error) like '%forbidden%' or lower(last_error) like '%403%' then 'forbidden'
				when lower(last_error) like '%rate_limit%' or lower(last_error) like '%429%' then 'rate_limit'
				when lower(last_error) like '%quota%' or lower(last_error) like '%402%' then 'quota_or_billing'
				when lower(last_error) like '%timeout%' then 'timeout'
				else left(regexp_replace(lower(coalesce(last_error, 'unknown')), '\s+', ' ', 'g'), 120)
			end as error_type,
			count(*)::int
		from codex_tokens
		where merged_into_token_id is null
		  and last_error is not null
		  and btrim(last_error) <> ''
		  and `+ownerWhere+`
		group by 1
		order by count(*) desc, error_type asc
		limit $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []TokenErrorCount{}
	for rows.Next() {
		var item TokenErrorCount
		if err := rows.Scan(&item.ErrorType, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
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
	ownerID, err := s.BootstrapUserID(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	return s.UpsertAccessTokensForOwner(ctx, ownerID, accessTokens, source)
}

func (s *Store) UpsertAccessTokensForOwner(ctx context.Context, ownerUserID int64, accessTokens []string, source string) (ImportResult, error) {
	payloads := make([]map[string]any, 0, len(accessTokens))
	for _, accessToken := range accessTokens {
		payloads = append(payloads, map[string]any{"access_token": accessToken})
	}
	return s.UpsertTokenPayloadsForOwner(ctx, ownerUserID, payloads, source)
}

func (s *Store) UpsertTokenPayloads(ctx context.Context, payloads []map[string]any, source string) (ImportResult, error) {
	ownerID, err := s.BootstrapUserID(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	return s.UpsertTokenPayloadsForOwner(ctx, ownerID, payloads, source)
}

func (s *Store) UpsertTokenPayloadsForOwner(ctx context.Context, ownerUserID int64, payloads []map[string]any, source string) (ImportResult, error) {
	result := ImportResult{}
	if len(payloads) == 0 {
		return result, nil
	}
	if ownerUserID <= 0 {
		return result, fmt.Errorf("owner user id is required")
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
		agentCredentials, isAgentIdentity, agentErr := agentidentity.Parse(payload)
		if agentErr != nil {
			result.Failed++
			result.Items = append(result.Items, ImportResultItem{
				Index:        resultIndex,
				Action:       "failed",
				ErrorMessage: agentErr.Error(),
				Status:       "failed",
			})
			continue
		}
		accessToken := stringFromPayload(payload, "access_token", "accessToken")
		refreshToken := stringFromPayload(payload, "refresh_token", "refreshToken")
		lastRefresh := timeFromPayload(payload, "last_refresh")
		if isAgentIdentity {
			accessToken = ""
			refreshToken = agentCredentials.IdentityToken()
			previousRefreshToken = ""
			payload["auth_mode"] = agentidentity.AuthMode
		}
		if refreshToken == "" && accessToken != "" {
			refreshToken = accessTokenOnlyRefreshToken(accessToken)
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
		if isAgentIdentity {
			accountID = nullableString(agentCredentials.AccountID)
			email = nullableString(agentCredentials.Email)
			planType = nullableString(agentCredentials.PlanType)
		}
		idToken := nullableString(stringFromPayload(payload, "id_token", "idToken"))
		tokenType := stringFromPayload(payload, "type")
		if tokenType == "" || isAgentIdentity {
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
		shareEnabled, shareStatus, shareExplicit := importSharingFromPayload(payload)
		storedPayload := payload
		if isAgentIdentity {
			storedPayload = agentidentity.SanitizedPayload(payload)
		}

		existingID, err := findExistingTokenForImport(ctx, tx, ownerUserID, refreshToken, previousRefreshToken, accountID, email, !isAgentIdentity)
		if err != nil {
			return result, err
		}
		if existingID > 0 {
			var previousActive bool
			var previousDisabledAt *time.Time
			if err := tx.QueryRow(ctx, `
				select is_active, disabled_at
				from codex_tokens
				where id = $1
				for update
			`, existingID).Scan(&previousActive, &previousDisabledAt); err != nil {
				return result, err
			}
			row := tx.QueryRow(ctx, `
				update codex_tokens
				set access_token = case when $17 then null else coalesce(nullif($2, ''), access_token) end,
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
					    share_enabled = case when $14 then $15 else share_enabled end,
					    share_status = case when $14 then $16 else share_status end,
					    share_disabled_reason = case when $14 and $15 then null when $14 and not $15 then null else share_disabled_reason end,
					    share_enabled_at = case when $14 and $15 then coalesce(share_enabled_at, now()) else share_enabled_at end,
					    share_disabled_at = case when $14 and $15 then null when $14 and not $15 then now() else share_disabled_at end,
					    updated_at = now()
					where id = $1
					returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
					          is_active, cooldown_until, disabled_at,
					          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
					          marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
					          last_used_at, last_error, created_at, updated_at
				`, existingID, accessToken, accountID, email, planType, nullableString(source), idToken, tokenType, jsonBytes(storedPayload), isActive, lastError, refreshToken, lastRefresh, shareExplicit, shareEnabled, shareStatus, isAgentIdentity)
			token, scanErr := scanTokenWithSharing(row)
			if scanErr != nil {
				return result, scanErr
			}
			if isAgentIdentity {
				if err := recordAgentIdentity(ctx, tx, ownerUserID, token.ID, agentCredentials, idToken); err != nil {
					return result, err
				}
			} else {
				if err := recordTokenSecretAndRefreshHistory(ctx, tx, ownerUserID, token.ID, accessToken, refreshToken, idToken); err != nil {
					return result, err
				}
				if _, err := tx.Exec(ctx, `delete from token_agent_identities where token_id = $1`, token.ID); err != nil {
					return result, err
				}
			}
			action := "updated"
			result.Updated++
			result.Tokens = append(result.Tokens, token)
			tokenID := token.ID
			nextActive := token.IsActive
			result.Items = append(result.Items, ImportResultItem{
				Index:                  resultIndex,
				TokenID:                &tokenID,
				Action:                 action,
				Status:                 "published",
				MatchedExistingTokenID: &tokenID,
				Reactivated:            !previousActive && token.IsActive,
				PreviousIsActive:       &previousActive,
				NextIsActive:           &nextActive,
				PreviousDisabledAt:     previousDisabledAt,
				NextDisabledAt:         token.DisabledAt,
			})
			if !previousActive && token.IsActive {
				_, _ = tx.Exec(ctx, `
					insert into token_state_events(
						token_id, owner_user_id, event_type, reason, previous_is_active, next_is_active, metadata
					)
					values ($1, $2, 'reactivated_by_import', $3, $4, $5, $6)
				`, token.ID, ownerUserID, truncate("token reactivated by import", 4000), previousActive, nextActive, jsonBytes(map[string]any{
					"source":                    source,
					"matched_existing_token_id": token.ID,
					"previous_disabled_at":      previousDisabledAt,
					"next_disabled_at":          token.DisabledAt,
				}))
			}
			continue
		}

		row := tx.QueryRow(ctx, `
				insert into codex_tokens (
					owner_user_id, email, account_id, id_token, access_token, refresh_token, raw_payload, plan_type, source_file,
					type, is_active, disabled_at, last_error, last_refresh, share_enabled, share_status,
					share_enabled_at, share_disabled_at, created_at, updated_at
				)
				values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, case when $11 then null else now() end,
				        $12, $13, $14, $15, case when $14 then now() else null end, case when $16 and not $14 then now() else null end, now(), now())
				returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
				          is_active, cooldown_until, disabled_at,
				          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
				          marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
				          last_used_at, last_error, created_at, updated_at
			`, ownerUserID, email, accountID, idToken, accessToken, refreshToken, jsonBytes(storedPayload), planType, nullableString(source), tokenType, isActive, lastError, lastRefresh, shareEnabled, shareStatus, shareExplicit)
		token, scanErr := scanTokenWithSharing(row)
		if scanErr != nil {
			return result, scanErr
		}
		if isAgentIdentity {
			if err := recordAgentIdentity(ctx, tx, ownerUserID, token.ID, agentCredentials, idToken); err != nil {
				return result, err
			}
		} else {
			if err := recordTokenSecretAndRefreshHistory(ctx, tx, ownerUserID, token.ID, accessToken, refreshToken, idToken); err != nil {
				return result, err
			}
		}
		result.Created++
		result.Tokens = append(result.Tokens, token)
		tokenID := token.ID
		nextActive := token.IsActive
		result.Items = append(result.Items, ImportResultItem{
			Index:        resultIndex,
			TokenID:      &tokenID,
			Action:       "created",
			Status:       "published",
			NextIsActive: &nextActive,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func findExistingTokenForImport(ctx context.Context, tx pgx.Tx, ownerUserID int64, refreshToken string, previousRefreshToken string, accountID *string, email *string, allowEmailFallback bool) (int64, error) {
	var existingID int64
	err := tx.QueryRow(ctx, `
		select id
		from codex_tokens
		where merged_into_token_id is null
		  and owner_user_id = $3
		  and refresh_token = any($1::text[])
		order by case when $2 <> '' and refresh_token = $2 then 0 else 1 end, id desc
		limit 1
	`, postgresTextArray(refreshToken, previousRefreshToken), strings.TrimSpace(previousRefreshToken), ownerUserID).Scan(&existingID)
	if err == nil {
		return existingID, nil
	}
	if err != pgx.ErrNoRows {
		return 0, err
	}
	if !allowEmailFallback {
		return 0, nil
	}

	emailKey := normalizedImportEmail(email)
	if emailKey == "" {
		return 0, nil
	}
	accountKey := importStringValue(accountID)
	err = tx.QueryRow(ctx, `
		select id
		from codex_tokens
		where merged_into_token_id is null
		  and owner_user_id = $1
		  and lower(btrim(email)) = $2
		  and ($3 = '' or account_id is null or btrim(account_id) = '' or btrim(account_id) = $3)
		order by
		  case when is_active = true and disabled_at is null then 0 else 1 end,
		  case when $3 <> '' and btrim(account_id) = $3 then 0 else 1 end,
		  id desc
		limit 1
	`, ownerUserID, emailKey, accountKey).Scan(&existingID)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return existingID, nil
}

func recordAgentIdentity(ctx context.Context, tx pgx.Tx, ownerUserID int64, tokenID int64, credentials agentidentity.Credentials, idToken *string) error {
	if err := credentials.Validate(); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		insert into token_agent_identities(
			token_id, owner_user_id, agent_runtime_id, agent_private_key, task_id,
			chatgpt_user_id, workspace_id, chatgpt_account_is_fedramp, created_at, updated_at
		)
		values ($1, $2, $3, $4, nullif($5, ''), $6, nullif($7, ''), $8, now(), now())
		on conflict (token_id) do update
		set owner_user_id = excluded.owner_user_id,
		    agent_runtime_id = excluded.agent_runtime_id,
		    agent_private_key = excluded.agent_private_key,
		    task_id = excluded.task_id,
		    chatgpt_user_id = excluded.chatgpt_user_id,
		    workspace_id = excluded.workspace_id,
		    chatgpt_account_is_fedramp = excluded.chatgpt_account_is_fedramp,
		    updated_at = now()
	`, tokenID, ownerUserID, credentials.RuntimeID, credentials.PrivateKey, credentials.TaskID, credentials.UserID, credentials.WorkspaceID, credentials.FedRAMP); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		insert into token_secrets(token_id, owner_user_id, access_token, refresh_token, id_token, updated_at)
		values ($1, $2, null, null, $3, now())
		on conflict (token_id) do update
		set owner_user_id = coalesce(excluded.owner_user_id, token_secrets.owner_user_id),
		    access_token = null,
		    refresh_token = null,
		    id_token = coalesce(excluded.id_token, token_secrets.id_token),
		    updated_at = now()
	`, tokenID, ownerUserID, idToken)
	return err
}

func (s *Store) GetAgentIdentityCredentials(ctx context.Context, tokenID int64) (agentidentity.Credentials, error) {
	var credentials agentidentity.Credentials
	var taskID sql.NullString
	var workspaceID sql.NullString
	var accountID sql.NullString
	var email sql.NullString
	var planType sql.NullString
	err := s.pool.QueryRow(ctx, `
		select ai.agent_runtime_id, ai.agent_private_key, ai.task_id, ai.chatgpt_user_id,
		       ai.workspace_id, ai.chatgpt_account_is_fedramp,
		       t.account_id, t.email, t.plan_type
		from token_agent_identities ai
		join codex_tokens t on t.id = ai.token_id
		where ai.token_id = $1
		  and t.merged_into_token_id is null
	`, tokenID).Scan(
		&credentials.RuntimeID,
		&credentials.PrivateKey,
		&taskID,
		&credentials.UserID,
		&workspaceID,
		&credentials.FedRAMP,
		&accountID,
		&email,
		&planType,
	)
	if err != nil {
		return agentidentity.Credentials{}, err
	}
	credentials.TaskID = taskID.String
	credentials.WorkspaceID = workspaceID.String
	credentials.AccountID = accountID.String
	credentials.Email = email.String
	credentials.PlanType = planType.String
	return credentials, nil
}

func (s *Store) UpdateAgentIdentityTask(ctx context.Context, tokenID int64, expectedTaskID string, taskID string) error {
	taskID = strings.TrimSpace(taskID)
	if tokenID <= 0 || taskID == "" {
		return fmt.Errorf("token id and agent identity task id are required")
	}
	tag, err := s.pool.Exec(ctx, `
		update token_agent_identities
		set task_id = $3, updated_at = now()
		where token_id = $1
		  and coalesce(task_id, '') = $2
	`, tokenID, strings.TrimSpace(expectedTaskID), taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrAgentIdentityTaskChanged
	}
	return nil
}

func normalizedImportEmail(value *string) string {
	return strings.ToLower(importStringValue(value))
}

func importStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func recordTokenSecretAndRefreshHistory(ctx context.Context, tx pgx.Tx, ownerUserID int64, tokenID int64, accessToken string, refreshToken string, idToken *string) error {
	if strings.TrimSpace(refreshToken) != "" {
		if _, err := tx.Exec(ctx, `
			insert into token_refresh_history(token_id, owner_user_id, refresh_token_hash, seen_at)
			values ($1, $2, $3, now())
			on conflict (token_id, refresh_token_hash) do nothing
		`, tokenID, ownerUserID, hashString(refreshToken)); err != nil {
			return err
		}
	}
	if strings.TrimSpace(accessToken) == "" && strings.TrimSpace(refreshToken) == "" && idToken == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into token_secrets(token_id, owner_user_id, access_token, refresh_token, id_token, updated_at)
		values ($1, $2, nullif($3, ''), nullif($4, ''), $5, now())
		on conflict (token_id) do update
		set owner_user_id = coalesce(excluded.owner_user_id, token_secrets.owner_user_id),
		    access_token = coalesce(excluded.access_token, token_secrets.access_token),
		    refresh_token = coalesce(excluded.refresh_token, token_secrets.refresh_token),
		    id_token = coalesce(excluded.id_token, token_secrets.id_token),
		    updated_at = now()
	`, tokenID, ownerUserID, accessToken, refreshToken, idToken)
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
		with succeeded as (
			update codex_tokens
			set last_used_at = now(),
			    last_error = null,
			    updated_at = now()
			where id = $1
			  and merged_into_token_id is null
			  and is_active = true
			  and disabled_at is null
			  and (cooldown_until is null or cooldown_until <= now())
			returning id, owner_user_id
		)
		insert into token_runtime_state(
			token_id, owner_user_id, active_streams, cooldown_until, disabled_reason,
			failure_streak, last_success_at, updated_at
		)
		select id, owner_user_id, 0, null, null, 0, now(), now()
		from succeeded
		on conflict (token_id) do update
		set owner_user_id = coalesce(excluded.owner_user_id, token_runtime_state.owner_user_id),
		    cooldown_until = null,
		    disabled_reason = null,
		    failure_streak = 0,
		    last_success_at = now(),
		    updated_at = now()
	`, tokenID)
	return err
}

type ManualProbeStateFence struct {
	IsActive      bool
	DisabledAt    *time.Time
	CooldownUntil *time.Time
	LastError     *string
	Credentials   QuotaRecoveryCredentialFence
}

func (s *Store) MarkManualProbeSuccess(ctx context.Context, tokenID int64, fence ManualProbeStateFence, statusCode int, model string) error {
	return s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var ownerUserID sql.NullInt64
		err := tx.QueryRow(ctx, `
			update codex_tokens
			set is_active = true,
			    cooldown_until = null,
			    disabled_at = null,
			    last_used_at = now(),
			    last_error = null,
			    updated_at = now()
			where id = $1
			  and merged_into_token_id is null
			  and is_active = $2
			  and disabled_at is not distinct from $3::timestamptz
			  and cooldown_until is not distinct from $4::timestamptz
			  and last_error is not distinct from $5::text
			  and coalesce(access_token, '') = $6
			  and coalesce(refresh_token, '') = $7
			  and btrim(coalesce(account_id, '')) = $8
			returning owner_user_id
		`, tokenID, fence.IsActive, fence.DisabledAt, fence.CooldownUntil,
			fence.LastError, fence.Credentials.AccessToken, fence.Credentials.RefreshToken, strings.TrimSpace(fence.Credentials.AccountID)).Scan(&ownerUserID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenStateChanged
		}
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
			insert into token_runtime_state(
				token_id, owner_user_id, active_streams, cooldown_until, disabled_reason,
				failure_streak, last_success_at, updated_at
			)
			values ($1, $2, 0, null, null, 0, now(), now())
			on conflict (token_id) do update
			set owner_user_id = coalesce(excluded.owner_user_id, token_runtime_state.owner_user_id),
			    cooldown_until = null,
			    disabled_reason = null,
			    failure_streak = 0,
			    last_success_at = now(),
			    updated_at = now()
		`, tokenID, nullableOwnerID(ownerUserID)); err != nil {
			return err
		}
		eventType := "manual_probe_succeeded"
		reason := "manual probe confirmed token is usable"
		if !fence.IsActive || fence.DisabledAt != nil {
			eventType = "reactivated_by_manual_probe"
			reason = "manual probe reactivated token"
		}
		_, err = tx.Exec(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, model, status_code,
				previous_is_active, next_is_active, metadata
			)
			values ($1, $2, $3, $4, nullif($5, ''), nullif($6, 0), $7, true, $8)
		`, tokenID, nullableOwnerID(ownerUserID), eventType, reason, strings.TrimSpace(model), statusCode, fence.IsActive, jsonBytes(map[string]any{
			"source":           "manual_probe",
			"cleared_cooldown": fence.CooldownUntil != nil,
			"cleared_error":    fence.LastError != nil,
		}))
		return err
	})
}

func (s *Store) MarkManualProbeUsageLimit(ctx context.Context, tokenID int64, fence ManualProbeStateFence, message string, cooldownUntil time.Time, model string) error {
	message = truncate(strings.TrimSpace(message), 4000)
	if message == "" {
		message = "usage_limit_reached"
	} else if !strings.Contains(strings.ToLower(message), "usage_limit_reached") {
		message = truncate("usage_limit_reached: "+message, 4000)
	}
	return s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var ownerUserID sql.NullInt64
		err := tx.QueryRow(ctx, `
			update codex_tokens
			set last_error = $9,
			    cooldown_until = $10,
			    updated_at = now()
			where id = $1
			  and merged_into_token_id is null
			  and is_active = $2
			  and disabled_at is not distinct from $3::timestamptz
			  and cooldown_until is not distinct from $4::timestamptz
			  and last_error is not distinct from $5::text
			  and coalesce(access_token, '') = $6
			  and coalesce(refresh_token, '') = $7
			  and btrim(coalesce(account_id, '')) = $8
			returning owner_user_id
		`, tokenID, fence.IsActive, fence.DisabledAt, fence.CooldownUntil,
			fence.LastError, fence.Credentials.AccessToken, fence.Credentials.RefreshToken, strings.TrimSpace(fence.Credentials.AccountID),
			truncate(message, 4000), cooldownUntil.UTC()).Scan(&ownerUserID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenStateChanged
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into token_runtime_state(
				token_id, owner_user_id, cooldown_until, disabled_reason,
				failure_streak, last_failure_at, updated_at
			)
			values ($1, $2, $3, $4, 1, now(), now())
			on conflict (token_id) do update
			set owner_user_id = coalesce(excluded.owner_user_id, token_runtime_state.owner_user_id),
			    cooldown_until = excluded.cooldown_until,
			    disabled_reason = excluded.disabled_reason,
			    failure_streak = token_runtime_state.failure_streak + 1,
			    last_failure_at = now(),
			    updated_at = now()
		`, tokenID, nullableOwnerID(ownerUserID), cooldownUntil.UTC(), truncate(message, 4000)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, cooldown_until,
				model, status_code, previous_is_active, next_is_active, metadata
			)
			values ($1, $2, $3, $4, $5, nullif($6, ''), 429, $7, $7, '{"failure_kind":"usage_limit_reached"}'::jsonb)
		`, tokenID, nullableOwnerID(ownerUserID), TokenUsageLimitConfirmedEvent, message, cooldownUntil.UTC(), strings.TrimSpace(model), fence.IsActive)
		return err
	})
}

func (s *Store) MarkManualProbeDisabled(ctx context.Context, tokenID int64, fence ManualProbeStateFence, message string, clearAccess bool, statusCode int, model string) error {
	message = truncate(strings.TrimSpace(message), 4000)
	if message == "" {
		message = "manual probe confirmed token is disabled"
	}
	return s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var ownerUserID sql.NullInt64
		var cooldownUntil sql.NullTime
		err := tx.QueryRow(ctx, `
			update codex_tokens
			set last_error = $9,
			    is_active = false,
			    disabled_at = now(),
			    access_token = case when $10 then null else access_token end,
			    updated_at = now()
			where id = $1
			  and merged_into_token_id is null
			  and is_active = $2
			  and disabled_at is not distinct from $3::timestamptz
			  and cooldown_until is not distinct from $4::timestamptz
			  and last_error is not distinct from $5::text
			  and coalesce(access_token, '') = $6
			  and coalesce(refresh_token, '') = $7
			  and btrim(coalesce(account_id, '')) = $8
			returning owner_user_id, cooldown_until
		`, tokenID, fence.IsActive, fence.DisabledAt, fence.CooldownUntil,
			fence.LastError, fence.Credentials.AccessToken, fence.Credentials.RefreshToken, strings.TrimSpace(fence.Credentials.AccountID),
			message, clearAccess).Scan(&ownerUserID, &cooldownUntil)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTokenStateChanged
		}
		if err != nil {
			return err
		}
		var cooldownValue any
		if cooldownUntil.Valid {
			cooldownValue = cooldownUntil.Time.UTC()
		}
		if clearAccess {
			if _, err := tx.Exec(ctx, `
				update token_secrets
				set access_token = null, updated_at = now()
				where token_id = $1
			`, tokenID); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			insert into token_runtime_state(
				token_id, owner_user_id, cooldown_until, disabled_reason,
				failure_streak, last_failure_at, updated_at
			)
			values ($1, $2, $3, $4, 1, now(), now())
			on conflict (token_id) do update
			set owner_user_id = coalesce(excluded.owner_user_id, token_runtime_state.owner_user_id),
			    cooldown_until = excluded.cooldown_until,
			    disabled_reason = excluded.disabled_reason,
			    failure_streak = token_runtime_state.failure_streak + 1,
			    last_failure_at = now(),
			    updated_at = now()
		`, tokenID, nullableOwnerID(ownerUserID), cooldownValue, message); err != nil {
			return err
		}
		metadata := map[string]any{
			"source":                    "manual_probe",
			"credential_access_cleared": clearAccess,
		}
		_, err = tx.Exec(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, cooldown_until,
				model, status_code, previous_is_active, next_is_active, metadata
			)
			values ($1, $2, 'disabled', $3, $4, nullif($5, ''), nullif($6, 0), $7, false, $8)
		`, tokenID, nullableOwnerID(ownerUserID), message, cooldownValue, strings.TrimSpace(model), statusCode, fence.IsActive, jsonBytes(metadata))
		return err
	})
}

func (s *Store) MarkTokenError(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time) error {
	return s.MarkTokenErrorWithContext(ctx, tokenID, message, deactivate, cooldownUntil, TokenStateEventContext{})
}

func (s *Store) MarkTokenErrorWithContext(ctx context.Context, tokenID int64, message string, deactivate bool, cooldownUntil *time.Time, eventCtx TokenStateEventContext) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var ownerUserID sql.NullInt64
	var previousActive bool
	if err := tx.QueryRow(ctx, `
		select owner_user_id, is_active
		from codex_tokens
		where id = $1
		for update
	`, tokenID).Scan(&ownerUserID, &previousActive); err != nil {
		return err
	}
	disabledAtExpr := "disabled_at"
	isActiveExpr := "is_active"
	nextActive := previousActive
	if deactivate {
		disabledAtExpr = "now()"
		isActiveExpr = "false"
		nextActive = false
	}
	if eventCtx.PreviousIsActive == nil {
		eventCtx.PreviousIsActive = &previousActive
	}
	if eventCtx.NextIsActive == nil {
		eventCtx.NextIsActive = &nextActive
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
	var ownerParam any
	if ownerUserID.Valid {
		ownerParam = ownerUserID.Int64
	}
	_, err = tx.Exec(ctx, `
		insert into token_state_events(
			token_id, owner_user_id, event_type, reason, cooldown_until, request_id,
			gateway_request_log_id, gateway_request_attempt_id, endpoint, model, status_code,
			selection_mode, caller_owner_user_id, previous_is_active, next_is_active, metadata
		)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	`, tokenID, ownerParam, map[bool]string{true: "disabled", false: "error"}[deactivate], truncate(message, 4000), cooldownUntil,
		nullableString(eventCtx.RequestID), eventCtx.GatewayRequestLogID, eventCtx.GatewayRequestAttemptID,
		nullableString(eventCtx.Endpoint), nullableString(eventCtx.Model), eventCtx.StatusCode,
		nullableString(eventCtx.SelectionMode), eventCtx.CallerOwnerUserID,
		eventCtx.PreviousIsActive, eventCtx.NextIsActive, jsonBytes(eventCtx.Metadata))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) SetTokenActive(ctx context.Context, tokenID int64, active bool, clearCooldown bool) (*Token, error) {
	return s.SetTokenActiveScoped(ctx, AllResources(), tokenID, active, clearCooldown)
}

func (s *Store) SetTokenActiveScoped(ctx context.Context, scope ResourceScope, tokenID int64, active bool, clearCooldown bool) (*Token, error) {
	args := []any{tokenID, active, clearCooldown || !active}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set is_active = $2,
		    disabled_at = case when $2 then null else now() end,
		    cooldown_until = case when $3 then null else cooldown_until end,
		    updated_at = now()
		where id = $1
		  and `+ownerWhere+`
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at,
		          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		          marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		          last_used_at, last_error, created_at, updated_at
	`, args...)
	token, err := scanTokenWithSharing(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) GetToken(ctx context.Context, tokenID int64) (*Token, error) {
	return s.GetTokenScoped(ctx, AllResources(), tokenID)
}

func (s *Store) GetTokenScoped(ctx context.Context, scope ResourceScope, tokenID int64) (*Token, error) {
	args := []any{tokenID}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	row := s.pool.QueryRow(ctx, `
		select id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		       is_active, cooldown_until, disabled_at,
		       share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		       marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		       last_used_at, last_error, created_at, updated_at
		from codex_tokens
		where id = $1 and merged_into_token_id is null
		  and `+ownerWhere, args...)
	token, err := scanTokenWithSharing(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) TokenOAuthClientID(ctx context.Context, tokenID int64) (string, error) {
	return s.TokenOAuthClientIDScoped(ctx, AllResources(), tokenID)
}

func (s *Store) TokenOAuthClientIDScoped(ctx context.Context, scope ResourceScope, tokenID int64) (string, error) {
	args := []any{tokenID}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	var value sql.NullString
	err := s.pool.QueryRow(ctx, `
		select coalesce(raw_payload->>'client_id', raw_payload->>'clientId', raw_payload->>'oauth_client_id', '')
		from codex_tokens
		where id = $1 and merged_into_token_id is null
		  and `+ownerWhere, args...).Scan(&value)
	if err != nil {
		return "", err
	}
	if value.Valid {
		return strings.TrimSpace(value.String), nil
	}
	return "", nil
}

func (s *Store) UpdateTokenRemark(ctx context.Context, tokenID int64, remark string) (*Token, error) {
	return s.UpdateTokenRemarkScoped(ctx, AllResources(), tokenID, remark)
}

func (s *Store) UpdateTokenRemarkScoped(ctx context.Context, scope ResourceScope, tokenID int64, remark string) (*Token, error) {
	args := []any{tokenID, truncate(remark, 2000)}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set remark = nullif($2, ''), updated_at = now()
		where id = $1 and merged_into_token_id is null
		  and `+ownerWhere+`
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at,
		          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		          marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		          last_used_at, last_error, created_at, updated_at
	`, args...)
	token, err := scanTokenWithSharing(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) SetTokenSharingScoped(ctx context.Context, scope ResourceScope, tokenID int64, enabled bool, status string, reason string) (*Token, error) {
	args := []any{tokenID, enabled, normalizeShareStatus(enabled, status), truncate(reason, 1000)}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set share_enabled = $2,
		    share_status = $3,
		    share_disabled_reason = case when $2 then null else nullif($4, '') end,
		    share_enabled_at = case when $2 then coalesce(share_enabled_at, now()) else share_enabled_at end,
		    share_disabled_at = case when $2 then null else now() end,
		    updated_at = now()
		where id = $1 and merged_into_token_id is null
		  and `+ownerWhere+`
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at,
		          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		          marketplace_price_bps, marketplace_price_updated_at, marketplace_price_source,
		          last_used_at, last_error, created_at, updated_at
	`, args...)
	token, err := scanTokenWithSharing(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) SetTokensSharingScoped(ctx context.Context, scope ResourceScope, tokenIDs []int64, all bool, enabled bool, status string, reason string) (int64, error) {
	ids := postgresIntIDs(tokenIDs)
	if !all && len(ids) == 0 {
		return 0, nil
	}
	args := []any{enabled, normalizeShareStatus(enabled, status), truncate(reason, 1000)}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	idWhere := ""
	if !all {
		args = append(args, ids)
		idWhere = fmt.Sprintf(" and id = any($%d::integer[])", len(args))
	}
	tag, err := s.pool.Exec(ctx, `
		update codex_tokens
		set share_enabled = $1,
		    share_status = $2,
		    share_disabled_reason = case when $1 then null else nullif($3, '') end,
		    share_enabled_at = case when $1 then coalesce(share_enabled_at, now()) else share_enabled_at end,
		    share_disabled_at = case when $1 then null else now() end,
		    updated_at = now()
		where merged_into_token_id is null
		  and `+ownerWhere+idWhere, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) SetTokensMarketplacePriceScoped(ctx context.Context, scope ResourceScope, tokenIDs []int64, all bool, priceBPS int, source string) (int64, error) {
	if priceBPS < 0 || priceBPS > MaxMarketplacePriceBPS {
		return 0, fmt.Errorf("marketplace price bps must be between 0 and %d", MaxMarketplacePriceBPS)
	}
	if priceBPS%MarketplacePriceStepBPS != 0 {
		return 0, fmt.Errorf("marketplace price bps must use %d bps increments", MarketplacePriceStepBPS)
	}
	ids := postgresIntIDs(tokenIDs)
	if !all && len(ids) == 0 {
		return 0, nil
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = "owner_default"
	}
	source = truncate(source, 64)
	args := []any{priceBPS, source}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	idWhere := ""
	if !all {
		args = append(args, ids)
		idWhere = fmt.Sprintf(" and id = any($%d::integer[])", len(args))
	}
	tag, err := s.pool.Exec(ctx, `
		update codex_tokens
		set marketplace_price_bps = $1,
		    marketplace_price_source = $2,
		    marketplace_price_updated_at = now(),
		    updated_at = now()
		where merged_into_token_id is null
		  and `+ownerWhere+idWhere, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) UpdateTokenPlanTypes(ctx context.Context, planByTokenID map[int64]string) error {
	if len(planByTokenID) == 0 {
		return nil
	}
	values := make([]string, 0, len(planByTokenID))
	args := make([]any, 0, len(planByTokenID)*2)
	for tokenID, plan := range planByTokenID {
		plan = normalizePlanFilter(plan)
		if tokenID <= 0 || plan == "" || len(plan) > 32 {
			continue
		}
		args = append(args, tokenID, plan)
		base := len(args) - 1
		values = append(values, fmt.Sprintf("($%d::integer, $%d::varchar(32))", base, base+1))
	}
	if len(values) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		update codex_tokens as token
		set plan_type = incoming.plan_type,
		    updated_at = now()
		from (values `+strings.Join(values, ",")+`) as incoming(id, plan_type)
		where token.id = incoming.id
		  and token.merged_into_token_id is null
		  and coalesce(lower(btrim(token.plan_type)), '') <> incoming.plan_type
	`, args...)
	return err
}

func (s *Store) DeleteToken(ctx context.Context, tokenID int64) error {
	return s.DeleteTokenScoped(ctx, AllResources(), tokenID)
}

func (s *Store) DeleteTokenScoped(ctx context.Context, scope ResourceScope, tokenID int64) error {
	args := []any{tokenID}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	tag, err := s.pool.Exec(ctx, `delete from codex_tokens where (id = $1 or merged_into_token_id = $1) and `+ownerWhere, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) DeleteDisabledTokensScoped(ctx context.Context, scope ResourceScope) ([]int64, error) {
	args := []any{}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		with roots as (
			select id
			from codex_tokens
			where merged_into_token_id is null
			  and (is_active = false or disabled_at is not null)
			  and `+ownerWhere+`
		),
		deleted as (
			delete from codex_tokens
			where (id in (select id from roots) or merged_into_token_id in (select id from roots))
			  and `+ownerWhere+`
			returning id
		)
		select id
		from deleted
		order by id
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
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
	tag, err := tx.Exec(ctx, `
		update codex_tokens
		set access_token = coalesce(nullif($2, ''), access_token),
		    refresh_token = coalesce(nullif($3, ''), refresh_token),
		    id_token = coalesce(nullif($4, ''), id_token),
		    expired = coalesce($5, expired),
		    account_id = coalesce(nullif($6, ''), account_id),
		    email = coalesce(nullif($7, ''), email),
		    plan_type = coalesce(nullif($8, ''), plan_type),
		    last_refresh = now(),
		    last_error = case when $9 then last_error else null end,
		    is_active = case when $9 then is_active else true end,
		    disabled_at = case when $9 then disabled_at else null end,
		    updated_at = now()
		where id = $1
		  and merged_into_token_id is null
		  and (
		    not $10::boolean
		    or (
		      coalesce(access_token, '') = $11
		      and coalesce(refresh_token, '') = $12
		    )
		  )
	`, update.TokenID, update.AccessToken, update.RefreshToken, update.IDToken, update.ExpiresAt, update.AccountID, update.Email, update.PlanType, update.PreserveActivation,
		update.RequireCredentialMatch, update.ExpectedAccessToken, update.ExpectedRefreshToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		if update.RequireCredentialMatch {
			return ErrTokenCredentialsChanged
		}
		return pgx.ErrNoRows
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

func (s *Store) ClearTokenAccess(ctx context.Context, tokenID int64) error {
	if tokenID <= 0 {
		return fmt.Errorf("token id is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		update codex_tokens
		set access_token = null, updated_at = now()
		where id = $1 and merged_into_token_id is null
	`, tokenID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		update token_secrets
		set access_token = null, updated_at = now()
		where token_id = $1
	`, tokenID); err != nil {
		return err
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

func scanAvailableTokens(rows pgx.Rows) ([]Token, error) {
	var tokens []Token
	for rows.Next() {
		var token Token
		var accessToken sql.NullString
		var refreshToken sql.NullString
		var shareStatus sql.NullString
		var marketplacePriceBPS sql.NullInt64
		var marketplacePriceSource sql.NullString
		err := rows.Scan(
			&token.ID,
			&token.OwnerUserID,
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
			&token.ShareEnabled,
			&shareStatus,
			&token.ShareReason,
			&token.ShareEnabledAt,
			&token.ShareDisabledAt,
			&marketplacePriceBPS,
			&token.MarketplacePriceUpdatedAt,
			&marketplacePriceSource,
			&token.LastUsedAt,
			&token.LastError,
			&token.CreatedAt,
			&token.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		if accessToken.Valid {
			token.AccessToken = accessToken.String
		}
		if refreshToken.Valid {
			token.RefreshToken = refreshToken.String
		}
		if shareStatus.Valid {
			token.ShareStatus = shareStatus.String
		}
		if marketplacePriceBPS.Valid {
			value := int(marketplacePriceBPS.Int64)
			token.MarketplacePriceBPS = &value
		}
		if marketplacePriceSource.Valid {
			token.MarketplacePriceSource = marketplacePriceSource.String
		}
		tokens = append(tokens, token)
	}
	return tokens, rows.Err()
}

func scanRuntimeTokens(rows pgx.Rows) ([]Token, error) {
	var tokens []Token
	for rows.Next() {
		var token Token
		var accessToken sql.NullString
		var refreshToken sql.NullString
		var shareStatus sql.NullString
		var marketplacePriceBPS sql.NullInt64
		var marketplacePriceSource sql.NullString
		var runtimeID sql.NullString
		var privateKey sql.NullString
		var taskID sql.NullString
		var userID sql.NullString
		var workspaceID sql.NullString
		var fedRAMP sql.NullBool
		err := rows.Scan(
			&token.ID,
			&token.OwnerUserID,
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
			&token.ShareEnabled,
			&shareStatus,
			&token.ShareReason,
			&token.ShareEnabledAt,
			&token.ShareDisabledAt,
			&marketplacePriceBPS,
			&token.MarketplacePriceUpdatedAt,
			&marketplacePriceSource,
			&token.LastUsedAt,
			&token.LastError,
			&token.CreatedAt,
			&token.UpdatedAt,
			&runtimeID,
			&privateKey,
			&taskID,
			&userID,
			&workspaceID,
			&fedRAMP,
		)
		if err != nil {
			return nil, err
		}
		if accessToken.Valid {
			token.AccessToken = accessToken.String
		}
		if refreshToken.Valid {
			token.RefreshToken = refreshToken.String
		}
		if shareStatus.Valid {
			token.ShareStatus = shareStatus.String
		}
		if marketplacePriceBPS.Valid {
			value := int(marketplacePriceBPS.Int64)
			token.MarketplacePriceBPS = &value
		}
		if marketplacePriceSource.Valid {
			token.MarketplacePriceSource = marketplacePriceSource.String
		}
		if runtimeID.Valid && privateKey.Valid {
			credentials := agentidentity.Credentials{
				RuntimeID:   runtimeID.String,
				PrivateKey:  privateKey.String,
				TaskID:      taskID.String,
				UserID:      userID.String,
				WorkspaceID: workspaceID.String,
				FedRAMP:     fedRAMP.Valid && fedRAMP.Bool,
			}
			if token.AccountID != nil {
				credentials.AccountID = *token.AccountID
			}
			if token.Email != nil {
				credentials.Email = *token.Email
			}
			if token.PlanType != nil {
				credentials.PlanType = *token.PlanType
			}
			token.AgentIdentity = &credentials
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
		&token.OwnerUserID,
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

func scanTokenWithSharing(row rowScanner) (Token, error) {
	var token Token
	var accessToken sql.NullString
	var refreshToken sql.NullString
	var shareStatus sql.NullString
	var marketplacePriceBPS sql.NullInt64
	var marketplacePriceSource sql.NullString
	err := row.Scan(
		&token.ID,
		&token.OwnerUserID,
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
		&token.ShareEnabled,
		&shareStatus,
		&token.ShareReason,
		&token.ShareEnabledAt,
		&token.ShareDisabledAt,
		&marketplacePriceBPS,
		&token.MarketplacePriceUpdatedAt,
		&marketplacePriceSource,
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
	if shareStatus.Valid {
		token.ShareStatus = shareStatus.String
	}
	if marketplacePriceBPS.Valid {
		value := int(marketplacePriceBPS.Int64)
		token.MarketplacePriceBPS = &value
	}
	if marketplacePriceSource.Valid {
		token.MarketplacePriceSource = marketplacePriceSource.String
	}
	return token, err
}

func normalizeShareStatus(enabled bool, status string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	if enabled {
		switch status {
		case "active", "enabled", "approved":
			return "active"
		default:
			return "active"
		}
	}
	if status == "" || status == "active" || status == "enabled" || status == "approved" {
		return "private"
	}
	return truncate(status, 32)
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

func importSharingFromPayload(payload map[string]any) (bool, string, bool) {
	enabled, ok := payloadBoolExplicit(payload["_share_enabled"])
	if !ok {
		enabled, ok = payloadBoolExplicit(payload["share_enabled"])
	}
	if !ok {
		enabled, ok = payloadBoolExplicit(payload["shareEnabled"])
	}
	status := stringFromPayload(payload, "_share_status", "share_status", "shareStatus")
	if !ok {
		return false, normalizeShareStatus(false, ""), false
	}
	return enabled, normalizeShareStatus(enabled, status), true
}

func payloadBoolExplicit(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on", "active", "enabled":
			return true, true
		case "0", "false", "no", "off", "inactive", "disabled", "private":
			return false, true
		}
	case float64:
		return typed != 0, true
	case int:
		return typed != 0, true
	case int64:
		return typed != 0, true
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return parsed != 0, true
		}
	}
	return false, false
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
