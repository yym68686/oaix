package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const sub2APIUsageSyncMinInterval = 5 * time.Minute

type Sub2APIUsageSyncMapping struct {
	TargetID              int64
	TokenID               int64
	RemoteAccountID       int64
	MappingCreatedAt      time.Time
	HistoricalThroughDate *time.Time
}

type Sub2APIUsageSnapshotInput struct {
	TokenID          int64
	RemoteAccountID  int64
	AccountCostUSD   float64
	StandardCostUSD  float64
	UserCostUSD      float64
	TotalRequests    int64
	TotalTokens      int64
	SourceComputedAt *time.Time
	ThroughDate      *time.Time
}

type Sub2APIDailyUsageSnapshotInput struct {
	TokenID          int64
	RemoteAccountID  int64
	UsageDate        time.Time
	AccountCostUSD   float64
	StandardCostUSD  float64
	UserCostUSD      float64
	TotalRequests    int64
	TotalTokens      int64
	SourceComputedAt *time.Time
	Finalized        bool
}

type Sub2APIDailyUsageFinalization struct {
	Sub2APIUsageSyncMapping
	UsageDate time.Time
}

type Sub2APIUsageCost struct {
	AccountCostUSD  float64
	StandardCostUSD float64
	UserCostUSD     float64
	TotalRequests   int64
	TotalTokens     int64
	SyncedAt        *time.Time
	Stale           bool
}

func (s *Store) ListSub2APIUsageBaselineMappings(ctx context.Context, target Sub2APISyncTarget, limit int) ([]Sub2APIUsageSyncMapping, error) {
	if target.ID <= 0 || !target.Enabled {
		return nil, nil
	}
	if limit <= 0 || limit > 64 {
		limit = 64
	}
	interval := time.Duration(target.CheckIntervalSeconds) * time.Second
	if interval < sub2APIUsageSyncMinInterval {
		interval = sub2APIUsageSyncMinInterval
	}
	intervalSeconds := int(interval / time.Second)
	rows, err := s.pool.Query(ctx, `
		select m.target_id, m.token_id, coalesce(m.remote_account_id, 0), m.created_at, usage.through_date
		from sub2api_sync_mappings m
		join codex_tokens token on token.id = m.token_id
		left join sub2api_usage_snapshots usage
		  on usage.target_id = m.target_id
		 and usage.remote_account_id = m.remote_account_id
		where m.target_id = $1
		  and m.status = 'synced'
		  and coalesce(m.remote_account_id, 0) > 0
		  and token.merged_into_token_id is null
		  and token.owner_user_id = $2
		  and usage.through_date is null
		  and (usage.status is null or usage.status = 'synced' or usage.updated_at < now() - make_interval(secs => $3))
		order by (usage.status = 'failed') asc nulls first, m.created_at desc, m.token_id desc
		limit $4
	`, target.ID, target.OwnerUserID, intervalSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Sub2APIUsageSyncMapping, 0, limit)
	for rows.Next() {
		var item Sub2APIUsageSyncMapping
		if err := rows.Scan(&item.TargetID, &item.TokenID, &item.RemoteAccountID, &item.MappingCreatedAt, &item.HistoricalThroughDate); err != nil {
			return nil, err
		}
		if item.TokenID > 0 && item.RemoteAccountID > 0 {
			items = append(items, item)
		}
	}
	return items, rows.Err()
}

func (s *Store) ListDueSub2APITodayUsageSyncMappings(ctx context.Context, target Sub2APISyncTarget, usageDate time.Time, limit int) ([]Sub2APIUsageSyncMapping, error) {
	if target.ID <= 0 || !target.Enabled || usageDate.IsZero() {
		return nil, nil
	}
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	interval := time.Duration(target.CheckIntervalSeconds) * time.Second
	if interval < sub2APIUsageSyncMinInterval {
		interval = sub2APIUsageSyncMinInterval
	}
	intervalSeconds := int(interval / time.Second)
	rows, err := s.pool.Query(ctx, `
		select m.target_id, m.token_id, coalesce(m.remote_account_id, 0), m.created_at, baseline.through_date
		from sub2api_sync_mappings m
		join codex_tokens token on token.id = m.token_id
		left join sub2api_usage_snapshots baseline
		  on baseline.target_id = m.target_id
		 and baseline.remote_account_id = m.remote_account_id
		left join sub2api_usage_daily_snapshots daily
		  on daily.target_id = m.target_id
		 and daily.remote_account_id = m.remote_account_id
		 and daily.usage_date = $3::date
		where m.target_id = $1
		  and m.status = 'synced'
		  and coalesce(m.remote_account_id, 0) > 0
		  and token.merged_into_token_id is null
		  and token.owner_user_id = $2
		  and (
			daily.remote_account_id is null
			or (daily.status = 'synced' and daily.synced_at < now() - make_interval(secs => $4))
			or (daily.status <> 'synced' and daily.updated_at < now() - make_interval(secs => $4))
		  )
		order by daily.synced_at asc nulls first, m.created_at desc, m.token_id desc
		limit $5
	`, target.ID, target.OwnerUserID, usageDate, intervalSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Sub2APIUsageSyncMapping, 0, limit)
	for rows.Next() {
		var item Sub2APIUsageSyncMapping
		if err := rows.Scan(&item.TargetID, &item.TokenID, &item.RemoteAccountID, &item.MappingCreatedAt, &item.HistoricalThroughDate); err != nil {
			return nil, err
		}
		if item.TokenID > 0 && item.RemoteAccountID > 0 {
			items = append(items, item)
		}
	}
	return items, rows.Err()
}

func (s *Store) ListPendingSub2APIDailyUsageFinalizations(ctx context.Context, target Sub2APISyncTarget, beforeDate time.Time, limit int) ([]Sub2APIDailyUsageFinalization, error) {
	if target.ID <= 0 || !target.Enabled || beforeDate.IsZero() {
		return nil, nil
	}
	if limit <= 0 || limit > 32 {
		limit = 32
	}
	interval := time.Duration(target.CheckIntervalSeconds) * time.Second
	if interval < sub2APIUsageSyncMinInterval {
		interval = sub2APIUsageSyncMinInterval
	}
	intervalSeconds := int(interval / time.Second)
	rows, err := s.pool.Query(ctx, `
		select m.target_id, m.token_id, m.remote_account_id, m.created_at, baseline.through_date, due.usage_date::date
		from sub2api_sync_mappings m
		join codex_tokens token on token.id = m.token_id
		join sub2api_usage_snapshots baseline
		  on baseline.target_id = m.target_id
		 and baseline.remote_account_id = m.remote_account_id
		cross join lateral generate_series(
			baseline.through_date + 1,
			$3::date - 1,
			interval '1 day'
		) due(usage_date)
		left join sub2api_usage_daily_snapshots daily
		  on daily.target_id = m.target_id
		 and daily.remote_account_id = m.remote_account_id
		 and daily.usage_date = due.usage_date::date
		where m.target_id = $1
		  and baseline.through_date is not null
		  and daily.finalized_at is null
		  and m.status = 'synced'
		  and token.merged_into_token_id is null
		  and token.owner_user_id = $2
		  and (daily.error_message is null or daily.updated_at < now() - make_interval(secs => $4))
		order by (daily.remote_account_id is null) desc, (daily.error_message is not null) asc,
		         due.usage_date asc, daily.updated_at asc, m.token_id asc
		limit $5
	`, target.ID, target.OwnerUserID, beforeDate, intervalSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Sub2APIDailyUsageFinalization, 0, limit)
	for rows.Next() {
		var item Sub2APIDailyUsageFinalization
		if err := rows.Scan(&item.TargetID, &item.TokenID, &item.RemoteAccountID, &item.MappingCreatedAt, &item.HistoricalThroughDate, &item.UsageDate); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SaveSub2APIUsageSnapshots(ctx context.Context, targetID int64, items []Sub2APIUsageSnapshotInput) error {
	if targetID <= 0 || len(items) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, item := range items {
		if item.TokenID <= 0 || item.RemoteAccountID <= 0 {
			continue
		}
		batch.Queue(`
			insert into sub2api_usage_snapshots(
				target_id, remote_account_id, token_id,
				account_cost_usd, standard_cost_usd, user_cost_usd,
				total_requests, total_tokens, source_computed_at, through_date,
				synced_at, status, error_message, created_at, updated_at
			)
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), 'synced', null, now(), now())
			on conflict(target_id, remote_account_id) do update
			set token_id = excluded.token_id,
			    account_cost_usd = excluded.account_cost_usd,
			    standard_cost_usd = excluded.standard_cost_usd,
			    user_cost_usd = excluded.user_cost_usd,
			    total_requests = excluded.total_requests,
			    total_tokens = excluded.total_tokens,
			    source_computed_at = excluded.source_computed_at,
			    through_date = excluded.through_date,
			    synced_at = excluded.synced_at,
			    status = 'synced',
			    error_message = null,
			    updated_at = now()
		`, targetID, item.RemoteAccountID, item.TokenID, item.AccountCostUSD, item.StandardCostUSD, item.UserCostUSD, item.TotalRequests, item.TotalTokens, item.SourceComputedAt, item.ThroughDate)
	}
	if batch.Len() == 0 {
		return nil
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SaveSub2APIDailyUsageSnapshots(ctx context.Context, targetID int64, items []Sub2APIDailyUsageSnapshotInput) error {
	if targetID <= 0 || len(items) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, item := range items {
		if item.TokenID <= 0 || item.RemoteAccountID <= 0 || item.UsageDate.IsZero() {
			continue
		}
		batch.Queue(`
			insert into sub2api_usage_daily_snapshots(
				target_id, remote_account_id, token_id, usage_date,
				account_cost_usd, standard_cost_usd, user_cost_usd,
				total_requests, total_tokens, source_computed_at,
				synced_at, finalized_at, status, error_message, created_at, updated_at
			)
			values(
				$1, $2, $3, $4::date,
				$5, $6, $7, $8, $9, $10,
				now(), case when $11::boolean then now() else null end, 'synced', null, now(), now()
			)
			on conflict(target_id, remote_account_id, usage_date) do update
			set token_id = excluded.token_id,
			    account_cost_usd = excluded.account_cost_usd,
			    standard_cost_usd = excluded.standard_cost_usd,
			    user_cost_usd = excluded.user_cost_usd,
			    total_requests = excluded.total_requests,
			    total_tokens = excluded.total_tokens,
			    source_computed_at = excluded.source_computed_at,
			    synced_at = excluded.synced_at,
			    finalized_at = case when $11::boolean then excluded.finalized_at else sub2api_usage_daily_snapshots.finalized_at end,
			    status = 'synced',
			    error_message = null,
			    updated_at = now()
		`, targetID, item.RemoteAccountID, item.TokenID, item.UsageDate, item.AccountCostUSD, item.StandardCostUSD, item.UserCostUSD, item.TotalRequests, item.TotalTokens, item.SourceComputedAt, item.Finalized)
	}
	if batch.Len() == 0 {
		return nil
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkSub2APIDailyUsageSyncError(ctx context.Context, targetID int64, usageDate time.Time, mappings []Sub2APIUsageSyncMapping, message string) error {
	if targetID <= 0 || usageDate.IsZero() || len(mappings) == 0 {
		return nil
	}
	message = truncate(strings.TrimSpace(message), 2000)
	batch := &pgx.Batch{}
	for _, mapping := range mappings {
		if mapping.TokenID <= 0 || mapping.RemoteAccountID <= 0 {
			continue
		}
		batch.Queue(`
			insert into sub2api_usage_daily_snapshots(
				target_id, remote_account_id, token_id, usage_date,
				status, error_message, created_at, updated_at
			)
			values($1, $2, $3, $4::date, 'failed', nullif($5, ''), now(), now())
			on conflict(target_id, remote_account_id, usage_date) do update
			set token_id = excluded.token_id,
			    error_message = excluded.error_message,
			    updated_at = now()
		`, targetID, mapping.RemoteAccountID, mapping.TokenID, usageDate, message)
	}
	if batch.Len() == 0 {
		return nil
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkSub2APIUsageSyncError(ctx context.Context, targetID int64, mappings []Sub2APIUsageSyncMapping, message string) error {
	if targetID <= 0 || len(mappings) == 0 {
		return nil
	}
	message = truncate(strings.TrimSpace(message), 2000)
	batch := &pgx.Batch{}
	for _, mapping := range mappings {
		if mapping.TokenID <= 0 || mapping.RemoteAccountID <= 0 {
			continue
		}
		batch.Queue(`
			insert into sub2api_usage_snapshots(
				target_id, remote_account_id, token_id, status, error_message, created_at, updated_at
			)
			values($1, $2, $3, 'failed', nullif($4, ''), now(), now())
			on conflict(target_id, remote_account_id) do update
			set token_id = excluded.token_id,
			    status = 'failed',
			    error_message = excluded.error_message,
			    updated_at = now()
		`, targetID, mapping.RemoteAccountID, mapping.TokenID, message)
	}
	if batch.Len() == 0 {
		return nil
	}
	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()
	for range batch.Len() {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Sub2APIUsageByTokens(ctx context.Context, tokens []Token) (map[int64]Sub2APIUsageCost, error) {
	result := make(map[int64]Sub2APIUsageCost, len(tokens))
	requestedIDs := make([]int64, 0, len(tokens))
	requested := make(map[int64]struct{}, len(tokens))
	for _, token := range tokens {
		if token.ID <= 0 {
			continue
		}
		if _, exists := requested[token.ID]; exists {
			continue
		}
		requested[token.ID] = struct{}{}
		requestedIDs = append(requestedIDs, token.ID)
		result[token.ID] = Sub2APIUsageCost{}
	}
	if len(requestedIDs) == 0 {
		return result, nil
	}
	canonicalBySnapshotTokenID, err := s.canonicalTokenMap(ctx, requestedIDs, requested)
	if err != nil {
		return nil, err
	}
	snapshotTokenIDs := make([]int64, 0, len(canonicalBySnapshotTokenID))
	for tokenID := range canonicalBySnapshotTokenID {
		snapshotTokenIDs = append(snapshotTokenIDs, tokenID)
	}
	if len(snapshotTokenIDs) == 0 {
		return result, nil
	}

	rows, err := s.pool.Query(ctx, `
		with per_account as (
			select baseline.token_id,
			       case when baseline.through_date is null
				then baseline.account_cost_usd
				else baseline.account_cost_usd + coalesce(sum(daily.account_cost_usd) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as account_cost_usd,
			       case when baseline.through_date is null
				then baseline.standard_cost_usd
				else baseline.standard_cost_usd + coalesce(sum(daily.standard_cost_usd) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as standard_cost_usd,
			       case when baseline.through_date is null
				then baseline.user_cost_usd
				else baseline.user_cost_usd + coalesce(sum(daily.user_cost_usd) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as user_cost_usd,
			       case when baseline.through_date is null
				then baseline.total_requests
				else baseline.total_requests + coalesce(sum(daily.total_requests) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as total_requests,
			       case when baseline.through_date is null
				then baseline.total_tokens
				else baseline.total_tokens + coalesce(sum(daily.total_tokens) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as total_tokens,
			       greatest(baseline.synced_at, max(daily.synced_at) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date)) as synced_at
			from sub2api_usage_snapshots baseline
			left join sub2api_usage_daily_snapshots daily
			  on daily.target_id = baseline.target_id
			 and daily.remote_account_id = baseline.remote_account_id
			where baseline.token_id = any($1::integer[])
			group by baseline.target_id, baseline.remote_account_id, baseline.token_id, baseline.through_date,
			         baseline.account_cost_usd, baseline.standard_cost_usd, baseline.user_cost_usd,
			         baseline.total_requests, baseline.total_tokens, baseline.synced_at
		)
		select token_id,
		       coalesce(sum(account_cost_usd), 0)::float8,
		       coalesce(sum(standard_cost_usd), 0)::float8,
		       coalesce(sum(user_cost_usd), 0)::float8,
		       coalesce(sum(total_requests), 0)::bigint,
		       coalesce(sum(total_tokens), 0)::bigint,
		       max(synced_at)
		from per_account
		group by token_id
	`, postgresIntIDs(snapshotTokenIDs))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var tokenID int64
		var item Sub2APIUsageCost
		if err := rows.Scan(&tokenID, &item.AccountCostUSD, &item.StandardCostUSD, &item.UserCostUSD, &item.TotalRequests, &item.TotalTokens, &item.SyncedAt); err != nil {
			rows.Close()
			return nil, err
		}
		canonicalID, ok := canonicalBySnapshotTokenID[tokenID]
		if !ok {
			continue
		}
		current := result[canonicalID]
		current.AccountCostUSD = roundCostUSD(current.AccountCostUSD + item.AccountCostUSD)
		current.StandardCostUSD = roundCostUSD(current.StandardCostUSD + item.StandardCostUSD)
		current.UserCostUSD = roundCostUSD(current.UserCostUSD + item.UserCostUSD)
		current.TotalRequests += item.TotalRequests
		current.TotalTokens += item.TotalTokens
		if current.SyncedAt == nil || item.SyncedAt != nil && item.SyncedAt.After(*current.SyncedAt) {
			current.SyncedAt = item.SyncedAt
		}
		result[canonicalID] = current
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	staleRows, err := s.pool.Query(ctx, `
		select m.token_id,
		       bool_or(
			case when usage.through_date is null then
				usage.synced_at is null
				or usage.status <> 'synced'
				or usage.synced_at < now() - make_interval(secs => greatest(t.check_interval_seconds * 3, 900))
			else
				usage.status <> 'synced'
				or daily.last_synced_at is null
				or daily.failed
				or daily.missing_days
				or daily.last_synced_at < now() - make_interval(secs => greatest(t.check_interval_seconds * 3, 900))
			end
		       )
		from sub2api_sync_mappings m
		join sub2api_sync_targets t on t.id = m.target_id and t.enabled = true
		left join sub2api_usage_snapshots usage
		  on usage.target_id = m.target_id
		 and usage.remote_account_id = m.remote_account_id
		left join lateral (
			select max(synced_at) filter (where status = 'synced') as last_synced_at,
			       coalesce(bool_or(status <> 'synced'), false) as failed,
			       coalesce(
				(max(usage_date) - usage.through_date) > count(*) filter (where status = 'synced'),
				true
			       ) as missing_days
			from sub2api_usage_daily_snapshots
			where target_id = m.target_id
			  and remote_account_id = m.remote_account_id
			  and usage_date > usage.through_date
		) daily on usage.through_date is not null
		where m.token_id = any($1::integer[])
		  and m.status = 'synced'
		  and coalesce(m.remote_account_id, 0) > 0
		group by m.token_id
	`, postgresIntIDs(snapshotTokenIDs))
	if err != nil {
		return nil, err
	}
	defer staleRows.Close()
	for staleRows.Next() {
		var tokenID int64
		var stale bool
		if err := staleRows.Scan(&tokenID, &stale); err != nil {
			return nil, err
		}
		canonicalID, ok := canonicalBySnapshotTokenID[tokenID]
		if !ok {
			continue
		}
		item := result[canonicalID]
		item.Stale = item.Stale || stale
		result[canonicalID] = item
	}
	return result, staleRows.Err()
}

func (s *Store) Sub2APIUsageCostsByOwner(ctx context.Context, ownerIDs []int64) (map[int64]float64, error) {
	ids := uniquePositiveInt64s(ownerIDs)
	result := make(map[int64]float64, len(ids))
	for _, ownerID := range ids {
		result[ownerID] = 0
	}
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := s.pool.Query(ctx, `
		with per_account as (
			select baseline.token_id,
			       case when baseline.through_date is null
				then baseline.account_cost_usd
				else baseline.account_cost_usd + coalesce(sum(daily.account_cost_usd) filter (where daily.status = 'synced' and daily.usage_date > baseline.through_date), 0)
			       end as account_cost_usd
			from sub2api_usage_snapshots baseline
			left join sub2api_usage_daily_snapshots daily
			  on daily.target_id = baseline.target_id
			 and daily.remote_account_id = baseline.remote_account_id
			group by baseline.target_id, baseline.remote_account_id, baseline.token_id,
			         baseline.through_date, baseline.account_cost_usd
		)
		select coalesce(parent.owner_user_id, token.owner_user_id) as owner_user_id,
		       coalesce(sum(usage.account_cost_usd), 0)::float8
		from per_account usage
		join codex_tokens token on token.id = usage.token_id
		left join codex_tokens parent on parent.id = token.merged_into_token_id
		where coalesce(parent.owner_user_id, token.owner_user_id) = any($1::bigint[])
		group by coalesce(parent.owner_user_id, token.owner_user_id)
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ownerID int64
		var cost float64
		if err := rows.Scan(&ownerID, &cost); err != nil {
			return nil, err
		}
		result[ownerID] = roundCostUSD(cost)
	}
	return result, rows.Err()
}

func ValidateDistinctSub2APIRemoteAccounts(mappings []Sub2APIUsageSyncMapping) error {
	seen := make(map[int64]int64, len(mappings))
	for _, mapping := range mappings {
		if previousTokenID, exists := seen[mapping.RemoteAccountID]; exists && previousTokenID != mapping.TokenID {
			return fmt.Errorf("remote sub2api account %d maps to multiple oaix tokens (%d, %d)", mapping.RemoteAccountID, previousTokenID, mapping.TokenID)
		}
		seen[mapping.RemoteAccountID] = mapping.TokenID
	}
	return nil
}
