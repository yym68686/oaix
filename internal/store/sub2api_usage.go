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
	TargetID         int64
	TokenID          int64
	RemoteAccountID  int64
	MappingCreatedAt time.Time
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

func (s *Store) ListDueSub2APIUsageSyncMappings(ctx context.Context, target Sub2APISyncTarget, limit int) ([]Sub2APIUsageSyncMapping, error) {
	if target.ID <= 0 || !target.Enabled {
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
		select m.target_id, m.token_id, coalesce(m.remote_account_id, 0), m.created_at
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
		  and (
			usage.remote_account_id is null
			or (usage.status = 'synced' and usage.synced_at < now() - make_interval(secs => $3))
			or (usage.status <> 'synced' and usage.updated_at < now() - make_interval(secs => $3))
		  )
		order by usage.synced_at asc nulls first, m.created_at asc, m.token_id asc
		limit $4
	`, target.ID, target.OwnerUserID, intervalSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Sub2APIUsageSyncMapping, 0, limit)
	for rows.Next() {
		var item Sub2APIUsageSyncMapping
		if err := rows.Scan(&item.TargetID, &item.TokenID, &item.RemoteAccountID, &item.MappingCreatedAt); err != nil {
			return nil, err
		}
		if item.TokenID > 0 && item.RemoteAccountID > 0 {
			items = append(items, item)
		}
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
				total_requests, total_tokens, source_computed_at,
				synced_at, status, error_message, created_at, updated_at
			)
			values($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), 'synced', null, now(), now())
			on conflict(target_id, remote_account_id) do update
			set token_id = excluded.token_id,
			    account_cost_usd = excluded.account_cost_usd,
			    standard_cost_usd = excluded.standard_cost_usd,
			    user_cost_usd = excluded.user_cost_usd,
			    total_requests = excluded.total_requests,
			    total_tokens = excluded.total_tokens,
			    source_computed_at = excluded.source_computed_at,
			    synced_at = excluded.synced_at,
			    status = 'synced',
			    error_message = null,
			    updated_at = now()
		`, targetID, item.RemoteAccountID, item.TokenID, item.AccountCostUSD, item.StandardCostUSD, item.UserCostUSD, item.TotalRequests, item.TotalTokens, item.SourceComputedAt)
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
		select token_id,
		       coalesce(sum(account_cost_usd), 0)::float8,
		       coalesce(sum(standard_cost_usd), 0)::float8,
		       coalesce(sum(user_cost_usd), 0)::float8,
		       coalesce(sum(total_requests), 0)::bigint,
		       coalesce(sum(total_tokens), 0)::bigint,
		       max(synced_at)
		from sub2api_usage_snapshots
		where token_id = any($1::integer[])
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
			usage.synced_at is null
			or usage.status <> 'synced'
			or usage.synced_at < now() - make_interval(secs => greatest(t.check_interval_seconds * 3, 900))
		       )
		from sub2api_sync_mappings m
		join sub2api_sync_targets t on t.id = m.target_id and t.enabled = true
		left join sub2api_usage_snapshots usage
		  on usage.target_id = m.target_id
		 and usage.remote_account_id = m.remote_account_id
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
		select coalesce(parent.owner_user_id, token.owner_user_id) as owner_user_id,
		       coalesce(sum(usage.account_cost_usd), 0)::float8
		from sub2api_usage_snapshots usage
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
