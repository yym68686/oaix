package store

import (
	"context"
	"time"
)

// TokenUsageBucket projects existing request facts, without foreign billing or
// scheduler state. Scope is the token owner, including traffic from buyers of
// that owner's shared tokens. Historical usage from a previous owner is hidden.
type TokenUsageBucket struct {
	TokenID                                                                                     int64
	Date, Model, Endpoint                                                                       string
	Requests, Successes, InputTokens, OutputTokens, CachedTokens, CacheWriteTokens, TotalTokens int64
	EstimatedCostUSD, DurationMSSum                                                             float64
	DurationCount                                                                               int64
}

func (s *Store) TokenUsageBuckets(ctx context.Context, scope ResourceScope, ids []int64, from, to time.Time) ([]TokenUsageBucket, error) {
	out := []TokenUsageBucket{}
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{postgresIntIDs(ids), from, to}
	ownerWhere := scope.ownerFilter("t.owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `select t.id,to_char(l.started_at at time zone 'UTC','YYYY-MM-DD'),
 coalesce(l.model_name,l.model,''), l.endpoint,count(*)::bigint,count(*) filter(where l.success=true)::bigint,
 coalesce(sum(l.input_tokens),0)::bigint,coalesce(sum(l.output_tokens),0)::bigint,
 coalesce(sum(l.cached_input_tokens),0)::bigint,coalesce(sum(l.cache_write_input_tokens),0)::bigint,
 coalesce(sum(l.total_tokens),0)::bigint,coalesce(sum(l.estimated_cost_usd),0)::float8,
 coalesce(sum(l.duration_ms),0)::float8,count(l.duration_ms)::bigint
 from gateway_request_logs l join codex_tokens t on t.id=l.token_id and t.owner_user_id=l.token_owner_user_id
 where t.id=any($1::integer[]) and t.merged_into_token_id is null and l.started_at >= $2 and l.started_at < $3 and `+ownerWhere+`
 group by 1,2,3,4 order by 1,2,3,4`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b TokenUsageBucket
		if err := rows.Scan(&b.TokenID, &b.Date, &b.Model, &b.Endpoint, &b.Requests, &b.Successes, &b.InputTokens, &b.OutputTokens, &b.CachedTokens, &b.CacheWriteTokens, &b.TotalTokens, &b.EstimatedCostUSD, &b.DurationMSSum, &b.DurationCount); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// LatestTokenQuotaSnapshots loads an owner's latest persisted quota facts in
// one query, including empty results, without scheduling any refresh work.
func (s *Store) LatestTokenQuotaSnapshots(ctx context.Context, scope ResourceScope, ids []int64) (map[int64]QuotaSnapshot, error) {
	out := map[int64]QuotaSnapshot{}
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{postgresIntIDs(ids)}
	ownerWhere := scope.ownerFilter("t.owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `select q.id,q.token_id,q.snapshot,q.plan_type,q.error_message,q.fetched_at
 from codex_tokens t join lateral (select s.id,s.token_id,s.snapshot,s.plan_type,s.error_message,s.fetched_at from token_quota_snapshots s
 where s.token_id=t.id and s.owner_user_id=t.owner_user_id order by s.fetched_at desc,s.id desc limit 1) q on true
 where t.merged_into_token_id is null and t.id=any($1::integer[]) and `+ownerWhere, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s QuotaSnapshot
		if err := rows.Scan(&s.ID, &s.TokenID, &s.Snapshot, &s.PlanType, &s.ErrorMessage, &s.FetchedAt); err != nil {
			return nil, err
		}
		out[s.TokenID] = s
	}
	return out, rows.Err()
}
