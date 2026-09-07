package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Current costs are maintained in the log transaction, including late changes
// to already aggregated request IDs. Only fully initialized canonical accounts
// use this path; other accounts keep the existing exact reader during rollout.
func (s *Store) currentTokenCosts(ctx context.Context, tokens []Token) (map[int64]*float64, error) {
	requested := map[int64]struct{}{}
	ids := make([]int64, 0, len(tokens))
	for _, token := range tokens {
		if token.ID > 0 {
			if _, exists := requested[token.ID]; !exists {
				requested[token.ID] = struct{}{}
				ids = append(ids, token.ID)
			}
		}
	}
	result := make(map[int64]*float64, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	canonical, err := s.canonicalTokenMap(ctx, ids, requested)
	if err != nil {
		return nil, err
	}
	logIDs := make([]int64, 0, len(canonical))
	for id := range canonical {
		logIDs = append(logIDs, id)
	}
	rows, err := s.pool.Query(ctx, `select token_id, estimated_cost_usd::float8
		from gateway_current_token_costs where token_id=any($1::integer[])`, postgresIntIDs(logIDs))
	if err != nil {
		return nil, err
	}
	ready := map[int64]bool{}
	for rows.Next() {
		var id int64
		var cost float64
		if err := rows.Scan(&id, &cost); err != nil {
			rows.Close()
			return nil, err
		}
		ready[id] = true
		addCanonicalCost(canonical, result, id, cost)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	missing := map[int64]bool{}
	for id, parent := range canonical {
		if !ready[id] {
			missing[parent] = true
		}
	}
	var fallback []Token
	for _, token := range tokens {
		if missing[token.ID] {
			fallback = append(fallback, token)
		}
	}
	if len(fallback) > 0 {
		values, err := s.tokenObservedCostsAggregateSnapshot(ctx, fallback, true, true)
		if err != nil {
			return nil, err
		}
		for id, value := range values {
			result[id] = value
		}
	}
	fillMissingObservedCosts(tokens, result)
	return result, nil
}

// prepareCurrentTokenCostSeed briefly serializes with cost writers. Once this
// transaction commits, every subsequent cost delta is captured privately.
func (s *Store) prepareCurrentTokenCostSeed(ctx context.Context, tokenID int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var locked bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(17984321,$1::integer)`, tokenID).Scan(&locked); err != nil || !locked {
		return false, err
	}
	var ready bool
	if err := tx.QueryRow(ctx, `select exists(select 1 from gateway_current_token_costs where token_id=$1)`, tokenID).Scan(&ready); err != nil {
		return false, err
	}
	if ready {
		if _, err := tx.Exec(ctx, `delete from gateway_token_cost_seed_deltas where token_id=$1`, tokenID); err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `insert into gateway_token_cost_seed_deltas(token_id) values($1) on conflict do nothing`, tokenID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type currentTokenCostSeed struct{ amount, delta string }

// Both source total and captured delta use one MVCC snapshot. Aggregation can
// move a log from pending to recorded without double counting it.
func readCurrentTokenCostSeed(ctx context.Context, tx pgx.Tx, tokenID int64) (currentTokenCostSeed, error) {
	var seed currentTokenCostSeed
	err := tx.QueryRow(ctx, `
  select (case when t.created_at >= (select started_at from gateway_request_logs order by started_at limit 1)
  then (select coalesce(sum(l.estimated_cost_usd::numeric),0) from gateway_request_logs l where l.token_id=t.id and l.estimated_cost_usd is not null)
  else coalesce(a.estimated_cost_usd::numeric,0) +
       (select coalesce(sum(l.estimated_cost_usd::numeric),0) from gateway_request_logs l where l.token_id=t.id and l.analytics_recorded_at is null and l.estimated_cost_usd is not null)
  end)::text,p.delta_usd::text
  from codex_tokens t left join gateway_request_token_costs a on a.token_id=t.id
  join gateway_token_cost_seed_deltas p on p.token_id=t.id
  where t.id=$1 and not exists(select 1 from gateway_current_token_costs c where c.token_id=t.id)
 `, tokenID).Scan(&seed.amount, &seed.delta)
	return seed, err
}

// Publication takes the writer lock only for two small-row writes. A writer
// committed after the snapshot contributes exactly delta_now - delta_snapshot.
func publishCurrentTokenCostSeed(ctx context.Context, tx pgx.Tx, tokenID int64, seed currentTokenCostSeed) (bool, error) {
	var locked bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(17984321,$1::integer)`, tokenID).Scan(&locked); err != nil || !locked {
		return false, err
	}
	tag, err := tx.Exec(ctx, `insert into gateway_current_token_costs(token_id,estimated_cost_usd)
 select token_id,$2::numeric+delta_usd-$3::numeric from gateway_token_cost_seed_deltas where token_id=$1
 on conflict do nothing`, tokenID, seed.amount, seed.delta)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `delete from gateway_token_cost_seed_deltas where token_id=$1`, tokenID); err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (s *Store) initializeCurrentTokenCost(ctx context.Context, tokenID int64) (bool, error) {
	prepared, err := s.prepareCurrentTokenCostSeed(ctx, tokenID)
	if err != nil || !prepared {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// A separate lock serializes new initializers without blocking log writers
	// or older release initializers. ON CONFLICT preserves an older seed winner.
	var locked bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(17984322,$1::integer)`, tokenID).Scan(&locked); err != nil || !locked {
		return false, err
	}
	if _, err := tx.Exec(ctx, `set local statement_timeout='3s'; set local lock_timeout='100ms'`); err != nil {
		return false, err
	}
	seed, err := readCurrentTokenCostSeed(ctx, tx, tokenID)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `delete from gateway_token_cost_seed_deltas where token_id=$1 and exists(select 1 from gateway_current_token_costs where token_id=$1)`, tokenID)
		if err != nil {
			return false, err
		}
		return false, tx.Commit(ctx)
	}
	if err != nil {
		return false, err
	}
	changed, err := publishCurrentTokenCostSeed(ctx, tx, tokenID, seed)
	if err != nil {
		return false, err
	}
	return changed, tx.Commit(ctx)
}

// BackfillCurrentTokenCosts visits missing rows in descending ID order. The
// caller retains the cursor so a costly account never starves later accounts.
func (s *Store) BackfillCurrentTokenCosts(ctx context.Context, beforeID int64) (nextID int64, initialized int, err error) {
	if beforeID <= 0 {
		beforeID = 1 << 62
	}
	ready, err := s.RequestTokenCostsReconciled(ctx)
	if err != nil || !ready {
		return beforeID, 0, err
	}
	rows, err := s.pool.Query(ctx, `select t.id from codex_tokens t
		where t.id < $1::bigint and not exists(select 1 from gateway_current_token_costs c where c.token_id=t.id)
		order by t.id desc limit 32`, beforeID)
	if err != nil {
		return beforeID, 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return beforeID, 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return beforeID, 0, err
	}
	nextID = 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return nextID, initialized, ctx.Err()
		}
		nextID = id
		stepCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		changed, seedErr := s.initializeCurrentTokenCost(stepCtx, id)
		cancel()
		if seedErr != nil {
			// Statement timeouts are reported and retried on the next cursor pass.
			return nextID, initialized, seedErr
		}
		if changed {
			initialized++
		}
	}
	return nextID, initialized, nil
}

func (s *Store) BackfillSub2APIUsageRollups(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `select b.target_id,b.remote_account_id
		from sub2api_usage_snapshots b left join sub2api_usage_rollups r using(target_id,remote_account_id)
		where r.ready is distinct from true order by b.target_id,b.remote_account_id limit 64`)
	if err != nil {
		return 0, err
	}
	var accounts [][2]int64
	for rows.Next() {
		var id [2]int64
		if err := rows.Scan(&id[0], &id[1]); err != nil {
			rows.Close()
			return 0, err
		}
		accounts = append(accounts, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for i, id := range accounts {
		if _, err := s.pool.Exec(ctx, `select oaix_refresh_usage_rollup($1,$2)`, id[0], id[1]); err != nil {
			return i, err
		}
	}
	return len(accounts), nil
}

func (s *Store) maintenanceSettingCompleted(ctx context.Context, key string) (bool, error) {
	var completed bool
	err := s.pool.QueryRow(ctx, `select coalesce((value->>'completed')::boolean,false) from gateway_settings where key=$1`, key).Scan(&completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return completed, err
}
