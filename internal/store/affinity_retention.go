package store

import (
	"context"
	"errors"
)

const gatewayRequestAttemptsRetentionIndexName = "ix_gateway_request_attempts_retention"

var errRequestAttemptRetentionIndexBuildInProgress = errors.New("request attempt retention index build is already in progress")

// EnsureRequestAttemptRetentionIndex builds the age index asynchronously from
// the worker path. It is concurrent and advisory-lock guarded, so a gateway
// restart or a second worker cannot hold an application table lock or start a
// duplicate build.
func (s *Store) EnsureRequestAttemptRetentionIndex(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, "select pg_try_advisory_lock(hashtext($1))", gatewayRequestAttemptsRetentionIndexName).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return errRequestAttemptRetentionIndexBuildInProgress
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "select pg_advisory_unlock(hashtext($1))", gatewayRequestAttemptsRetentionIndexName)
	}()
	var ready bool
	if err := conn.QueryRow(ctx, `
		select exists (
			select 1
			from pg_class c
			join pg_namespace n on n.oid=c.relnamespace
			join pg_index i on i.indexrelid=c.oid
			where n.nspname=current_schema() and c.relname=$1 and i.indisready and i.indisvalid
		)
	`, gatewayRequestAttemptsRetentionIndexName).Scan(&ready); err != nil {
		return err
	}
	if ready {
		return nil
	}
	var invalid, building bool
	if err := conn.QueryRow(ctx, `
		select exists (
			select 1
			from pg_class c
			join pg_namespace n on n.oid=c.relnamespace
			join pg_index i on i.indexrelid=c.oid
			where n.nspname=current_schema() and c.relname=$1 and (not i.indisready or not i.indisvalid)
		), exists (
			select 1
			from pg_stat_progress_create_index progress
			join pg_class c on c.oid=progress.index_relid
			join pg_namespace n on n.oid=c.relnamespace
			where n.nspname=current_schema() and c.relname=$1
		)
	`, gatewayRequestAttemptsRetentionIndexName).Scan(&invalid, &building); err != nil {
		return err
	}
	if building {
		return nil
	}
	if invalid {
		if _, err := conn.Exec(ctx, "drop index concurrently if exists "+gatewayRequestAttemptsRetentionIndexName); err != nil {
			return err
		}
	}
	_, err = conn.Exec(ctx, "create index concurrently if not exists "+gatewayRequestAttemptsRetentionIndexName+" on gateway_request_attempts (started_at, id) where finished_at is not null")
	return err
}

// AffinityRetentionResult reports rows removed from the two persistent prompt
// cache tables. Expired rows are never consulted by the hot-path lookups, so
// this cleanup only removes unreachable cache state.
type AffinityRetentionResult struct {
	PromptAffinityLanes   int64
	ResponseOwnerBindings int64
}

// DeleteExpiredAffinityRows removes expired prompt-cache rows in bounded
// batches. Each batch is its own short transaction so cleanup cannot hold a
// long-lived lock while the gateway is serving requests.
func (s *Store) DeleteExpiredAffinityRows(ctx context.Context, batchSize, maxBatches int) (AffinityRetentionResult, error) {
	result := AffinityRetentionResult{}
	if batchSize <= 0 || maxBatches <= 0 {
		return result, nil
	}
	for batch := 0; batch < maxBatches; batch++ {
		deleted, err := s.deleteExpiredAffinityRowsBatch(ctx, "prompt_affinity_lanes", batchSize)
		if err != nil {
			return result, err
		}
		result.PromptAffinityLanes += deleted
		if deleted < int64(batchSize) {
			break
		}
	}
	for batch := 0; batch < maxBatches; batch++ {
		deleted, err := s.deleteExpiredAffinityRowsBatch(ctx, "response_owner_bindings", batchSize)
		if err != nil {
			return result, err
		}
		result.ResponseOwnerBindings += deleted
		if deleted < int64(batchSize) {
			break
		}
	}
	return result, nil
}

func (s *Store) deleteExpiredAffinityRowsBatch(ctx context.Context, table string, batchSize int) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "set local lock_timeout = '500ms'"); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, "set local statement_timeout = '2s'"); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `
		delete from `+table+` expired
		where expired.ctid in (
			select ctid
			from `+table+`
			where expires_at <= now()
			order by expires_at asc
			limit $1
			for update skip locked
		)
	`, batchSize)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DeleteOldRequestAttempts removes completed diagnostic attempts beyond the
// configured TTL. Attempts are not a source of truth for routing or billing;
// online metrics use only the recent window and old request diagnostics are
// intentionally bounded rather than archived.
func (s *Store) DeleteOldRequestAttempts(ctx context.Context, retentionDays, batchSize, maxBatches int) (int64, error) {
	if retentionDays <= 0 || batchSize <= 0 || maxBatches <= 0 {
		return 0, nil
	}
	var ready bool
	if err := s.pool.QueryRow(ctx, `
		select exists (
			select 1 from pg_class c
			join pg_namespace n on n.oid=c.relnamespace
			join pg_index i on i.indexrelid=c.oid
			where n.nspname=current_schema() and c.relname=$1 and i.indisready and i.indisvalid
		)
	`, gatewayRequestAttemptsRetentionIndexName).Scan(&ready); err != nil {
		return 0, err
	}
	if !ready {
		return 0, nil
	}
	var deleted int64
	for batch := 0; batch < maxBatches; batch++ {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return deleted, err
		}
		if _, err := tx.Exec(ctx, "set local lock_timeout = '500ms'"); err != nil {
			_ = tx.Rollback(ctx)
			return deleted, err
		}
		if _, err := tx.Exec(ctx, "set local statement_timeout = '2s'"); err != nil {
			_ = tx.Rollback(ctx)
			return deleted, err
		}
		tag, err := tx.Exec(ctx, `
			delete from gateway_request_attempts expired
			where expired.ctid in (
				select ctid
				from gateway_request_attempts
				where started_at < now() - ($1::int * interval '1 day')
				  and finished_at is not null
				limit $2
				for update skip locked
			)
		`, retentionDays, batchSize)
		if err != nil {
			_ = tx.Rollback(ctx)
			return deleted, err
		}
		if err := tx.Commit(ctx); err != nil {
			return deleted, err
		}
		count := tag.RowsAffected()
		deleted += count
		if count < int64(batchSize) {
			break
		}
	}
	return deleted, nil
}
