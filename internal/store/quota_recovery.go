package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	QuotaRecoveryProbeStartedEvent = "quota_recovery_probe_started"
	QuotaRecoveryReactivatedEvent  = "quota_recovery_reactivated"
	QuotaRecoveryUsageLimitedEvent = "quota_recovery_usage_limited"
	QuotaRecoveryInconclusiveEvent = "quota_recovery_inconclusive"
	TokenUsageLimitConfirmedEvent  = "usage_limit_confirmed"
	QuotaRecoveryModel             = "gpt-5.4-mini"
)

type QuotaRecoveryCandidate struct {
	TokenID        int64
	OwnerUserID    int64
	CooldownUntil  time.Time
	SourceEventID  int64
	QuotaSnapshot  json.RawMessage
	QuotaFetchedAt *time.Time
}

type QuotaRecoveryClaim struct {
	TokenID       int64
	OwnerUserID   int64
	CooldownUntil time.Time
	SourceEventID int64
	ProbeEventID  int64
	StartedAt     time.Time
}

type QuotaRecoveryResult struct {
	Outcome    string
	Reason     string
	StatusCode int
	Metadata   map[string]any
}

type QuotaRecoveryCredentialFence struct {
	AccessToken  string
	RefreshToken string
	AccountID    string
}

type QuotaRecoveryCheckLease struct {
	conn    *pgxpool.Conn
	tokenID int64
	once    sync.Once
}

const quotaRecoveryAdvisoryNamespace int32 = 0x4f414958

func (s *Store) TryQuotaRecoveryCheckLease(ctx context.Context, tokenID int64) (*QuotaRecoveryCheckLease, bool, error) {
	if tokenID <= 0 || tokenID > int64(^uint32(0)>>1) {
		return nil, false, fmt.Errorf("invalid token id for quota recovery lease")
	}
	conn, err := s.PoolFor(WorkloadWorker).Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1, $2)`, quotaRecoveryAdvisoryNamespace, int32(tokenID)).Scan(&acquired); err != nil {
		// The server may have acquired the session lock even when the client did
		// not receive the result. Never return a connection with unknown lock
		// state to the pool.
		discardQuotaRecoveryLeaseConn(conn)
		return nil, false, err
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}
	return &QuotaRecoveryCheckLease{conn: conn, tokenID: tokenID}, true, nil
}

func (l *QuotaRecoveryCheckLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		if l.conn == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var unlocked bool
		err := l.conn.QueryRow(ctx, `select pg_advisory_unlock($1, $2)`, quotaRecoveryAdvisoryNamespace, int32(l.tokenID)).Scan(&unlocked)
		if err != nil || !unlocked {
			discardQuotaRecoveryLeaseConn(l.conn)
			return
		}
		l.conn.Release()
	})
}

func discardQuotaRecoveryLeaseConn(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Conn().Close(ctx)
	conn.Release()
}

func (s *Store) ListQuotaRecoveryCandidates(ctx context.Context) ([]QuotaRecoveryCandidate, error) {
	rows, err := s.PoolFor(WorkloadWorker).Query(ctx, `
		select t.id,
		       coalesce(t.owner_user_id, 0),
		       t.cooldown_until,
		       source_event.id,
		       quota.snapshot,
		       quota.fetched_at
		from codex_tokens t
		join lateral (
			select e.id, e.event_type, e.reason, e.cooldown_until, e.status_code, e.metadata
			from token_state_events e
			where e.token_id = t.id
			  and e.cooldown_until is not null
			order by e.created_at desc, e.id desc
			limit 1
		) source_event on source_event.cooldown_until = t.cooldown_until
		                  and (
		                    (
		                      source_event.event_type in ('quota_recovery_usage_limited', 'usage_limit_confirmed')
		                      and source_event.status_code = 429
		                      and source_event.metadata->>'failure_kind' = 'usage_limit_reached'
		                    )
		                    or (
		                      source_event.event_type = 'error'
		                      and (source_event.status_code = 429 or source_event.status_code is null)
		                      and lower(coalesce(source_event.reason, '')) in (
		                        'upstream usage limit cooldown',
		                        'http 429: the usage limit has been reached'
		                      )
		                    )
		                  )
		left join lateral (
			select q.snapshot, q.fetched_at
			from token_quota_snapshots q
			where q.token_id = t.id
			order by q.fetched_at desc, q.id desc
			limit 1
		) quota on true
		where t.is_active = true
		  and t.disabled_at is null
		  and t.merged_into_token_id is null
		  and t.cooldown_until > now()
		  and nullif(t.access_token, '') is not null
		order by t.cooldown_until asc,
		         t.id asc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	candidates := make([]QuotaRecoveryCandidate, 0)
	for rows.Next() {
		var candidate QuotaRecoveryCandidate
		var quotaSnapshot []byte
		var quotaFetchedAt sql.NullTime
		if err := rows.Scan(
			&candidate.TokenID,
			&candidate.OwnerUserID,
			&candidate.CooldownUntil,
			&candidate.SourceEventID,
			&quotaSnapshot,
			&quotaFetchedAt,
		); err != nil {
			return nil, err
		}
		if len(quotaSnapshot) > 0 {
			candidate.QuotaSnapshot = append(json.RawMessage(nil), quotaSnapshot...)
		}
		if quotaFetchedAt.Valid {
			value := quotaFetchedAt.Time.UTC()
			candidate.QuotaFetchedAt = &value
		}
		candidate.CooldownUntil = candidate.CooldownUntil.UTC()
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func (s *Store) BeginQuotaRecoveryProbe(ctx context.Context, candidate QuotaRecoveryCandidate, retryInterval time.Duration, minimumLeadTime time.Duration) (*QuotaRecoveryClaim, bool, error) {
	if candidate.TokenID <= 0 || candidate.SourceEventID <= 0 || candidate.CooldownUntil.IsZero() {
		return nil, false, fmt.Errorf("invalid quota recovery candidate")
	}
	if retryInterval <= 0 {
		return nil, false, fmt.Errorf("quota recovery retry interval must be positive")
	}
	if minimumLeadTime < 0 {
		return nil, false, fmt.Errorf("quota recovery minimum lead time must not be negative")
	}
	var claim *QuotaRecoveryClaim
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var ownerUserID sql.NullInt64
		var isActive bool
		var disabledAt sql.NullTime
		var cooldownUntil sql.NullTime
		var mergedInto sql.NullInt64
		var databaseNow time.Time
		if err := tx.QueryRow(ctx, `
			select owner_user_id, is_active, disabled_at, cooldown_until, merged_into_token_id, now()
			from codex_tokens
			where id = $1
			for update
		`, candidate.TokenID).Scan(&ownerUserID, &isActive, &disabledAt, &cooldownUntil, &mergedInto, &databaseNow); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		if !isActive || disabledAt.Valid || mergedInto.Valid || !cooldownUntil.Valid || !cooldownUntil.Time.Equal(candidate.CooldownUntil) || !cooldownUntil.Time.After(databaseNow.Add(minimumLeadTime)) {
			return nil
		}
		sourceEventID, ok, err := currentQuotaRecoverySourceEvent(ctx, tx, candidate.TokenID)
		if err != nil {
			return err
		}
		if !ok || sourceEventID != candidate.SourceEventID {
			return nil
		}
		var lastStartedAt sql.NullTime
		if err := tx.QueryRow(ctx, `
			select max(created_at)
			from token_state_events
			where token_id = $1 and event_type = $2
		`, candidate.TokenID, QuotaRecoveryProbeStartedEvent).Scan(&lastStartedAt); err != nil {
			return err
		}
		if lastStartedAt.Valid && lastStartedAt.Time.After(databaseNow.Add(-retryInterval)) {
			return nil
		}
		ownerID := int64(0)
		var ownerParam any
		if ownerUserID.Valid {
			ownerID = ownerUserID.Int64
			ownerParam = ownerID
		}
		metadata := map[string]any{
			"source_event_id": candidate.SourceEventID,
			"cooldown_until":  candidate.CooldownUntil.UTC().Format(time.RFC3339Nano),
		}
		var eventID int64
		var startedAt time.Time
		if err := tx.QueryRow(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, model,
				previous_is_active, next_is_active, metadata
			)
			values ($1, $2, $3, $4, $5, true, true, $6)
			returning id, created_at
		`, candidate.TokenID, ownerParam, QuotaRecoveryProbeStartedEvent,
			"automatic quota recovery probe started", QuotaRecoveryModel, jsonBytes(metadata),
		).Scan(&eventID, &startedAt); err != nil {
			return err
		}
		claim = &QuotaRecoveryClaim{
			TokenID:       candidate.TokenID,
			OwnerUserID:   ownerID,
			CooldownUntil: candidate.CooldownUntil,
			SourceEventID: candidate.SourceEventID,
			ProbeEventID:  eventID,
			StartedAt:     startedAt.UTC(),
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return claim, claim != nil, nil
}

func (s *Store) CompleteQuotaRecovery(ctx context.Context, claim QuotaRecoveryClaim, fence QuotaRecoveryCredentialFence, metadata map[string]any) (bool, error) {
	applied := false
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		ownerUserID, matches, err := quotaRecoveryClaimMatches(ctx, tx, claim, fence)
		if err != nil || !matches {
			return err
		}
		tag, err := tx.Exec(ctx, `
			update codex_tokens
			set cooldown_until = null,
			    last_error = null,
			    updated_at = now()
			where id = $1
			  and is_active = true
			  and disabled_at is null
			  and merged_into_token_id is null
			  and cooldown_until = $2
		`, claim.TokenID, claim.CooldownUntil)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return nil
		}
		if _, err := tx.Exec(ctx, `
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
		`, claim.TokenID, nullableOwnerID(ownerUserID)); err != nil {
			return err
		}
		metadata = quotaRecoveryMetadata(claim, metadata)
		if _, err := tx.Exec(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, model, status_code,
				previous_is_active, next_is_active, metadata
			)
			values ($1, $2, $3, $4, $5, 200, true, true, $6)
		`, claim.TokenID, nullableOwnerID(ownerUserID), QuotaRecoveryReactivatedEvent,
			"automatic quota recovery probe completed", QuotaRecoveryModel, jsonBytes(metadata)); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied, err
}

func (s *Store) ApplyQuotaRecoveryUsageLimit(ctx context.Context, claim QuotaRecoveryClaim, fence QuotaRecoveryCredentialFence, resetAt *time.Time, minimumRetry time.Duration, reason string, metadata map[string]any) (bool, *time.Time, error) {
	applied := false
	var appliedCooldown *time.Time
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		ownerUserID, matches, err := quotaRecoveryClaimMatches(ctx, tx, claim, fence)
		if err != nil || !matches {
			return err
		}
		var databaseNow time.Time
		if err := tx.QueryRow(ctx, `select clock_timestamp()`).Scan(&databaseNow); err != nil {
			return err
		}
		cooldownUntil := quotaRecoveryUsageLimitCooldown(claim.CooldownUntil, resetAt, databaseNow, minimumRetry)
		reason = truncate(strings.TrimSpace(reason), 4000)
		if reason == "" {
			reason = "usage_limit_reached"
		} else if !strings.Contains(strings.ToLower(reason), "usage_limit_reached") {
			reason = truncate("usage_limit_reached: "+reason, 4000)
		}
		tag, err := tx.Exec(ctx, `
			update codex_tokens
			set cooldown_until = $3,
			    last_error = $4,
			    updated_at = now()
			where id = $1
			  and is_active = true
			  and disabled_at is null
			  and merged_into_token_id is null
			  and cooldown_until = $2
		`, claim.TokenID, claim.CooldownUntil, cooldownUntil, reason)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return nil
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
		`, claim.TokenID, nullableOwnerID(ownerUserID), cooldownUntil, reason); err != nil {
			return err
		}
		metadata = quotaRecoveryMetadata(claim, metadata)
		metadata["failure_kind"] = "usage_limit_reached"
		metadata["applied_cooldown_until"] = cooldownUntil.Format(time.RFC3339Nano)
		if _, err := tx.Exec(ctx, `
			insert into token_state_events(
				token_id, owner_user_id, event_type, reason, cooldown_until, model, status_code,
				previous_is_active, next_is_active, metadata
			)
			values ($1, $2, $3, $4, $5, $6, 429, true, true, $7)
		`, claim.TokenID, nullableOwnerID(ownerUserID), QuotaRecoveryUsageLimitedEvent,
			reason, cooldownUntil, QuotaRecoveryModel, jsonBytes(metadata)); err != nil {
			return err
		}
		applied = true
		value := cooldownUntil
		appliedCooldown = &value
		return nil
	})
	return applied, appliedCooldown, err
}

func (s *Store) RecordQuotaRecoveryResult(ctx context.Context, claim QuotaRecoveryClaim, result QuotaRecoveryResult) error {
	metadata := quotaRecoveryMetadata(claim, result.Metadata)
	metadata["outcome"] = strings.TrimSpace(result.Outcome)
	_, err := s.PoolFor(WorkloadWorker).Exec(ctx, `
		insert into token_state_events(
			token_id, owner_user_id, event_type, reason, model, status_code,
			previous_is_active, next_is_active, metadata
		)
		select $1, owner_user_id, $2, $3, $4, nullif($5, 0), is_active, is_active, $6
		from codex_tokens
		where id = $1
	`, claim.TokenID, QuotaRecoveryInconclusiveEvent, truncate(result.Reason, 4000),
		QuotaRecoveryModel, result.StatusCode, jsonBytes(metadata))
	return err
}

func currentQuotaRecoverySourceEvent(ctx context.Context, tx pgx.Tx, tokenID int64) (int64, bool, error) {
	var id int64
	var eventType string
	var reason sql.NullString
	var statusCode sql.NullInt64
	var failureKind string
	err := tx.QueryRow(ctx, `
		select id, event_type, reason, status_code, coalesce(metadata->>'failure_kind', '')
		from token_state_events
		where token_id = $1 and cooldown_until is not null
		order by created_at desc, id desc
		limit 1
	`, tokenID).Scan(&id, &eventType, &reason, &statusCode, &failureKind)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, validQuotaRecoverySource(eventType, statusCode, reason.String, failureKind), nil
}

func validQuotaRecoverySource(eventType string, statusCode sql.NullInt64, reason string, failureKind string) bool {
	lowered := strings.ToLower(strings.TrimSpace(reason))
	if eventType == QuotaRecoveryUsageLimitedEvent || eventType == TokenUsageLimitConfirmedEvent {
		return statusCode.Valid && statusCode.Int64 == 429 && strings.TrimSpace(failureKind) == "usage_limit_reached"
	}
	if eventType != "error" || (statusCode.Valid && statusCode.Int64 != 429) {
		return false
	}
	return lowered == "upstream usage limit cooldown" || lowered == "http 429: the usage limit has been reached"
}

func quotaRecoveryUsageLimitCooldown(previous time.Time, resetAt *time.Time, now time.Time, minimumRetry time.Duration) time.Time {
	cooldownUntil := previous.UTC()
	if resetAt != nil {
		candidate := resetAt.UTC()
		if candidate.After(now) {
			cooldownUntil = candidate
		}
	}
	if cooldownUntil.After(now) {
		return cooldownUntil
	}
	if minimumRetry <= 0 {
		minimumRetry = 5 * time.Minute
	}
	return now.UTC().Add(minimumRetry)
}

func quotaRecoveryClaimMatches(ctx context.Context, tx pgx.Tx, claim QuotaRecoveryClaim, fence QuotaRecoveryCredentialFence) (sql.NullInt64, bool, error) {
	var ownerUserID sql.NullInt64
	var isActive bool
	var disabledAt sql.NullTime
	var cooldownUntil sql.NullTime
	var mergedInto sql.NullInt64
	var credentialsMatch bool
	if err := tx.QueryRow(ctx, `
		select owner_user_id,
		       is_active,
		       disabled_at,
		       cooldown_until,
		       merged_into_token_id,
		       coalesce(access_token, '') = $2
		         and coalesce(refresh_token, '') = $3
		         and btrim(coalesce(account_id, '')) = $4
		from codex_tokens
		where id = $1
		for update
	`, claim.TokenID, fence.AccessToken, fence.RefreshToken, strings.TrimSpace(fence.AccountID)).Scan(
		&ownerUserID, &isActive, &disabledAt, &cooldownUntil, &mergedInto, &credentialsMatch,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ownerUserID, false, nil
		}
		return ownerUserID, false, err
	}
	if !isActive || disabledAt.Valid || mergedInto.Valid || !cooldownUntil.Valid || !cooldownUntil.Time.Equal(claim.CooldownUntil) || !credentialsMatch {
		return ownerUserID, false, nil
	}
	sourceEventID, validSource, err := currentQuotaRecoverySourceEvent(ctx, tx, claim.TokenID)
	if err != nil || !validSource || sourceEventID != claim.SourceEventID {
		return ownerUserID, false, err
	}
	var latestProbeEventID int64
	if err := tx.QueryRow(ctx, `
		select id
		from token_state_events
		where token_id = $1 and event_type = $2
		order by created_at desc, id desc
		limit 1
	`, claim.TokenID, QuotaRecoveryProbeStartedEvent).Scan(&latestProbeEventID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ownerUserID, false, nil
		}
		return ownerUserID, false, err
	}
	return ownerUserID, latestProbeEventID == claim.ProbeEventID, nil
}

func quotaRecoveryMetadata(claim QuotaRecoveryClaim, metadata map[string]any) map[string]any {
	result := make(map[string]any, len(metadata)+4)
	for key, value := range metadata {
		result[key] = value
	}
	result["source_event_id"] = claim.SourceEventID
	result["probe_event_id"] = claim.ProbeEventID
	result["cooldown_before"] = claim.CooldownUntil.UTC().Format(time.RFC3339Nano)
	result["probe_started_at"] = claim.StartedAt.UTC().Format(time.RFC3339Nano)
	return result
}

func nullableOwnerID(ownerUserID sql.NullInt64) any {
	if ownerUserID.Valid {
		return ownerUserID.Int64
	}
	return nil
}
