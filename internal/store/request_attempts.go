package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type GatewayRequestAttempt struct {
	ID                  int64                `json:"id"`
	RequestID           string               `json:"request_id"`
	GatewayRequestLogID *int64               `json:"gateway_request_log_id,omitempty"`
	AttemptIndex        int                  `json:"attempt_index"`
	OwnerUserID         int64                `json:"owner_user_id"`
	SelectionMode       *string              `json:"selection_mode,omitempty"`
	CallerOwnerUserID   *int64               `json:"caller_owner_user_id,omitempty"`
	ExcludeOwnerUserID  *int64               `json:"exclude_owner_user_id,omitempty"`
	TokenID             *int64               `json:"token_id,omitempty"`
	TokenOwnerUserID    *int64               `json:"token_owner_user_id,omitempty"`
	Endpoint            string               `json:"endpoint,omitempty"`
	Model               *string              `json:"model,omitempty"`
	StartedAt           time.Time            `json:"started_at"`
	FinishedAt          *time.Time           `json:"finished_at,omitempty"`
	DurationMs          *int                 `json:"duration_ms,omitempty"`
	StatusCode          *int                 `json:"status_code,omitempty"`
	Success             *bool                `json:"success,omitempty"`
	Retry               *bool                `json:"retry,omitempty"`
	Outcome             string               `json:"outcome,omitempty"`
	Deactivated         bool                 `json:"deactivated"`
	CooldownUntil       *time.Time           `json:"cooldown_until,omitempty"`
	ErrorCode           *string              `json:"error_code,omitempty"`
	ErrorMessageExcerpt *string              `json:"error_message_excerpt,omitempty"`
	ErrorBodyHash       *string              `json:"error_body_hash,omitempty"`
	ClaimID             *int64               `json:"claim_id,omitempty"`
	CandidateCount      *int                 `json:"candidate_count,omitempty"`
	ReadyTokens         *int                 `json:"ready_tokens,omitempty"`
	SnapshotVersion     *int64               `json:"snapshot_version,omitempty"`
	StreamDeliveryTrace *StreamDeliveryTrace `json:"stream_delivery_trace,omitempty"`
}

type TokenStateEventContext struct {
	RequestID               string         `json:"request_id,omitempty"`
	GatewayRequestLogID     *int64         `json:"gateway_request_log_id,omitempty"`
	GatewayRequestAttemptID *int64         `json:"gateway_request_attempt_id,omitempty"`
	Endpoint                string         `json:"endpoint,omitempty"`
	Model                   string         `json:"model,omitempty"`
	StatusCode              *int           `json:"status_code,omitempty"`
	SelectionMode           string         `json:"selection_mode,omitempty"`
	CallerOwnerUserID       *int64         `json:"caller_owner_user_id,omitempty"`
	PreviousIsActive        *bool          `json:"previous_is_active,omitempty"`
	NextIsActive            *bool          `json:"next_is_active,omitempty"`
	Metadata                map[string]any `json:"metadata,omitempty"`
}

type TokenStateEvent struct {
	ID                      int64          `json:"id"`
	TokenID                 int64          `json:"token_id"`
	OwnerUserID             *int64         `json:"owner_user_id,omitempty"`
	EventType               string         `json:"event_type"`
	Reason                  *string        `json:"reason,omitempty"`
	CooldownUntil           *time.Time     `json:"cooldown_until,omitempty"`
	RequestID               *string        `json:"request_id,omitempty"`
	GatewayRequestLogID     *int64         `json:"gateway_request_log_id,omitempty"`
	GatewayRequestAttemptID *int64         `json:"gateway_request_attempt_id,omitempty"`
	Endpoint                *string        `json:"endpoint,omitempty"`
	Model                   *string        `json:"model,omitempty"`
	StatusCode              *int           `json:"status_code,omitempty"`
	SelectionMode           *string        `json:"selection_mode,omitempty"`
	CallerOwnerUserID       *int64         `json:"caller_owner_user_id,omitempty"`
	PreviousIsActive        *bool          `json:"previous_is_active,omitempty"`
	NextIsActive            *bool          `json:"next_is_active,omitempty"`
	Metadata                map[string]any `json:"metadata,omitempty"`
	CreatedAt               time.Time      `json:"created_at"`
}

type MetricRow struct {
	Labels map[string]string
	Value  int64
}

func intString(value int) string {
	return strconv.Itoa(value)
}

func (s *Store) InsertGatewayRequestAttempt(ctx context.Context, item GatewayRequestAttempt) (int64, error) {
	if item.StartedAt.IsZero() {
		item.StartedAt = time.Now().UTC()
	}
	var id int64
	streamDeliveryTrace := streamDeliveryTraceBytes(item.StreamDeliveryTrace)
	err := s.pool.QueryRow(ctx, `
		insert into gateway_request_attempts (
			request_id, gateway_request_log_id, attempt_index, owner_user_id, selection_mode,
			caller_owner_user_id, exclude_owner_user_id, token_id, token_owner_user_id,
			endpoint, model, started_at, finished_at, duration_ms, status_code, success, retry,
			outcome, deactivated, cooldown_until, error_code, error_message_excerpt, error_body_hash,
			claim_id, candidate_count, ready_tokens, snapshot_version, stream_delivery_trace
		)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)
		returning id
	`,
		item.RequestID,
		item.GatewayRequestLogID,
		item.AttemptIndex,
		item.OwnerUserID,
		item.SelectionMode,
		item.CallerOwnerUserID,
		item.ExcludeOwnerUserID,
		item.TokenID,
		item.TokenOwnerUserID,
		item.Endpoint,
		item.Model,
		item.StartedAt,
		item.FinishedAt,
		item.DurationMs,
		item.StatusCode,
		item.Success,
		item.Retry,
		nullableString(item.Outcome),
		item.Deactivated,
		item.CooldownUntil,
		item.ErrorCode,
		item.ErrorMessageExcerpt,
		item.ErrorBodyHash,
		item.ClaimID,
		item.CandidateCount,
		item.ReadyTokens,
		item.SnapshotVersion,
		streamDeliveryTrace,
	).Scan(&id)
	return id, err
}

func (s *Store) ListGatewayRequestAttempts(ctx context.Context, requestID string, limit int) ([]GatewayRequestAttempt, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		select id, request_id, gateway_request_log_id, attempt_index, owner_user_id, selection_mode,
		       caller_owner_user_id, exclude_owner_user_id, token_id, token_owner_user_id,
		       endpoint, model, started_at, finished_at, duration_ms, status_code, success, retry,
		       outcome, deactivated, cooldown_until, error_code, error_message_excerpt, error_body_hash,
		       claim_id, candidate_count, ready_tokens, snapshot_version, stream_delivery_trace
		from gateway_request_attempts
		where request_id = $1
		order by attempt_index asc, id asc
		limit $2
	`, requestID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanGatewayRequestAttempts(rows)
}

func (s *Store) ListTokenStateEvents(ctx context.Context, tokenID int64, limit int) ([]TokenStateEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		select id, token_id, owner_user_id, event_type, reason, cooldown_until, request_id,
		       gateway_request_log_id, gateway_request_attempt_id, endpoint, model, status_code,
		       selection_mode, caller_owner_user_id, previous_is_active, next_is_active, metadata,
		       created_at
		from token_state_events
		where token_id = $1
		order by created_at desc, id desc
		limit $2
	`, tokenID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokenStateEvents(rows)
}

func (s *Store) GatewayAttemptMetrics(ctx context.Context, since time.Time) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		select coalesce(selection_mode, '') as selection_mode,
		       coalesce(outcome, '') as outcome,
		       coalesce(status_code, 0) as status_code,
		       count(*)::bigint
		from gateway_request_attempts
		where created_at >= $1
		group by coalesce(selection_mode, ''), coalesce(outcome, ''), coalesce(status_code, 0)
		order by count(*) desc
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetricRow{}
	for rows.Next() {
		var selectionMode string
		var outcome string
		var statusCode int
		var count int64
		if err := rows.Scan(&selectionMode, &outcome, &statusCode, &count); err != nil {
			return nil, err
		}
		out = append(out, MetricRow{
			Labels: map[string]string{
				"selection_mode": selectionMode,
				"outcome":        outcome,
				"status_code":    intString(statusCode),
			},
			Value: count,
		})
	}
	return out, rows.Err()
}

func (s *Store) TokenStateMetrics(ctx context.Context, since time.Time, eventType string) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		select coalesce(e.status_code, 0) as status_code,
		       coalesce(e.reason, '') as reason,
		       coalesce(t.plan_type, '') as plan_type,
		       count(*)::bigint
		from token_state_events e
		left join codex_tokens t on t.id = e.token_id
		where e.created_at >= $1
		  and (
		    ($2 = 'disabled' and e.event_type = 'disabled')
		    or ($2 = 'cooldown' and e.cooldown_until is not null)
		  )
		group by coalesce(e.status_code, 0), coalesce(e.reason, ''), coalesce(t.plan_type, '')
		order by count(*) desc
	`, since, eventType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetricRow{}
	for rows.Next() {
		var statusCode int
		var reason string
		var planType string
		var count int64
		if err := rows.Scan(&statusCode, &reason, &planType, &count); err != nil {
			return nil, err
		}
		out = append(out, MetricRow{
			Labels: map[string]string{
				"status_code": intString(statusCode),
				"reason":      reason,
				"plan_type":   planType,
			},
			Value: count,
		})
	}
	return out, rows.Err()
}

func (s *Store) NoAvailableTokenMetrics(ctx context.Context, since time.Time) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		select case when caller_owner_user_id is not null and caller_owner_user_id <> owner_user_id then 'marketplace' else 'owner' end as scope,
		       coalesce(selection_mode, '') as selection_mode,
		       count(*)::bigint
		from gateway_request_attempts
		where created_at >= $1
		  and outcome = 'no_token'
		group by scope, coalesce(selection_mode, '')
		order by count(*) desc
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetricRow{}
	for rows.Next() {
		var scope string
		var selectionMode string
		var count int64
		if err := rows.Scan(&scope, &selectionMode, &count); err != nil {
			return nil, err
		}
		out = append(out, MetricRow{
			Labels: map[string]string{
				"scope":          scope,
				"selection_mode": selectionMode,
			},
			Value: count,
		})
	}
	return out, rows.Err()
}

func (s *Store) ImportItemMetrics(ctx context.Context, since time.Time) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		select coalesce(status, '') as status,
		       coalesce(refresh_error_code, '') as error_code,
		       count(*)::bigint
		from token_import_items
		where updated_at >= $1
		group by coalesce(status, ''), coalesce(refresh_error_code, '')
		order by count(*) desc
	`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetricRow{}
	for rows.Next() {
		var status string
		var errorCode string
		var count int64
		if err := rows.Scan(&status, &errorCode, &count); err != nil {
			return nil, err
		}
		out = append(out, MetricRow{
			Labels: map[string]string{
				"status":     status,
				"error_code": errorCode,
			},
			Value: count,
		})
	}
	return out, rows.Err()
}

func (s *Store) ImportReactivatedTotal(ctx context.Context, since time.Time) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx, `
		select count(*)::bigint
		from token_import_items
		where updated_at >= $1
		  and reactivated = true
	`, since).Scan(&count)
	return count, err
}

func scanGatewayRequestAttempts(rows pgx.Rows) ([]GatewayRequestAttempt, error) {
	items := make([]GatewayRequestAttempt, 0)
	for rows.Next() {
		var item GatewayRequestAttempt
		var streamDeliveryTrace []byte
		if err := rows.Scan(
			&item.ID,
			&item.RequestID,
			&item.GatewayRequestLogID,
			&item.AttemptIndex,
			&item.OwnerUserID,
			&item.SelectionMode,
			&item.CallerOwnerUserID,
			&item.ExcludeOwnerUserID,
			&item.TokenID,
			&item.TokenOwnerUserID,
			&item.Endpoint,
			&item.Model,
			&item.StartedAt,
			&item.FinishedAt,
			&item.DurationMs,
			&item.StatusCode,
			&item.Success,
			&item.Retry,
			&item.Outcome,
			&item.Deactivated,
			&item.CooldownUntil,
			&item.ErrorCode,
			&item.ErrorMessageExcerpt,
			&item.ErrorBodyHash,
			&item.ClaimID,
			&item.CandidateCount,
			&item.ReadyTokens,
			&item.SnapshotVersion,
			&streamDeliveryTrace,
		); err != nil {
			return nil, err
		}
		if len(streamDeliveryTrace) > 0 {
			if err := json.Unmarshal(streamDeliveryTrace, &item.StreamDeliveryTrace); err != nil {
				return nil, fmt.Errorf("decode stream delivery trace for attempt %d: %w", item.ID, err)
			}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanTokenStateEvents(rows pgx.Rows) ([]TokenStateEvent, error) {
	items := make([]TokenStateEvent, 0)
	for rows.Next() {
		var item TokenStateEvent
		var metadata []byte
		if err := rows.Scan(
			&item.ID,
			&item.TokenID,
			&item.OwnerUserID,
			&item.EventType,
			&item.Reason,
			&item.CooldownUntil,
			&item.RequestID,
			&item.GatewayRequestLogID,
			&item.GatewayRequestAttemptID,
			&item.Endpoint,
			&item.Model,
			&item.StatusCode,
			&item.SelectionMode,
			&item.CallerOwnerUserID,
			&item.PreviousIsActive,
			&item.NextIsActive,
			&metadata,
			&item.CreatedAt,
		); err != nil {
			return nil, err
		}
		if len(metadata) > 0 {
			_ = json.Unmarshal(metadata, &item.Metadata)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
