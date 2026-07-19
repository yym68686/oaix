package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type GatewayIdempotencyAction string

const (
	GatewayIdempotencyExecute  GatewayIdempotencyAction = "execute"
	GatewayIdempotencyReplay   GatewayIdempotencyAction = "replay"
	GatewayIdempotencyWait     GatewayIdempotencyAction = "wait"
	GatewayIdempotencyConflict GatewayIdempotencyAction = "conflict"
)

type GatewayIdempotencyBegin struct {
	OwnerUserID   int64
	KeyHash       string
	RequestHash   string
	RequestID     string
	LeaseToken    string
	LeaseDuration time.Duration
	RecordTTL     time.Duration
}

type GatewayIdempotencyRecord struct {
	OwnerUserID     int64
	KeyHash         string
	RequestHash     string
	RequestID       string
	Generation      int64
	State           string
	LeaseToken      *string
	LeaseExpiresAt  *time.Time
	StatusCode      *int
	ResponseHeaders json.RawMessage
	ResponseBody    []byte
	Replayable      bool
	FailureReason   *string
	CreatedAt       time.Time
	StartedAt       time.Time
	CompletedAt     *time.Time
	UpdatedAt       time.Time
	ExpiresAt       time.Time
}

type GatewayIdempotencyBeginResult struct {
	Action GatewayIdempotencyAction
	Record GatewayIdempotencyRecord
}

type GatewayIdempotencyCompletion struct {
	OwnerUserID     int64
	KeyHash         string
	LeaseToken      string
	StatusCode      int
	ResponseHeaders json.RawMessage
	ResponseBody    []byte
	RecordTTL       time.Duration
}

type gatewayIdempotencyRowScanner interface {
	Scan(dest ...any) error
}

const gatewayIdempotencyColumns = `
	owner_user_id, key_hash, request_hash, request_id, generation, state,
	lease_token, lease_expires_at, status_code, response_headers, response_body,
	replayable, failure_reason, created_at, started_at, completed_at, updated_at, expires_at
`

func (s *Store) BeginGatewayIdempotency(ctx context.Context, input GatewayIdempotencyBegin) (GatewayIdempotencyBeginResult, error) {
	if err := validateGatewayIdempotencyBegin(input); err != nil {
		return GatewayIdempotencyBeginResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return GatewayIdempotencyBeginResult{}, err
	}
	defer tx.Rollback(ctx)

	record, err := scanGatewayIdempotencyRecord(tx.QueryRow(ctx, `
		insert into gateway_idempotency_records(
			owner_user_id, key_hash, request_hash, request_id, generation, state,
			lease_token, lease_expires_at, replayable, created_at, started_at, updated_at, expires_at
		)
		values ($1, $2, $3, $4, 1, 'in_progress', $5,
		        now() + make_interval(secs => $6), false, now(), now(), now(),
		        now() + make_interval(secs => $7))
		on conflict (owner_user_id, key_hash) do nothing
		returning `+gatewayIdempotencyColumns,
		input.OwnerUserID,
		input.KeyHash,
		input.RequestHash,
		input.RequestID,
		input.LeaseToken,
		input.LeaseDuration.Seconds(),
		input.RecordTTL.Seconds(),
	))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return GatewayIdempotencyBeginResult{}, err
		}
		return GatewayIdempotencyBeginResult{Action: GatewayIdempotencyExecute, Record: record}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return GatewayIdempotencyBeginResult{}, err
	}

	record, err = scanGatewayIdempotencyRecord(tx.QueryRow(ctx, `
		select `+gatewayIdempotencyColumns+`
		from gateway_idempotency_records
		where owner_user_id = $1 and key_hash = $2
		for update
	`, input.OwnerUserID, input.KeyHash))
	if err != nil {
		return GatewayIdempotencyBeginResult{}, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `select now()`).Scan(&now); err != nil {
		return GatewayIdempotencyBeginResult{}, fmt.Errorf("read database time for gateway idempotency: %w", err)
	}
	if record.ExpiresAt.After(now) && record.RequestHash != input.RequestHash {
		if err := tx.Commit(ctx); err != nil {
			return GatewayIdempotencyBeginResult{}, err
		}
		return GatewayIdempotencyBeginResult{Action: GatewayIdempotencyConflict, Record: record}, nil
	}
	if record.ExpiresAt.After(now) && record.State == "completed" && record.Replayable {
		if record.StatusCode == nil {
			return GatewayIdempotencyBeginResult{}, errors.New("replayable gateway idempotency record has no status code")
		}
		if err := tx.Commit(ctx); err != nil {
			return GatewayIdempotencyBeginResult{}, err
		}
		return GatewayIdempotencyBeginResult{Action: GatewayIdempotencyReplay, Record: record}, nil
	}
	if record.ExpiresAt.After(now) && record.State == "in_progress" && record.LeaseExpiresAt != nil && record.LeaseExpiresAt.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return GatewayIdempotencyBeginResult{}, err
		}
		return GatewayIdempotencyBeginResult{Action: GatewayIdempotencyWait, Record: record}, nil
	}

	record, err = scanGatewayIdempotencyRecord(tx.QueryRow(ctx, `
		update gateway_idempotency_records
		set request_hash = $3,
		    request_id = $4,
		    generation = generation + 1,
		    state = 'in_progress',
		    lease_token = $5,
		    lease_expires_at = now() + make_interval(secs => $6),
		    status_code = null,
		    response_headers = null,
		    response_body = null,
		    replayable = false,
		    failure_reason = null,
		    started_at = now(),
		    completed_at = null,
		    updated_at = now(),
		    expires_at = now() + make_interval(secs => $7)
		where owner_user_id = $1 and key_hash = $2
		returning `+gatewayIdempotencyColumns,
		input.OwnerUserID,
		input.KeyHash,
		input.RequestHash,
		input.RequestID,
		input.LeaseToken,
		input.LeaseDuration.Seconds(),
		input.RecordTTL.Seconds(),
	))
	if err != nil {
		return GatewayIdempotencyBeginResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return GatewayIdempotencyBeginResult{}, err
	}
	return GatewayIdempotencyBeginResult{Action: GatewayIdempotencyExecute, Record: record}, nil
}

func (s *Store) RenewGatewayIdempotencyLease(ctx context.Context, ownerUserID int64, keyHash, leaseToken string, leaseDuration, recordTTL time.Duration) (bool, error) {
	if ownerUserID <= 0 || strings.TrimSpace(keyHash) == "" || strings.TrimSpace(leaseToken) == "" || leaseDuration <= 0 || recordTTL <= 0 {
		return false, errors.New("invalid gateway idempotency lease renewal")
	}
	command, err := s.pool.Exec(ctx, `
		update gateway_idempotency_records
		set lease_expires_at = now() + make_interval(secs => $4),
		    expires_at = greatest(expires_at, now() + make_interval(secs => $5)),
		    updated_at = now()
		where owner_user_id = $1 and key_hash = $2 and lease_token = $3 and state = 'in_progress'
	`, ownerUserID, keyHash, leaseToken, leaseDuration.Seconds(), recordTTL.Seconds())
	if err != nil {
		return false, err
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) CompleteGatewayIdempotency(ctx context.Context, completion GatewayIdempotencyCompletion) (bool, error) {
	if completion.OwnerUserID <= 0 || strings.TrimSpace(completion.KeyHash) == "" || strings.TrimSpace(completion.LeaseToken) == "" || completion.StatusCode < 100 || completion.RecordTTL <= 0 {
		return false, errors.New("invalid gateway idempotency completion")
	}
	headers := completion.ResponseHeaders
	if len(headers) == 0 {
		headers = json.RawMessage(`{}`)
	}
	command, err := s.pool.Exec(ctx, `
		update gateway_idempotency_records
		set state = 'completed',
		    lease_token = null,
		    lease_expires_at = null,
		    status_code = $4,
		    response_headers = $5,
		    response_body = $6,
		    replayable = true,
		    failure_reason = null,
		    completed_at = now(),
		    updated_at = now(),
		    expires_at = now() + make_interval(secs => $7)
		where owner_user_id = $1 and key_hash = $2 and lease_token = $3 and state = 'in_progress'
	`, completion.OwnerUserID, completion.KeyHash, completion.LeaseToken, completion.StatusCode, headers, completion.ResponseBody, completion.RecordTTL.Seconds())
	if err != nil {
		return false, err
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) FailGatewayIdempotency(ctx context.Context, ownerUserID int64, keyHash, leaseToken, reason string, recordTTL time.Duration) (bool, error) {
	if ownerUserID <= 0 || strings.TrimSpace(keyHash) == "" || strings.TrimSpace(leaseToken) == "" || recordTTL <= 0 {
		return false, errors.New("invalid gateway idempotency failure")
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > 128 {
		reason = reason[:128]
	}
	command, err := s.pool.Exec(ctx, `
		update gateway_idempotency_records
		set state = 'failed',
		    lease_token = null,
		    lease_expires_at = null,
		    status_code = null,
		    response_headers = null,
		    response_body = null,
		    replayable = false,
		    failure_reason = $4,
		    completed_at = now(),
		    updated_at = now(),
		    expires_at = now() + make_interval(secs => $5)
		where owner_user_id = $1 and key_hash = $2 and lease_token = $3 and state = 'in_progress'
	`, ownerUserID, keyHash, leaseToken, reason, recordTTL.Seconds())
	if err != nil {
		return false, err
	}
	return command.RowsAffected() == 1, nil
}

func (s *Store) DeleteExpiredGatewayIdempotencyRecords(ctx context.Context, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	command, err := s.pool.Exec(ctx, `
		with expired as (
			select ctid
			from gateway_idempotency_records
			where expires_at <= now()
			  and (state <> 'in_progress' or lease_expires_at is null or lease_expires_at <= now())
			order by expires_at
			limit $1
		)
		delete from gateway_idempotency_records records
		using expired
		where records.ctid = expired.ctid
	`, limit)
	if err != nil {
		return 0, err
	}
	return command.RowsAffected(), nil
}

func validateGatewayIdempotencyBegin(input GatewayIdempotencyBegin) error {
	if input.OwnerUserID <= 0 {
		return errors.New("gateway idempotency owner user id must be positive")
	}
	if len(input.KeyHash) != 64 || len(input.RequestHash) != 64 {
		return errors.New("gateway idempotency hashes must be SHA-256 hex strings")
	}
	if strings.TrimSpace(input.RequestID) == "" || len(input.RequestID) > 128 {
		return errors.New("gateway idempotency request id must contain 1 to 128 characters")
	}
	if strings.TrimSpace(input.LeaseToken) == "" || len(input.LeaseToken) > 64 {
		return errors.New("gateway idempotency lease token must contain 1 to 64 characters")
	}
	if input.LeaseDuration <= 0 || input.RecordTTL <= 0 {
		return errors.New("gateway idempotency durations must be positive")
	}
	return nil
}

func scanGatewayIdempotencyRecord(row gatewayIdempotencyRowScanner) (GatewayIdempotencyRecord, error) {
	var record GatewayIdempotencyRecord
	err := row.Scan(
		&record.OwnerUserID,
		&record.KeyHash,
		&record.RequestHash,
		&record.RequestID,
		&record.Generation,
		&record.State,
		&record.LeaseToken,
		&record.LeaseExpiresAt,
		&record.StatusCode,
		&record.ResponseHeaders,
		&record.ResponseBody,
		&record.Replayable,
		&record.FailureReason,
		&record.CreatedAt,
		&record.StartedAt,
		&record.CompletedAt,
		&record.UpdatedAt,
		&record.ExpiresAt,
	)
	if err != nil {
		return GatewayIdempotencyRecord{}, fmt.Errorf("scan gateway idempotency record: %w", err)
	}
	return record, nil
}
