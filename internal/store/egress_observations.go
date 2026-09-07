package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/egress"
)

const createEgressObservations = `create table if not exists gateway_egress_observations (
	attempt_id varchar(32) primary key,
	request_id varchar(128) not null,
	token_id bigint not null,
	started_at timestamptz not null,
	trace_id varchar(32) not null,
	observation jsonb not null,
	created_at timestamptz not null default now(),
	check (octet_length(observation::text) <= 24576)
);
create index if not exists ix_egress_observations_request on gateway_egress_observations(request_id, started_at);
create index if not exists ix_egress_observations_retention on gateway_egress_observations(created_at);
create index if not exists ix_egress_observations_token on gateway_egress_observations(token_id, started_at desc)`

func (s *Store) GetEgressPolicy(ctx context.Context) (egress.Policy, error) {
	setting, err := s.GetSetting(ctx, egress.PolicyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return egress.Policy{}, nil
	}
	if err != nil {
		return egress.Policy{}, err
	}
	var p egress.Policy
	if err := json.Unmarshal(setting.Value, &p); err != nil {
		return p, err
	}
	p.UpdatedAt = setting.UpdatedAt
	return p, p.Validate()
}

func (s *Store) UpdateEgressPolicy(ctx context.Context, p egress.Policy) (egress.Policy, error) {
	if err := p.Validate(); err != nil {
		return p, err
	}
	p.UpdatedAt = time.Time{}
	raw, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	setting, err := s.UpsertSetting(ctx, egress.PolicyKey, raw)
	if err != nil {
		return p, err
	}
	p.UpdatedAt = setting.UpdatedAt
	return p, nil
}

func (s *Store) SaveEgressObservation(ctx context.Context, record egress.TraceRecord, data []byte) error {
	if len(data) > egress.MaxTraceBytes {
		return errors.New("egress observation exceeds byte limit")
	}
	_, err := s.PoolFor(WorkloadAnalytics).Exec(ctx, `insert into gateway_egress_observations(attempt_id, request_id, token_id, started_at, trace_id, observation)
		values($1,$2,$3,$4,$5,$6) on conflict(attempt_id) do nothing`, record.AttemptID, record.RequestID, record.TokenID, record.StartedAt, record.TraceID, json.RawMessage(data))
	return err
}

func (s *Store) ListEgressObservations(ctx context.Context, requestID string) ([]json.RawMessage, error) {
	rows, err := s.PoolFor(WorkloadAnalytics).Query(ctx, `select observation from gateway_egress_observations where request_id=$1 order by started_at limit 100`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []json.RawMessage{}
	for rows.Next() {
		var item json.RawMessage
		if err := rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// Independent diagnostic retention; never touches request logs or accounting.
func (s *Store) CleanupEgressObservations(ctx context.Context) error {
	_, err := s.PoolFor(WorkloadAnalytics).Exec(ctx, `delete from gateway_egress_observations where attempt_id in (
		select attempt_id from gateway_egress_observations where created_at < now()-interval '24 hours'
		order by created_at limit 1000 for update skip locked)`)
	return err
}
