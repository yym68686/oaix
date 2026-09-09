package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yym68686/oaix/internal/admindiag"
)

const createAdminObservations = `create table if not exists gateway_admin_observations (
 id varchar(32) primary key,
 request_id varchar(128) not null,
 started_at timestamptz not null,
 observation jsonb not null,
 created_at timestamptz not null default now(),
 check(octet_length(observation::text)<=49152)
);
create index if not exists ix_admin_observations_request on gateway_admin_observations(request_id,started_at);
create index if not exists ix_admin_observations_retention on gateway_admin_observations(created_at)`

func (s *Store) AdminRecorder() *admindiag.Recorder { return s.adminRecorder }
func diagnosticPool(ctx context.Context, base *pgxpool.Config, readOnly bool) (*pgxpool.Pool, error) {
	cfg := base.Copy()
	cfg.MinConns = 0
	cfg.MaxConns = 1
	cfg.ConnConfig.Tracer = nil
	cfg.AfterConnect = nil
	cfg.BeforeClose = nil
	cfg.ConnConfig.ConnectTimeout = time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "oaix-admin-diagnostics"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "900ms"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "50ms"
	if readOnly {
		cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "100ms"
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}
func (s *Store) GetAdminPolicy(ctx context.Context) (admindiag.Policy, error) {
	var raw []byte
	var at time.Time
	err := s.diagnosticWriter.QueryRow(ctx, `select value,updated_at from gateway_settings where key=$1`, admindiag.PolicyKey).Scan(&raw, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return admindiag.Policy{}, nil
	}
	if err != nil {
		return admindiag.Policy{}, err
	}
	var p admindiag.Policy
	if err = json.Unmarshal(raw, &p); err != nil {
		return p, err
	}
	p.UpdatedAt = at
	return p, p.Validate()
}
func (s *Store) UpdateAdminPolicy(ctx context.Context, p admindiag.Policy) (admindiag.Policy, error) {
	if err := p.Validate(); err != nil {
		return p, err
	}
	p.UpdatedAt = time.Time{}
	raw, _ := json.Marshal(p)
	err := s.diagnosticWriter.QueryRow(ctx, `insert into gateway_settings(key,value,updated_at) values($1,$2,clock_timestamp()) on conflict(key) do update set value=excluded.value,updated_at=greatest(clock_timestamp(),gateway_settings.updated_at+interval '1 microsecond') returning updated_at`, admindiag.PolicyKey, raw).Scan(&p.UpdatedAt)
	return p, err
}
func (s *Store) SaveAdminObservation(ctx context.Context, d admindiag.Record, raw []byte) error {
	if len(raw) > admindiag.MaxBytes {
		return errors.New("observation exceeds size budget")
	}
	_, err := s.diagnosticWriter.Exec(ctx, `insert into gateway_admin_observations(id,request_id,started_at,observation) values($1,$2,$3,$4) on conflict(id) do nothing`, d.ID, d.RequestID, d.StartedAt, json.RawMessage(raw))
	return err
}
func (s *Store) ListAdminObservations(ctx context.Context, id string) ([]json.RawMessage, error) {
	rows, err := s.diagnosticWriter.Query(ctx, `select observation from gateway_admin_observations where request_id=$1 order by started_at limit 100`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var raw json.RawMessage
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		out = append(out, raw)
	}
	return out, rows.Err()
}
func (s *Store) CleanupAdminObservations(ctx context.Context) error {
	_, err := s.diagnosticWriter.Exec(ctx, `delete from gateway_admin_observations where id in (select id from gateway_admin_observations where created_at<now()-interval '24 hours' order by created_at limit 1000 for update skip locked)`)
	return err
}

// One SELECT, read-only connection, at most eight registered backend PIDs.
// Match query text in memory (never export it) and retain backend_start to
// distinguish PID reuse. A reused connection running another SQL is a miss.
func (s *Store) SampleAdminQueries(ctx context.Context) {
	targets := s.adminRecorder.Targets()
	if len(targets) == 0 {
		return
	}
	ids := make([]int32, 0, len(targets))
	for _, t := range targets {
		ids = append(ids, int32(t.PID))
	}
	c, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	rows, err := s.diagnosticReader.Query(c, `select pid,backend_start,query_start,coalesce(state,''),coalesce(wait_event_type,''),coalesce(wait_event,''),coalesce(query_id::text,''),query,case when wait_event_type='Lock' then pg_blocking_pids(pid) else '{}'::integer[] end from pg_stat_activity where datname=current_database() and pid=any($1::integer[])`, ids)
	type entry struct {
		sample admindiag.Sample
		sql    string
	}
	found := map[uint32]entry{}
	if err == nil {
		for rows.Next() {
			var pid uint32
			var e entry
			err = rows.Scan(&pid, &e.sample.BackendStart, &e.sample.QueryStart, &e.sample.State, &e.sample.WaitType, &e.sample.Wait, &e.sample.PGQueryID, &e.sql, &e.sample.Blockers)
			if err != nil {
				break
			}
			if len(e.sample.Blockers) > 8 {
				e.sample.Blockers = e.sample.Blockers[:8]
			}
			found[pid] = e
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
	}
	for _, t := range targets {
		e, ok := found[t.PID]
		t.Add(e.sample, ok && e.sql == t.SQL, err != nil)
	}
}
