package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Recovery401Candidate carries the exact failed state. Completion uses it as
// a compare-and-swap fence so imports, manual disable, and other workers win.
type Recovery401Candidate struct {
	Token         Token
	SourceEventID int64
	Name          string
}

const recovery401RetryPredicate = `
 and not exists (select 1 from token_state_events a where a.token_id=t.id
  and a.event_type='oauth_401_recovery_started' and a.created_at>now()-interval '15 minutes')
 and (select count(*) from token_state_events a where a.token_id=t.id
  and a.event_type='oauth_401_recovery_started' and a.created_at>now()-interval '24 hours')<3
 and not exists(select 1 from token_state_events a where a.token_id=t.id
  and a.event_type='oauth_401_recovery_result' and a.created_at>now()-interval '24 hours'
  and a.reason in ('account_deactivated','workspace_unavailable','workspace_record_not_found','email_mismatch','workspace_mismatch','subscription_ineligible'))
`

// Only a current 401 event is recoverable. Manual disabling updates disabled_at
// independently of that event and therefore opts an account out. Attempts are
// persisted in existing state events, including failures, to survive restarts.
func (s *Store) ListRecovery401Candidates(ctx context.Context) ([]int64, error) {
	rows, err := s.PoolFor(WorkloadWorker).Query(ctx, `
 select t.id from codex_tokens t
 join lateral (
  select e.id,e.status_code,e.reason,e.created_at from token_state_events e
  where e.token_id=t.id and e.event_type in ('disabled','error')
  order by e.id desc limit 1
 ) e on e.status_code=401 and e.reason=t.last_error
 where t.merged_into_token_id is null
 and t.plan_type='self_serve_business_prolite'
 and (t.is_active or t.disabled_at=e.created_at)
 and coalesce(t.email,'')<>'' and coalesce(t.account_id,'')<>''
 and not exists (select 1 from token_agent_identities a where a.token_id=t.id)
 and lower(e.reason) not like '%account_deactivated%'
 `+recovery401RetryPredicate+`
 order by e.id desc limit 16`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) BeginRecovery401(ctx context.Context, id int64) (*Recovery401Candidate, error) {
	var candidate *Recovery401Candidate
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var eventID int64
		// Row lock plus a durable attempt marker prevent concurrent submissions.
		err := tx.QueryRow(ctx, `
   select t.id from codex_tokens t where t.id=$1 and t.merged_into_token_id is null for update`, id).Scan(&eventID)
		if err != nil {
			return err
		}
		var name string
		err = tx.QueryRow(ctx, `
   select e.id,coalesce(nullif(t.raw_payload->>'name',''),nullif(t.remark,''),'')
   from codex_tokens t join lateral (
    select e.id,e.status_code,e.reason,e.created_at from token_state_events e
    where e.token_id=t.id and e.event_type in ('disabled','error') order by e.id desc limit 1
   ) e on e.status_code=401 and e.reason=t.last_error
   where t.id=$1 and t.plan_type='self_serve_business_prolite'
   and (t.is_active or t.disabled_at=e.created_at)
   and lower(e.reason) not like '%account_deactivated%'
   and not exists(select 1 from token_agent_identities a where a.token_id=t.id)
 `+recovery401RetryPredicate, id).Scan(&eventID, &name)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `select id,coalesce(owner_user_id,0),email,account_id,access_token,refresh_token,plan_type,remark,source_file,
    is_active,cooldown_until,disabled_at,share_enabled,share_status,share_disabled_reason,share_enabled_at,share_disabled_at,
    marketplace_price_bps,marketplace_price_updated_at,marketplace_price_source,last_used_at,last_error,created_at,updated_at,
    codex_fingerprint_enabled,active_stream_cap_override from codex_tokens where id=$1`, id)
		token, err := scanTokenWithSharing(row)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into token_state_events(token_id,owner_user_id,event_type,reason,status_code,metadata)
   values($1,$2,'oauth_401_recovery_started','website OAuth recovery started',401,$3)`, id, token.OwnerUserID, jsonBytes(map[string]any{"source_event_id": eventID}))
		if err != nil {
			return err
		}
		candidate = &Recovery401Candidate{Token: token, SourceEventID: eventID, Name: name}
		return nil
	})
	return candidate, err
}

func (s *Store) RecordRecovery401Result(ctx context.Context, c Recovery401Candidate, outcome, reason string) error {
	_, err := s.PoolFor(WorkloadWorker).Exec(ctx, `insert into token_state_events(token_id,owner_user_id,event_type,reason,metadata)
 values($1,$2,'oauth_401_recovery_result',$3,$4)`, c.Token.ID, c.Token.OwnerUserID, reason, jsonBytes(map[string]any{"source_event_id": c.SourceEventID, "outcome": outcome}))
	return err
}

// CommitRecovery401 installs only already-probed credentials. State, secrets,
// refresh history, and recovery evidence are committed in one transaction.
func (s *Store) CommitRecovery401(ctx context.Context, c Recovery401Candidate, u TokenSecretUpdate) error {
	if u.AccessToken == "" || u.RefreshToken == "" || u.ExpiresAt == nil {
		return errors.New("incomplete recovered credentials")
	}
	return s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		t := c.Token
		tag, err := tx.Exec(ctx, `update codex_tokens set access_token=$2,refresh_token=$3,id_token=nullif($4,''),expired=$5,
    last_refresh=now(),is_active=true,disabled_at=null,cooldown_until=null,last_error=null,updated_at=now(),
    raw_payload=(coalesce(raw_payload::jsonb,'{}'::jsonb)-'access_token'-'refresh_token'-'id_token') || $14::jsonb
    where id=$1 and merged_into_token_id is null and owner_user_id=$6
    and is_active=$7 and disabled_at is not distinct from $8::timestamptz
    and cooldown_until is not distinct from $9::timestamptz and last_error is not distinct from $10::text
    and coalesce(access_token,'')=$11 and coalesce(refresh_token,'')=$12 and account_id=$13
    and plan_type='self_serve_business_prolite' and email=$15 and updated_at=$16`, t.ID, u.AccessToken, u.RefreshToken, u.IDToken, u.ExpiresAt,
			t.OwnerUserID, t.IsActive, t.DisabledAt, t.CooldownUntil, t.LastError, t.AccessToken, t.RefreshToken, stringPtrValue(t.AccountID),
			jsonBytes(map[string]any{"access_token": u.AccessToken, "refresh_token": u.RefreshToken, "id_token": u.IDToken}), stringPtrValue(t.Email), t.UpdatedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrTokenStateChanged
		}
		if err := recordTokenSecretAndRefreshHistory(ctx, tx, t.OwnerUserID, t.ID, u.AccessToken, u.RefreshToken, nullableString(u.IDToken)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into token_runtime_state(token_id,owner_user_id,active_streams,failure_streak,last_success_at,updated_at)
    values($1,$2,0,0,now(),now()) on conflict(token_id) do update
    set cooldown_until=null,disabled_reason=null,failure_streak=0,last_success_at=now(),updated_at=now()`, t.ID, t.OwnerUserID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `insert into token_state_events(token_id,owner_user_id,event_type,reason,status_code,previous_is_active,next_is_active,metadata)
    values($1,$2,'oauth_401_recovered','website OAuth credentials verified by response.completed',200,$3,true,$4)`, t.ID, t.OwnerUserID, t.IsActive, jsonBytes(map[string]any{"source_event_id": c.SourceEventID, "model": QuotaRecoveryModel}))
		return err
	})
}
