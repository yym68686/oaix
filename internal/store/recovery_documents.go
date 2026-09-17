package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/yym68686/oaix/internal/recovery"
)

const createRecoveryDocuments = `create table if not exists recovery_documents (
 id bigserial primary key,
 owner_user_id bigint not null references platform_users(id) on delete cascade,
 original_sha256 text not null,
 original_ciphertext text not null,
 latest_ciphertext text,
 session_ciphertext text,
 created_at timestamptz not null default now(),
 updated_at timestamptz not null default now(),
 unique(owner_user_id,original_sha256)
)`
const createRecoveryDocumentAccounts = `create table if not exists recovery_document_accounts (
 document_id bigint not null references recovery_documents(id) on delete cascade,
 email text not null,
 account_id text not null,
 primary key(document_id,email,account_id)
)`
const createRecoveryDocumentBindings = `create table if not exists token_recovery_documents (
 token_id integer primary key references codex_tokens(id) on delete cascade,
 document_id bigint not null references recovery_documents(id),
 owner_user_id bigint not null references platform_users(id) on delete cascade,
 created_at timestamptz not null default now(),
 updated_at timestamptz not null default now()
)`

type RecoveryDocument struct {
	ID          int64
	OwnerUserID int64
	Raw         []byte
	Latest      []byte
	Session     recovery.SignedSession
}

func (s *Store) encryptRecoveryBytes(raw []byte) (string, error) {
	return s.apiKeyCipher.Encrypt("oaix-recovery:v1:" + base64.StdEncoding.EncodeToString(raw))
}
func (s *Store) decryptRecoveryBytes(ciphertext string) ([]byte, error) {
	text, err := s.apiKeyCipher.Decrypt(ciphertext)
	if err != nil {
		return nil, errors.New("recovery document decryption failed")
	}
	const prefix = "oaix-recovery:v1:"
	if !strings.HasPrefix(text, prefix) {
		return nil, errors.New("invalid recovery document ciphertext")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(text, prefix))
	if err != nil {
		return nil, errors.New("invalid recovery document encoding")
	}
	return raw, nil
}

func (s *Store) SaveRecoveryDocument(ctx context.Context, owner int64, raw []byte) (int64, error) {
	if owner <= 0 {
		return 0, errors.New("recovery document owner is required")
	}
	doc, err := recovery.ParseSignedDocument(raw)
	if err != nil {
		return 0, err
	}
	encrypted, err := s.encryptRecoveryBytes(raw)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `insert into recovery_documents(owner_user_id,original_sha256,original_ciphertext) values($1,$2,$3)
   on conflict(owner_user_id,original_sha256) do update set original_sha256=excluded.original_sha256 returning id`, owner, hashString(string(raw)), encrypted).Scan(&id); err != nil {
			return err
		}
		for _, a := range doc.Accounts {
			email, account, _ := a.Identity()
			if _, err := tx.Exec(ctx, `insert into recovery_document_accounts(document_id,email,account_id) values($1,$2,$3) on conflict do nothing`, id, strings.ToLower(email), account); err != nil {
				return err
			}
		}
		return nil
	})
	return id, err
}

func bindRecoveryDocument(ctx context.Context, tx pgx.Tx, owner int64, token Token, documentID int64) error {
	var matched int64
	if err := tx.QueryRow(ctx, `select d.id from recovery_documents d join recovery_document_accounts a on a.document_id=d.id
 where d.id=$1 and d.owner_user_id=$2 and a.email=lower($3) and a.account_id=$4`, documentID, owner, stringPtrValue(token.Email), stringPtrValue(token.AccountID)).Scan(&matched); err != nil {
		return errors.New("recovery document does not match account owner and identity")
	}
	_, err := tx.Exec(ctx, `insert into token_recovery_documents(token_id,document_id,owner_user_id) values($1,$2,$3)
 on conflict(token_id) do update set document_id=excluded.document_id,owner_user_id=excluded.owner_user_id,updated_at=now()`, token.ID, documentID, owner)
	return err
}

func (s *Store) BindRecoveryDocument(ctx context.Context, owner, tokenID, documentID int64) error {
	token, err := s.GetTokenScoped(ctx, OwnerResources(owner), tokenID)
	if err != nil {
		return err
	}
	return s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return bindRecoveryDocument(ctx, tx, owner, *token, documentID)
	})
}

func (s *Store) RecoveryDocumentForToken(ctx context.Context, owner, tokenID int64) (*RecoveryDocument, error) {
	var d RecoveryDocument
	var original, latest, session string
	err := s.PoolFor(WorkloadWorker).QueryRow(ctx, `select d.id,d.owner_user_id,d.original_ciphertext,coalesce(d.latest_ciphertext,''),coalesce(d.session_ciphertext,'')
 from token_recovery_documents b join recovery_documents d on d.id=b.document_id
 join codex_tokens t on t.id=b.token_id and t.owner_user_id=b.owner_user_id
 where b.token_id=$1 and b.owner_user_id=$2 and d.owner_user_id=$2`, tokenID, owner).Scan(&d.ID, &d.OwnerUserID, &original, &latest, &session)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d.Raw, err = s.decryptRecoveryBytes(original)
	if err != nil {
		return nil, err
	}
	if latest != "" {
		d.Latest, err = s.decryptRecoveryBytes(latest)
		if err != nil {
			return nil, err
		}
	}
	if session != "" {
		raw, err := s.decryptRecoveryBytes(session)
		if err != nil {
			return nil, err
		}
		if json.Unmarshal(raw, &d.Session) != nil {
			return nil, errors.New("invalid recovery session")
		}
	}
	return &d, nil
}

func (s *Store) SaveRecoverySession(ctx context.Context, d *RecoveryDocument, session recovery.SignedSession) error {
	raw, err := json.Marshal(session)
	if err != nil {
		return err
	}
	cipher, err := s.encryptRecoveryBytes(raw)
	if err != nil {
		return err
	}
	tag, err := s.PoolFor(WorkloadWorker).Exec(ctx, `update recovery_documents set session_ciphertext=$3,updated_at=now() where id=$1 and owner_user_id=$2`, d.ID, d.OwnerUserID, cipher)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	d.Session = session
	return nil
}

func (s *Store) SaveRecoveryDownload(ctx context.Context, d *RecoveryDocument, raw []byte) error {
	doc, err := recovery.ParseSignedDocument(raw)
	if err != nil {
		return err
	}
	original, err := recovery.ParseSignedDocument(d.Raw)
	if err != nil {
		return err
	}
	identities := make(map[string]bool, len(original.Accounts))
	for _, account := range original.Accounts {
		email, id, _ := account.Identity()
		identities[strings.ToLower(email)+"\x00"+id] = true
	}
	for _, account := range doc.Accounts {
		email, id, _ := account.Identity()
		if !identities[strings.ToLower(email)+"\x00"+id] {
			return errors.New("download contains an unexpected recovery identity")
		}
	}

	cipher, err := s.encryptRecoveryBytes(raw)
	if err != nil {
		return err
	}
	_, err = s.PoolFor(WorkloadWorker).Exec(ctx, `update recovery_documents set latest_ciphertext=$3,updated_at=now() where id=$1 and owner_user_id=$2`, d.ID, d.OwnerUserID, cipher)
	return err
}

// A separate advisory namespace serializes a signed bundle across accounts.
func (s *Store) TryRecoveryDocumentLease(ctx context.Context, id int64) (func(), bool, error) {
	if id <= 0 || id > int64(^uint32(0)>>1) {
		return nil, false, fmt.Errorf("invalid recovery document id")
	}
	conn, err := s.PoolFor(WorkloadWorker).Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	const namespace int32 = 0x52454344
	var locked bool
	if err := conn.QueryRow(ctx, `select pg_try_advisory_lock($1,$2)`, namespace, int32(id)).Scan(&locked); err != nil {
		discardQuotaRecoveryLeaseConn(conn)
		return nil, false, err
	}
	if !locked {
		conn.Release()
		return nil, false, nil
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var ok bool
		if err := conn.QueryRow(releaseCtx, `select pg_advisory_unlock($1,$2)`, namespace, int32(id)).Scan(&ok); err != nil || !ok {
			discardQuotaRecoveryLeaseConn(conn)
			return
		}
		conn.Release()
	}, true, nil
}

type RecoveryStatus struct {
	Provider      string     `json:"provider"`
	Status        string     `json:"status"`
	Reason        string     `json:"reason,omitempty"`
	HasSignedFile bool       `json:"has_signed_file"`
	UpdatedAt     *time.Time `json:"updated_at,omitempty"`
	NextRetryAt   *time.Time `json:"next_retry_at,omitempty"`
}

func (s *Store) RecoveryStatuses(ctx context.Context, tokens []Token) (map[int64]RecoveryStatus, error) {
	ids := make([]int64, 0, len(tokens))
	for _, t := range tokens {
		if stringPtrValue(t.PlanType) == recovery.EligiblePlan {
			ids = append(ids, t.ID)
		}
	}
	out := map[int64]RecoveryStatus{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.PoolFor(WorkloadAdmin).Query(ctx, `select t.id,b.document_id is not null,
 coalesce(e.event_type,''),coalesce(e.reason,''),e.created_at,t.is_active,
 coalesce(t.last_error,''),coalesce(e.metadata->>'outcome',''),retry.next_at,trigger.created_at
 from codex_tokens t left join token_recovery_documents b on b.token_id=t.id and b.owner_user_id=t.owner_user_id
 left join lateral(select created_at from token_state_events where token_id=t.id and event_type in ('disabled','error') and status_code=401 order by created_at desc,id desc limit 1) trigger on true
 left join lateral (select event_type,reason,created_at,metadata from token_state_events e where e.token_id=t.id
 and e.event_type in ('oauth_401_recovery_started','oauth_401_recovery_result','oauth_401_recovered')
 and coalesce(e.metadata->>'provider','5xteam')=case when b.document_id is null then '5xteam' else 'signed' end
 and coalesce(e.metadata->>'document_id','0')=coalesce(b.document_id,0)::text
 order by e.created_at desc,e.id desc limit 1)e on true
 left join lateral (
  select greatest(
   max(created_at) filter(where event_type='oauth_401_recovery_started')+interval '15 minutes',
   case when count(*) filter(where event_type='oauth_401_recovery_started')>=3 then min(created_at) filter(where event_type='oauth_401_recovery_started')+interval '24 hours' end,
   max(created_at) filter(where event_type='oauth_401_recovery_result' and reason in ('account_deactivated','workspace_unavailable','workspace_record_not_found','email_mismatch','workspace_mismatch','subscription_ineligible','submission_uncertain','signed_http_400','signed_http_403','signed_http_404','signed_task_failed','target_not_recovered','signed_document_ineligible_plan'))+interval '24 hours'
  ) as next_at from token_state_events x where x.token_id=t.id and x.created_at>now()-interval '24 hours'
  and coalesce(x.metadata->>'provider','5xteam')=case when b.document_id is null then '5xteam' else 'signed' end
  and coalesce(x.metadata->>'document_id','0')=coalesce(b.document_id,0)::text
 ) retry on true where t.id=any($1::integer[])`, postgresIntIDs(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var r RecoveryStatus
		var event, lastError, outcome string
		var active bool
		var triggerAt *time.Time
		if err := rows.Scan(&id, &r.HasSignedFile, &event, &r.Reason, &r.UpdatedAt, &active, &lastError, &outcome, &r.NextRetryAt, &triggerAt); err != nil {
			return nil, err
		}
		r.Provider = "5xteam"
		if r.HasSignedFile {
			r.Provider = "zzledu"
		}
		switch event {
		case "oauth_401_recovery_started":
			r.Status = "running"
			if r.UpdatedAt != nil && time.Since(*r.UpdatedAt) > 6*time.Minute {
				r.Status = "interrupted"
			}
		case "oauth_401_recovered":
			r.Status = "recovered"
		case "oauth_401_recovery_result":
			r.Status = outcome
		default:
			r.Status = "idle"
			if !active && strings.Contains(lastError, "401") {
				r.Status = "pending"
			}
		}
		if !active && strings.Contains(lastError, "401") && triggerAt != nil && (r.UpdatedAt == nil || triggerAt.After(*r.UpdatedAt)) {
			r.Status = "pending"
			r.Reason = ""
		}
		if active || r.Status == "recovered" {
			r.NextRetryAt = nil
		}
		out[id] = r
	}
	return out, rows.Err()
}
