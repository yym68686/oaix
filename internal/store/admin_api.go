package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type TokenMetadataUpdate struct {
	TokenID    int64
	Remark     *string
	PlanType   *string
	Email      *string
	AccountID  *string
	Source     *string
	SourceFile *string
	IsActive   *bool
}

type TokenRefreshHistoryItem struct {
	ID               int64     `json:"id"`
	TokenID          int64     `json:"token_id"`
	RefreshTokenHash string    `json:"refresh_token_hash"`
	SeenAt           time.Time `json:"seen_at"`
}

type ImportJobListOptions struct {
	Limit  int
	Offset int
	Status string
	Source string
	From   *time.Time
	To     *time.Time
	Sort   string
}

type ImportItemListOptions struct {
	Limit  int
	Offset int
	Status string
	Action string
	Error  string
}

type AuditLog struct {
	ID         int64           `json:"id"`
	Action     string          `json:"action"`
	Actor      *string         `json:"actor,omitempty"`
	TargetType *string         `json:"target_type,omitempty"`
	TargetID   *string         `json:"target_id,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

type AuditLogListOptions struct {
	Limit      int
	Offset     int
	Action     string
	Actor      string
	TargetType string
	TargetID   string
	From       *time.Time
	To         *time.Time
}

type AdminAPIKey struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	Role       string     `json:"role"`
	KeyHash    string     `json:"key_hash,omitempty"`
	CreatedBy  *string    `json:"created_by,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

type CreatedAdminAPIKey struct {
	AdminAPIKey
	PlaintextKey string `json:"plaintext_key"`
}

type OAuthSession struct {
	SessionID           string    `json:"session_id"`
	OwnerUserID         int64     `json:"owner_user_id"`
	State               string    `json:"state,omitempty"`
	CodeVerifier        string    `json:"-"`
	RedirectURI         string    `json:"redirect_uri"`
	ImportQueuePosition string    `json:"import_queue_position"`
	Status              string    `json:"status"`
	ImportJobID         *int64    `json:"import_job_id,omitempty"`
	ErrorMessage        *string   `json:"error_message,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type IdempotencyRecord struct {
	Key         string          `json:"key"`
	Method      string          `json:"method"`
	Path        string          `json:"path"`
	RequestHash string          `json:"request_hash"`
	StatusCode  int             `json:"status_code"`
	Response    json.RawMessage `json:"response"`
	CreatedAt   time.Time       `json:"created_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
}

type QuotaSnapshot struct {
	ID           int64           `json:"id"`
	TokenID      int64           `json:"token_id"`
	Snapshot     json.RawMessage `json:"snapshot,omitempty"`
	PlanType     *string         `json:"plan_type,omitempty"`
	ErrorMessage *string         `json:"error_message,omitempty"`
	FetchedAt    time.Time       `json:"fetched_at"`
}

type PromptAffinityLane struct {
	PromptKeyHash     string          `json:"prompt_key_hash"`
	PrimaryTokenID    *int64          `json:"primary_token_id,omitempty"`
	SecondaryTokenIDs json.RawMessage `json:"secondary_token_ids,omitempty"`
	PolicyVersion     int             `json:"policy_version"`
	ExpiresAt         time.Time       `json:"expires_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

type ResponseOwnerBinding struct {
	ResponseIDHash string    `json:"response_id_hash"`
	TokenID        int64     `json:"token_id"`
	ExpiresAt      time.Time `json:"expires_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (s *Store) UpdateTokenMetadata(ctx context.Context, update TokenMetadataUpdate) (*Token, error) {
	if update.TokenID <= 0 {
		return nil, fmt.Errorf("token id is required")
	}
	sourcePayload := map[string]any{}
	if update.Source != nil && strings.TrimSpace(*update.Source) != "" {
		sourcePayload["source"] = strings.TrimSpace(*update.Source)
	}
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set remark = case when $2::boolean then nullif($3, '') else remark end,
		    plan_type = case when $4::boolean then nullif($5, '') else plan_type end,
		    email = case when $6::boolean then nullif($7, '') else email end,
		    account_id = case when $8::boolean then nullif($9, '') else account_id end,
		    source_file = case when $10::boolean then nullif($11, '') else source_file end,
		    is_active = case when $12::boolean then $13 else is_active end,
		    disabled_at = case when $12::boolean and $13 then null when $12::boolean and not $13 then coalesce(disabled_at, now()) else disabled_at end,
		    raw_payload = case
		      when $14::jsonb <> '{}'::jsonb then coalesce(raw_payload::jsonb, '{}'::jsonb) || $14::jsonb
		      else raw_payload::jsonb
		    end,
		    updated_at = now()
		where id = $1 and merged_into_token_id is null
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at,
		          share_enabled, share_status, share_disabled_reason, share_enabled_at, share_disabled_at,
		          last_used_at, last_error, created_at, updated_at
	`, update.TokenID,
		update.Remark != nil, stringPtrValue(update.Remark),
		update.PlanType != nil, stringPtrValue(update.PlanType),
		update.Email != nil, stringPtrValue(update.Email),
		update.AccountID != nil, stringPtrValue(update.AccountID),
		update.SourceFile != nil, stringPtrValue(update.SourceFile),
		update.IsActive != nil, boolPtrValue(update.IsActive),
		jsonBytes(sourcePayload),
	)
	token, err := scanTokenWithSharing(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) SetTokenCooldown(ctx context.Context, tokenID int64, until time.Time, reason string) (*Token, error) {
	var token Token
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			update codex_tokens
			set cooldown_until = $2,
			    last_error = nullif($3, ''),
			    is_active = true,
			    disabled_at = null,
			    updated_at = now()
			where id = $1 and merged_into_token_id is null
			returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
			          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
		`, tokenID, until, truncate(reason, 4000))
		updated, err := scanToken(row)
		if err != nil {
			return err
		}
		token = updated
		_, err = tx.Exec(ctx, `
			insert into token_state_events(token_id, owner_user_id, event_type, reason, cooldown_until)
			values ($1, nullif($2, 0), 'cooldown_set', $3, $4)
		`, tokenID, token.OwnerUserID, truncate(reason, 4000), until)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) ClearTokenCooldown(ctx context.Context, tokenID int64) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set cooldown_until = null, updated_at = now()
		where id = $1 and merged_into_token_id is null
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
	`, tokenID)
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	_, _ = s.pool.Exec(ctx, `delete from codex_token_scoped_cooldowns where token_id = $1`, tokenID)
	return &token, nil
}

func (s *Store) ClearTokenLastError(ctx context.Context, tokenID int64) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set last_error = null, updated_at = now()
		where id = $1 and merged_into_token_id is null
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
	`, tokenID)
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) ListTokenRefreshHistory(ctx context.Context, tokenID int64, limit int) ([]TokenRefreshHistoryItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		select id, token_id, refresh_token_hash, seen_at
		from token_refresh_history
		where token_id = $1
		order by seen_at desc, id desc
		limit $2
	`, tokenID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []TokenRefreshHistoryItem{}
	for rows.Next() {
		var item TokenRefreshHistoryItem
		if err := rows.Scan(&item.ID, &item.TokenID, &item.RefreshTokenHash, &item.SeenAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) MergeToken(ctx context.Context, tokenID int64, intoTokenID int64) error {
	if tokenID <= 0 || intoTokenID <= 0 || tokenID == intoTokenID {
		return fmt.Errorf("token_id and into_token_id must be different positive ids")
	}
	tag, err := s.pool.Exec(ctx, `
		update codex_tokens
		set merged_into_token_id = $2,
		    is_active = false,
		    disabled_at = coalesce(disabled_at, now()),
		    updated_at = now()
		where id = $1 and merged_into_token_id is null
	`, tokenID, intoTokenID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) UnmergeToken(ctx context.Context, tokenID int64) (*Token, error) {
	row := s.pool.QueryRow(ctx, `
		update codex_tokens
		set merged_into_token_id = null,
		    is_active = true,
		    disabled_at = null,
		    updated_at = now()
		where id = $1
		returning id, coalesce(owner_user_id, 0), email, account_id, access_token, refresh_token, plan_type, remark, source_file,
		          is_active, cooldown_until, disabled_at, last_used_at, last_error, created_at, updated_at
	`, tokenID)
	token, err := scanToken(row)
	if err != nil {
		return nil, err
	}
	return &token, nil
}

func (s *Store) ListImportJobsFiltered(ctx context.Context, opts ImportJobListOptions) ([]ImportJob, int, error) {
	return s.ListImportJobsFilteredScoped(ctx, AllResources(), opts)
}

func (s *Store) ListImportJobsFilteredScoped(ctx context.Context, scope ResourceScope, opts ImportJobListOptions) ([]ImportJob, int, error) {
	if opts.Limit <= 0 || opts.Limit > 500 {
		opts.Limit = 50
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	where, args := importJobWhereScoped(opts, scope)
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from token_import_jobs where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	orderBy := "id desc"
	if strings.EqualFold(opts.Sort, "oldest") || strings.EqualFold(opts.Sort, "id") {
		orderBy = "id asc"
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
		select id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		where `+where+`
		order by `+orderBy+`
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	jobs := []ImportJob{}
	for rows.Next() {
		job, err := scanImportJob(rows)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, job)
	}
	return jobs, total, rows.Err()
}

func importJobWhere(opts ImportJobListOptions) (string, []any) {
	return importJobWhereScoped(opts, AllResources())
}

func importJobWhereScoped(opts ImportJobListOptions, scope ResourceScope) (string, []any) {
	filters := []string{"true"}
	args := []any{}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	filters = append(filters, scope.ownerFilter("owner_user_id", &args))
	if status := strings.TrimSpace(opts.Status); status != "" && status != "all" {
		filters = append(filters, fmt.Sprintf("status = %s", arg(status)))
	}
	if source := strings.TrimSpace(opts.Source); source != "" {
		filters = append(filters, fmt.Sprintf("payloads::text ilike %s", arg("%"+source+"%")))
	}
	if opts.From != nil {
		filters = append(filters, fmt.Sprintf("submitted_at >= %s", arg(*opts.From)))
	}
	if opts.To != nil {
		filters = append(filters, fmt.Sprintf("submitted_at <= %s", arg(*opts.To)))
	}
	return strings.Join(filters, " and "), args
}

func (s *Store) ListImportJobItemsFiltered(ctx context.Context, jobID int64, opts ImportItemListOptions) ([]ImportItem, int, error) {
	return s.ListImportJobItemsFilteredScoped(ctx, AllResources(), jobID, opts)
}

func (s *Store) ListImportJobItemsFilteredScoped(ctx context.Context, scope ResourceScope, jobID int64, opts ImportItemListOptions) ([]ImportItem, int, error) {
	if opts.Limit <= 0 || opts.Limit > 1000 {
		opts.Limit = 100
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	filters := []string{"job_id = $1"}
	args := []any{jobID}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	filters = append(filters, scope.ownerFilter("owner_user_id", &args))
	if status := strings.TrimSpace(opts.Status); status != "" && status != "all" {
		filters = append(filters, fmt.Sprintf("status = %s", arg(status)))
	}
	if action := strings.TrimSpace(opts.Action); action != "" && action != "all" {
		filters = append(filters, fmt.Sprintf("action = %s", arg(action)))
	}
	if errText := strings.TrimSpace(opts.Error); errText != "" {
		filters = append(filters, fmt.Sprintf("coalesce(error_message, '') ilike %s", arg("%"+errText+"%")))
	}
	where := strings.Join(filters, " and ")
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from token_import_items where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
			select `+importItemSelectColumns("")+`
		from token_import_items
		where `+where+`
		order by item_index asc, id asc
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items, err := scanImportItems(rows)
	return items, total, err
}

func (s *Store) RetryImportJob(ctx context.Context, jobID int64) (ImportJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		update token_import_items
		set status = 'queued',
		    error_message = null,
		    validated_payload = null,
		    token_id = null,
		    action = null,
		    matched_existing_token_id = null,
		    publish_attempted = false,
		    publish_skipped_reason = null,
		    reactivated = false,
		    previous_is_active = null,
		    next_is_active = null,
		    previous_disabled_at = null,
		    next_disabled_at = null,
		    refresh_error_code = null,
		    refresh_error_message_excerpt = null,
		    validation_started_at = null,
		    validation_finished_at = null,
		    validation_ms = null,
		    publish_ms = null,
		    published_at = null,
		    updated_at = now()
		where job_id = $1 and status in ('failed', 'skipped', 'canceled')
	`, jobID); err != nil {
		return ImportJob{}, err
	}
	row := tx.QueryRow(ctx, `
		with item_counts as (
			select
				count(*) filter (where status in ('published', 'skipped', 'failed', 'canceled'))::int as processed,
				count(*) filter (where status = 'published' and action = 'created')::int as created,
				count(*) filter (where status = 'published' and action = 'updated')::int as updated,
				count(*) filter (where status = 'skipped')::int as skipped,
				count(*) filter (where status = 'failed')::int as failed
			from token_import_items
			where job_id = $1
		)
		update token_import_jobs
		set status = 'queued',
		    processed_count = item_counts.processed,
		    created_count = item_counts.created,
		    updated_count = item_counts.updated,
		    skipped_count = item_counts.skipped,
		    failed_count = item_counts.failed,
		    last_error = null,
		    heartbeat_at = now(),
		    finished_at = null
		from item_counts
		where id = $1
		returning id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
		          created_count, updated_count, skipped_count, failed_count, last_error,
		          submitted_at, started_at, heartbeat_at, finished_at
	`, jobID)
	job, err := scanImportJob(row)
	if err != nil {
		return ImportJob{}, err
	}
	return job, tx.Commit(ctx)
}

func (s *Store) RetryImportJobScoped(ctx context.Context, scope ResourceScope, jobID int64) (ImportJob, error) {
	if _, err := s.GetImportJobScoped(ctx, scope, jobID); err != nil {
		return ImportJob{}, err
	}
	return s.RetryImportJob(ctx, jobID)
}

func (s *Store) RetryImportItem(ctx context.Context, itemID int64) (ImportItem, error) {
	row := s.pool.QueryRow(ctx, `
		update token_import_items
		set status = 'queued',
			    error_message = null,
			    validation_started_at = null,
			    validation_finished_at = null,
			    matched_existing_token_id = null,
			    publish_attempted = false,
			    publish_skipped_reason = null,
			    reactivated = false,
			    previous_is_active = null,
			    next_is_active = null,
			    previous_disabled_at = null,
			    next_disabled_at = null,
			    refresh_error_code = null,
			    refresh_error_message_excerpt = null,
			    updated_at = now()
			where id = $1
			returning `+importItemSelectColumns("")+`
	`, itemID)
	return scanImportItem(row)
}

func (s *Store) RetryImportItemScoped(ctx context.Context, scope ResourceScope, itemID int64) (ImportItem, error) {
	if err := s.ensureImportItemInScope(ctx, scope, itemID); err != nil {
		return ImportItem{}, err
	}
	return s.RetryImportItem(ctx, itemID)
}

func (s *Store) SkipImportItem(ctx context.Context, itemID int64, reason string) (ImportItem, error) {
	row := s.pool.QueryRow(ctx, `
		update token_import_items
		set status = 'skipped',
		    error_message = nullif($2, ''),
		    updated_at = now()
		where id = $1
			returning `+importItemSelectColumns("")+`
	`, itemID, truncate(reason, 1000))
	return scanImportItem(row)
}

func (s *Store) SkipImportItemScoped(ctx context.Context, scope ResourceScope, itemID int64, reason string) (ImportItem, error) {
	if err := s.ensureImportItemInScope(ctx, scope, itemID); err != nil {
		return ImportItem{}, err
	}
	return s.SkipImportItem(ctx, itemID, reason)
}

func (s *Store) ensureImportItemInScope(ctx context.Context, scope ResourceScope, itemID int64) error {
	args := []any{itemID}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	var id int64
	return s.pool.QueryRow(ctx, `
		select id
		from token_import_items
		where id = $1
		  and `+ownerWhere, args...).Scan(&id)
}

func (s *Store) DeleteImportJobDryRun(ctx context.Context, id int64) (DeletedImportJob, error) {
	job, err := s.GetImportJob(ctx, id)
	if err != nil {
		return DeletedImportJob{}, err
	}
	if importJobDeleteBlocked(job.Status) {
		return DeletedImportJob{}, fmt.Errorf("cannot delete %s import job", job.Status)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeletedImportJob{}, err
	}
	defer tx.Rollback(ctx)
	tokenIDs, err := s.importJobTokenIDsForDelete(ctx, tx, id)
	if err != nil {
		return DeletedImportJob{}, err
	}
	return DeletedImportJob{ImportJob: job, TokenIDs: tokenIDs, DeletedTokens: int64(len(tokenIDs))}, nil
}

func (s *Store) ListAuditLogs(ctx context.Context, opts AuditLogListOptions) ([]AuditLog, int, error) {
	if opts.Limit <= 0 || opts.Limit > 500 {
		opts.Limit = 100
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	where, args := auditLogWhere(opts)
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from admin_audit_logs where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
		select id, action, actor, target_type, target_id, payload, created_at
		from admin_audit_logs
		where `+where+`
		order by id desc
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []AuditLog{}
	for rows.Next() {
		var item AuditLog
		if err := rows.Scan(&item.ID, &item.Action, &item.Actor, &item.TargetType, &item.TargetID, &item.Payload, &item.CreatedAt); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func auditLogWhere(opts AuditLogListOptions) (string, []any) {
	filters := []string{"true"}
	args := []any{}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if opts.Action != "" {
		filters = append(filters, fmt.Sprintf("action = %s", arg(opts.Action)))
	}
	if opts.Actor != "" {
		filters = append(filters, fmt.Sprintf("actor = %s", arg(opts.Actor)))
	}
	if opts.TargetType != "" {
		filters = append(filters, fmt.Sprintf("target_type = %s", arg(opts.TargetType)))
	}
	if opts.TargetID != "" {
		filters = append(filters, fmt.Sprintf("target_id = %s", arg(opts.TargetID)))
	}
	if opts.From != nil {
		filters = append(filters, fmt.Sprintf("created_at >= %s", arg(*opts.From)))
	}
	if opts.To != nil {
		filters = append(filters, fmt.Sprintf("created_at <= %s", arg(*opts.To)))
	}
	return strings.Join(filters, " and "), args
}

func (s *Store) GetAuditLog(ctx context.Context, id int64) (AuditLog, error) {
	var item AuditLog
	err := s.pool.QueryRow(ctx, `
		select id, action, actor, target_type, target_id, payload, created_at
		from admin_audit_logs
		where id = $1
	`, id).Scan(&item.ID, &item.Action, &item.Actor, &item.TargetType, &item.TargetID, &item.Payload, &item.CreatedAt)
	return item, err
}

func (s *Store) ListAdminAPIKeys(ctx context.Context) ([]AdminAPIKey, error) {
	rows, err := s.pool.Query(ctx, `
		select id, name, role, key_hash, created_by, created_at, last_used_at, revoked_at
		from admin_api_keys
		order by id desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []AdminAPIKey{}
	for rows.Next() {
		var item AdminAPIKey
		if err := rows.Scan(&item.ID, &item.Name, &item.Role, &item.KeyHash, &item.CreatedBy, &item.CreatedAt, &item.LastUsedAt, &item.RevokedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) CreateAdminAPIKey(ctx context.Context, name string, role string, createdBy string) (CreatedAdminAPIKey, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "admin-key"
	}
	role = strings.TrimSpace(role)
	if role == "" {
		role = "admin"
	}
	plain, err := randomSecret("oaix_admin", 32)
	if err != nil {
		return CreatedAdminAPIKey{}, err
	}
	hash := hashString(plain)
	var item AdminAPIKey
	err = s.pool.QueryRow(ctx, `
		insert into admin_api_keys(name, role, key_hash, created_by)
		values ($1, $2, $3, nullif($4, ''))
		returning id, name, role, key_hash, created_by, created_at, last_used_at, revoked_at
	`, truncate(name, 128), truncate(role, 32), hash, truncate(createdBy, 128)).
		Scan(&item.ID, &item.Name, &item.Role, &item.KeyHash, &item.CreatedBy, &item.CreatedAt, &item.LastUsedAt, &item.RevokedAt)
	if err != nil {
		return CreatedAdminAPIKey{}, err
	}
	return CreatedAdminAPIKey{AdminAPIKey: item, PlaintextKey: plain}, nil
}

func (s *Store) RotateAdminAPIKey(ctx context.Context, id int64) (CreatedAdminAPIKey, error) {
	plain, err := randomSecret("oaix_admin", 32)
	if err != nil {
		return CreatedAdminAPIKey{}, err
	}
	hash := hashString(plain)
	var item AdminAPIKey
	err = s.pool.QueryRow(ctx, `
		update admin_api_keys
		set key_hash = $2, revoked_at = null
		where id = $1
		returning id, name, role, key_hash, created_by, created_at, last_used_at, revoked_at
	`, id, hash).Scan(&item.ID, &item.Name, &item.Role, &item.KeyHash, &item.CreatedBy, &item.CreatedAt, &item.LastUsedAt, &item.RevokedAt)
	if err != nil {
		return CreatedAdminAPIKey{}, err
	}
	return CreatedAdminAPIKey{AdminAPIKey: item, PlaintextKey: plain}, nil
}

func (s *Store) RevokeAdminAPIKey(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `update admin_api_keys set revoked_at = now() where id = $1 and revoked_at is null`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ValidateAdminAPIKey(ctx context.Context, plaintext string) (AdminAPIKey, bool, error) {
	plaintext = strings.TrimSpace(plaintext)
	if plaintext == "" {
		return AdminAPIKey{}, false, nil
	}
	hash := hashString(plaintext)
	var item AdminAPIKey
	err := s.pool.QueryRow(ctx, `
		update admin_api_keys
		set last_used_at = now()
		where key_hash = $1 and revoked_at is null
		returning id, name, role, key_hash, created_by, created_at, last_used_at, revoked_at
	`, hash).Scan(&item.ID, &item.Name, &item.Role, &item.KeyHash, &item.CreatedBy, &item.CreatedAt, &item.LastUsedAt, &item.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdminAPIKey{}, false, nil
	}
	if err != nil {
		return AdminAPIKey{}, false, err
	}
	return item, true, nil
}

func (s *Store) GetIdempotencyRecord(ctx context.Context, key string, method string, path string) (IdempotencyRecord, error) {
	var item IdempotencyRecord
	err := s.pool.QueryRow(ctx, `
		select key, method, path, request_hash, status_code, response, created_at, expires_at
		from admin_idempotency_keys
		where key = $1 and method = $2 and path = $3 and expires_at > now()
	`, key, method, path).Scan(&item.Key, &item.Method, &item.Path, &item.RequestHash, &item.StatusCode, &item.Response, &item.CreatedAt, &item.ExpiresAt)
	return item, err
}

func (s *Store) SaveIdempotencyRecord(ctx context.Context, key string, method string, path string, requestHash string, statusCode int, response any, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	_, err := s.pool.Exec(ctx, `
		insert into admin_idempotency_keys(key, method, path, request_hash, status_code, response, expires_at)
		values ($1, $2, $3, $4, $5, $6, now() + make_interval(secs => $7))
		on conflict (key) do nothing
	`, key, method, path, requestHash, statusCode, jsonBytes(response), int(ttl.Seconds()))
	return err
}

func (s *Store) SaveOAuthSession(ctx context.Context, session OAuthSession) error {
	if session.CreatedAt.IsZero() {
		session.CreatedAt = time.Now().UTC()
	}
	if session.ExpiresAt.IsZero() {
		session.ExpiresAt = session.CreatedAt.Add(30 * time.Minute)
	}
	_, err := s.pool.Exec(ctx, `
		insert into openai_oauth_sessions(
			session_id, owner_user_id, state, code_verifier, redirect_uri, import_queue_position,
			status, import_job_id, error_message, created_at, expires_at, updated_at
		)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,coalesce($10, now()),$11,now())
		on conflict (session_id) do update set
			owner_user_id = excluded.owner_user_id,
			state = excluded.state,
			code_verifier = excluded.code_verifier,
			redirect_uri = excluded.redirect_uri,
			import_queue_position = excluded.import_queue_position,
			status = excluded.status,
			import_job_id = excluded.import_job_id,
			error_message = excluded.error_message,
			expires_at = excluded.expires_at,
			updated_at = now()
	`, session.SessionID, session.OwnerUserID, session.State, session.CodeVerifier, session.RedirectURI, session.ImportQueuePosition,
		firstString(session.Status, "pending"), session.ImportJobID, session.ErrorMessage, session.CreatedAt, session.ExpiresAt)
	return err
}

func (s *Store) GetOAuthSession(ctx context.Context, sessionID string) (OAuthSession, error) {
	return s.getOAuthSession(ctx, "session_id", sessionID)
}

func (s *Store) GetOAuthSessionByState(ctx context.Context, state string) (OAuthSession, error) {
	return s.getOAuthSession(ctx, "state", state)
}

func (s *Store) getOAuthSession(ctx context.Context, column string, value string) (OAuthSession, error) {
	var item OAuthSession
	if column != "session_id" && column != "state" {
		return item, fmt.Errorf("invalid oauth session lookup")
	}
	err := s.pool.QueryRow(ctx, `
		select session_id, coalesce(owner_user_id, 0), state, code_verifier, redirect_uri, import_queue_position,
		       status, import_job_id, error_message, created_at, expires_at, updated_at
		from openai_oauth_sessions
		where `+column+` = $1
	`, value).Scan(&item.SessionID, &item.OwnerUserID, &item.State, &item.CodeVerifier, &item.RedirectURI, &item.ImportQueuePosition,
		&item.Status, &item.ImportJobID, &item.ErrorMessage, &item.CreatedAt, &item.ExpiresAt, &item.UpdatedAt)
	return item, err
}

func (s *Store) UpdateOAuthSessionStatus(ctx context.Context, sessionID string, status string, jobID *int64, message string) error {
	_, err := s.pool.Exec(ctx, `
		update openai_oauth_sessions
		set status = $2,
		    import_job_id = coalesce($3, import_job_id),
		    error_message = nullif($4, ''),
		    updated_at = now()
		where session_id = $1
	`, sessionID, status, jobID, truncate(message, 1000))
	return err
}

func (s *Store) CancelOAuthSession(ctx context.Context, sessionID string) error {
	tag, err := s.pool.Exec(ctx, `
		update openai_oauth_sessions
		set status = 'canceled', updated_at = now()
		where session_id = $1 and status not in ('completed', 'failed', 'canceled')
	`, sessionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

const saveQuotaSnapshotSQL = `
	insert into token_quota_snapshots(token_id, owner_user_id, snapshot, plan_type, error_message, fetched_at)
	select id, owner_user_id, $2, $3, $4, now()
	from codex_tokens
	where id = $1
`

func (s *Store) SaveQuotaSnapshot(ctx context.Context, tokenID int64, snapshot any, planType *string, errorMessage *string) error {
	_, err := s.SaveQuotaSnapshotAndPlan(ctx, tokenID, snapshot, planType, errorMessage)
	return err
}

type QuotaSnapshotSaveResult struct {
	OwnerUserID int64
	PlanType    string
	PlanChanged bool
}

func (s *Store) SaveQuotaSnapshotAndPlan(ctx context.Context, tokenID int64, snapshot any, planType *string, errorMessage *string) (QuotaSnapshotSaveResult, error) {
	result := QuotaSnapshotSaveResult{}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	var ownerUserID *int64
	var currentPlan *string
	if err := tx.QueryRow(ctx, `
		select owner_user_id, plan_type
		from codex_tokens
		where id = $1 and merged_into_token_id is null
		for update
	`, tokenID).Scan(&ownerUserID, &currentPlan); err != nil {
		return result, err
	}
	if ownerUserID != nil {
		result.OwnerUserID = *ownerUserID
	}

	tag, err := tx.Exec(ctx, saveQuotaSnapshotSQL, tokenID, jsonBytes(snapshot), planType, errorMessage)
	if err != nil {
		return result, err
	}
	if tag.RowsAffected() == 0 {
		return result, pgx.ErrNoRows
	}

	if planType != nil {
		result.PlanType = normalizePlanFilter(*planType)
	}
	if result.PlanType != "" && len(result.PlanType) <= 32 {
		current := ""
		if currentPlan != nil {
			current = normalizePlanFilter(*currentPlan)
		}
		if current != result.PlanType {
			tag, err = tx.Exec(ctx, `
				update codex_tokens
				set plan_type = $2, updated_at = now()
				where id = $1 and merged_into_token_id is null
			`, tokenID, result.PlanType)
			if err != nil {
				return result, err
			}
			if tag.RowsAffected() == 0 {
				return result, pgx.ErrNoRows
			}
			if err := deleteTokenModelCapabilityLossesTx(ctx, tx, tokenID); err != nil {
				return result, err
			}
			result.PlanChanged = true
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (s *Store) ListQuotaSnapshots(ctx context.Context, tokenID int64, limit int) ([]QuotaSnapshot, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		select id, token_id, snapshot, plan_type, error_message, fetched_at
		from token_quota_snapshots
		where token_id = $1
		order by fetched_at desc, id desc
		limit $2
	`, tokenID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []QuotaSnapshot{}
	for rows.Next() {
		var item QuotaSnapshot
		if err := rows.Scan(&item.ID, &item.TokenID, &item.Snapshot, &item.PlanType, &item.ErrorMessage, &item.FetchedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListPromptAffinityLanes(ctx context.Context, limit int, offset int) ([]PromptAffinityLane, int, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	var total int
	if err := s.pool.QueryRow(ctx, `select count(*) from prompt_affinity_lanes`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `
		select prompt_key_hash, primary_token_id, secondary_token_ids, policy_version, expires_at, updated_at
		from prompt_affinity_lanes
		order by updated_at desc
		limit $1 offset $2
	`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []PromptAffinityLane{}
	for rows.Next() {
		var item PromptAffinityLane
		if err := rows.Scan(&item.PromptKeyHash, &item.PrimaryTokenID, &item.SecondaryTokenIDs, &item.PolicyVersion, &item.ExpiresAt, &item.UpdatedAt); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) GetPromptAffinityLane(ctx context.Context, hash string) (PromptAffinityLane, error) {
	var item PromptAffinityLane
	err := s.pool.QueryRow(ctx, `
		select prompt_key_hash, primary_token_id, secondary_token_ids, policy_version, expires_at, updated_at
		from prompt_affinity_lanes
		where prompt_key_hash = $1
	`, hash).Scan(&item.PromptKeyHash, &item.PrimaryTokenID, &item.SecondaryTokenIDs, &item.PolicyVersion, &item.ExpiresAt, &item.UpdatedAt)
	return item, err
}

func (s *Store) DeletePromptAffinityLane(ctx context.Context, hash string) error {
	tag, err := s.pool.Exec(ctx, `delete from prompt_affinity_lanes where prompt_key_hash = $1`, hash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ListResponseOwnerBindings(ctx context.Context, limit int, offset int, tokenID int64) ([]ResponseOwnerBinding, int, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filters := []string{"true"}
	args := []any{}
	if tokenID > 0 {
		args = append(args, tokenID)
		filters = append(filters, fmt.Sprintf("token_id = $%d", len(args)))
	}
	where := strings.Join(filters, " and ")
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from response_owner_bindings where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	rows, err := s.pool.Query(ctx, `
		select response_id_hash, token_id, expires_at, updated_at
		from response_owner_bindings
		where `+where+`
		order by updated_at desc
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []ResponseOwnerBinding{}
	for rows.Next() {
		var item ResponseOwnerBinding
		if err := rows.Scan(&item.ResponseIDHash, &item.TokenID, &item.ExpiresAt, &item.UpdatedAt); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) DeleteResponseOwnerBinding(ctx context.Context, hash string) error {
	tag, err := s.pool.Exec(ctx, `delete from response_owner_bindings where response_id_hash = $1`, hash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) DeleteTokenResponseOwners(ctx context.Context, tokenID int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `delete from response_owner_bindings where token_id = $1`, tokenID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func scanImportItem(row rowScanner) (ImportItem, error) {
	var item ImportItem
	var payload []byte
	var validated []byte
	err := row.Scan(
		&item.ID,
		&item.OwnerUserID,
		&item.JobID,
		&item.ItemIndex,
		&item.Status,
		&item.RefreshTokenHash,
		&payload,
		&validated,
		&item.TokenID,
		&item.Action,
		&item.ErrorMessage,
		&item.ValidationMS,
		&item.PublishMS,
		&item.ValidationStarted,
		&item.ValidationFinished,
		&item.PublishedAt,
		&item.MatchedExistingTokenID,
		&item.PublishAttempted,
		&item.PublishSkippedReason,
		&item.Reactivated,
		&item.PreviousIsActive,
		&item.NextIsActive,
		&item.PreviousDisabledAt,
		&item.NextDisabledAt,
		&item.RefreshErrorCode,
		&item.RefreshErrorMessageExcerpt,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &item.Payload)
	}
	if len(validated) > 0 {
		_ = json.Unmarshal(validated, &item.ValidatedPayload)
	}
	return item, err
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func boolPtrValue(value *bool) bool {
	return value != nil && *value
}

func firstString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func randomSecret(prefix string, size int) (string, error) {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(data), nil
}
