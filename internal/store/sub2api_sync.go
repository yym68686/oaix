package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	Sub2APISyncStatusRunning   = "running"
	Sub2APISyncStatusCompleted = "completed"
	Sub2APISyncStatusSkipped   = "skipped"
	Sub2APISyncStatusFailed    = "failed"

	Sub2APITokenStatusAvailable = "available"
	Sub2APITokenStatusCooling   = "cooling"
	Sub2APITokenStatusDisabled  = "disabled"
)

type Sub2APISyncTarget struct {
	ID                   int64      `json:"id"`
	Name                 string     `json:"name"`
	BaseURL              string     `json:"base_url"`
	AdminKey             string     `json:"-"`
	AdminKeySet          bool       `json:"admin_key_set"`
	Enabled              bool       `json:"enabled"`
	OwnerUserID          int64      `json:"owner_user_id"`
	OwnerEmail           *string    `json:"owner_email,omitempty"`
	PlanFilters          []string   `json:"plan_filters"`
	TokenStatusFilters   []string   `json:"token_status_filters"`
	TargetGroupIDs       []int64    `json:"target_group_ids"`
	TargetGroupNames     []string   `json:"target_group_names"`
	CheckIntervalSeconds int        `json:"check_interval_seconds"`
	MinAvailable         int        `json:"min_available"`
	TopUpBatchSize       int        `json:"top_up_batch_size"`
	AutoSyncNew          bool       `json:"auto_sync_new"`
	LastCheckedAt        *time.Time `json:"last_checked_at,omitempty"`
	LastSyncedAt         *time.Time `json:"last_synced_at,omitempty"`
	LastStatus           *string    `json:"last_status,omitempty"`
	LastError            *string    `json:"last_error,omitempty"`
	LastRemoteCount      *int       `json:"last_remote_count,omitempty"`
	LastSyncedCount      int        `json:"last_synced_count"`
	LastRunID            *int64     `json:"last_run_id,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

type Sub2APISyncTargetInput struct {
	Name                 string
	BaseURL              string
	AdminKey             *string
	Enabled              *bool
	OwnerUserID          int64
	PlanFilters          []string
	TokenStatusFilters   []string
	TargetGroupIDs       []int64
	TargetGroupNames     []string
	CheckIntervalSeconds int
	MinAvailable         int
	TopUpBatchSize       int
	AutoSyncNew          *bool
}

type Sub2APISyncRun struct {
	ID            int64           `json:"id"`
	TargetID      int64           `json:"target_id"`
	Trigger       string          `json:"trigger"`
	Status        string          `json:"status"`
	RemoteCount   int             `json:"remote_count"`
	Threshold     int             `json:"threshold"`
	NeededCount   int             `json:"needed_count"`
	SelectedCount int             `json:"selected_count"`
	SyncedCount   int             `json:"synced_count"`
	FailedCount   int             `json:"failed_count"`
	ErrorMessage  *string         `json:"error_message,omitempty"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
	Details       json.RawMessage `json:"details,omitempty"`
}

type Sub2APISyncRunResult struct {
	Status        string
	RemoteCount   int
	Threshold     int
	NeededCount   int
	SelectedCount int
	SyncedCount   int
	FailedCount   int
	ErrorMessage  string
	Details       any
}

type Sub2APITokenCandidate struct {
	ID           int64           `json:"id"`
	OwnerUserID  int64           `json:"owner_user_id"`
	Email        string          `json:"email,omitempty"`
	AccountID    string          `json:"account_id,omitempty"`
	IDToken      string          `json:"-"`
	AccessToken  string          `json:"-"`
	RefreshToken string          `json:"-"`
	PlanType     string          `json:"plan_type,omitempty"`
	RawPayload   json.RawMessage `json:"raw_payload,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type Sub2APITokenCandidateOptions struct {
	Limit           int
	CreatedAfter    *time.Time
	ExcludeTokenIDs []int64
}

func (s *Store) ListSub2APISyncTargets(ctx context.Context) ([]Sub2APISyncTarget, error) {
	rows, err := s.pool.Query(ctx, `
		select t.id, t.name, t.base_url, t.admin_key, t.enabled, coalesce(t.owner_user_id, 0), u.email,
		       t.plan_filters, t.token_status_filters, t.target_group_ids, t.target_group_names,
		       t.check_interval_seconds, t.min_available, t.top_up_batch_size, t.auto_sync_new,
		       t.last_checked_at, t.last_synced_at, t.last_status, t.last_error,
		       t.last_remote_count, t.last_synced_count, t.last_run_id,
		       t.created_at, t.updated_at
		from sub2api_sync_targets t
		left join platform_users u on u.id = t.owner_user_id
		order by t.id desc
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Sub2APISyncTarget{}
	for rows.Next() {
		item, err := scanSub2APISyncTarget(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) GetSub2APISyncTarget(ctx context.Context, id int64) (Sub2APISyncTarget, error) {
	row := s.pool.QueryRow(ctx, `
		select t.id, t.name, t.base_url, t.admin_key, t.enabled, coalesce(t.owner_user_id, 0), u.email,
		       t.plan_filters, t.token_status_filters, t.target_group_ids, t.target_group_names,
		       t.check_interval_seconds, t.min_available, t.top_up_batch_size, t.auto_sync_new,
		       t.last_checked_at, t.last_synced_at, t.last_status, t.last_error,
		       t.last_remote_count, t.last_synced_count, t.last_run_id,
		       t.created_at, t.updated_at
		from sub2api_sync_targets t
		left join platform_users u on u.id = t.owner_user_id
		where t.id = $1
	`, id)
	return scanSub2APISyncTarget(row)
}

func (s *Store) CreateSub2APISyncTarget(ctx context.Context, input Sub2APISyncTargetInput) (Sub2APISyncTarget, error) {
	if err := validateSub2APISyncTargetInput(input, true); err != nil {
		return Sub2APISyncTarget{}, err
	}
	adminKey := ""
	if input.AdminKey != nil {
		adminKey = strings.TrimSpace(*input.AdminKey)
	}
	row := s.pool.QueryRow(ctx, `
		insert into sub2api_sync_targets(
			name, base_url, admin_key, enabled, owner_user_id, plan_filters, token_status_filters, target_group_ids, target_group_names,
			check_interval_seconds, min_available, top_up_batch_size, auto_sync_new,
			last_status, updated_at
		)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'idle', now())
		returning id
	`, normalizedSub2APIName(input.Name), normalizeSub2APIBaseURL(input.BaseURL), adminKey,
		boolValue(input.Enabled, true), input.OwnerUserID,
		normalizePlanFilters(input.PlanFilters), normalizeTokenStatusFilters(input.TokenStatusFilters), normalizeInt64IDs(input.TargetGroupIDs), normalizeStringList(input.TargetGroupNames),
		normalizeCheckInterval(input.CheckIntervalSeconds), normalizeNonNegative(input.MinAvailable, 10), normalizePositive(input.TopUpBatchSize, 10),
		boolValue(input.AutoSyncNew, false),
	)
	var id int64
	if err := row.Scan(&id); err != nil {
		return Sub2APISyncTarget{}, err
	}
	return s.GetSub2APISyncTarget(ctx, id)
}

func (s *Store) UpdateSub2APISyncTarget(ctx context.Context, id int64, input Sub2APISyncTargetInput) (Sub2APISyncTarget, error) {
	if err := validateSub2APISyncTargetInput(input, false); err != nil {
		return Sub2APISyncTarget{}, err
	}
	adminKey := ""
	hasAdminKey := false
	if input.AdminKey != nil {
		adminKey = strings.TrimSpace(*input.AdminKey)
		hasAdminKey = adminKey != ""
	}
	row := s.pool.QueryRow(ctx, `
		update sub2api_sync_targets
		set name = $2,
		    base_url = $3,
		    admin_key = case when $4 then $5 else admin_key end,
		    enabled = $6,
		    owner_user_id = $7,
		    plan_filters = $8,
		    token_status_filters = $9,
		    target_group_ids = $10,
		    target_group_names = $11,
		    check_interval_seconds = $12,
		    min_available = $13,
		    top_up_batch_size = $14,
		    auto_sync_new = $15,
		    updated_at = now()
		where id = $1
		returning id
	`, id, normalizedSub2APIName(input.Name), normalizeSub2APIBaseURL(input.BaseURL),
		hasAdminKey, adminKey, boolValue(input.Enabled, true), input.OwnerUserID,
		normalizePlanFilters(input.PlanFilters), normalizeTokenStatusFilters(input.TokenStatusFilters), normalizeInt64IDs(input.TargetGroupIDs), normalizeStringList(input.TargetGroupNames),
		normalizeCheckInterval(input.CheckIntervalSeconds), normalizeNonNegative(input.MinAvailable, 10), normalizePositive(input.TopUpBatchSize, 10),
		boolValue(input.AutoSyncNew, false),
	)
	var returnedID int64
	if err := row.Scan(&returnedID); err != nil {
		return Sub2APISyncTarget{}, err
	}
	return s.GetSub2APISyncTarget(ctx, returnedID)
}

func (s *Store) DeleteSub2APISyncTarget(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `delete from sub2api_sync_targets where id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ClaimDueSub2APISyncTargets(ctx context.Context, limit int) ([]Sub2APISyncTarget, error) {
	if limit <= 0 || limit > 25 {
		limit = 10
	}
	rows, err := s.pool.Query(ctx, `
		with due as (
			select id
			from sub2api_sync_targets
			where enabled = true
			  and coalesce(owner_user_id, 0) > 0
			  and btrim(base_url) <> ''
			  and btrim(admin_key) <> ''
			  and (
				last_checked_at is null
				or last_checked_at + (greatest(check_interval_seconds, 60) * interval '1 second') <= now()
			  )
			order by coalesce(last_checked_at, 'epoch'::timestamptz), id
			limit $1
			for update skip locked
		)
		update sub2api_sync_targets t
		set last_status = 'running',
		    last_error = null,
		    last_checked_at = now(),
		    updated_at = now()
		from due
		where t.id = due.id
		returning t.id, t.name, t.base_url, t.admin_key, t.enabled, coalesce(t.owner_user_id, 0),
		          (select email from platform_users where id = t.owner_user_id),
		          t.plan_filters, t.token_status_filters, t.target_group_ids, t.target_group_names,
		          t.check_interval_seconds, t.min_available, t.top_up_batch_size, t.auto_sync_new,
		          t.last_checked_at, t.last_synced_at, t.last_status, t.last_error,
		          t.last_remote_count, t.last_synced_count, t.last_run_id,
		          t.created_at, t.updated_at
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Sub2APISyncTarget{}
	for rows.Next() {
		item, err := scanSub2APISyncTarget(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) StartSub2APISyncRun(ctx context.Context, targetID int64, trigger string) (Sub2APISyncRun, error) {
	trigger = normalizeSub2APITrigger(trigger)
	row := s.pool.QueryRow(ctx, `
		insert into sub2api_sync_runs(target_id, trigger, status, started_at)
		values ($1, $2, 'running', now())
		returning id, target_id, trigger, status, remote_count, threshold, needed_count, selected_count,
		          synced_count, failed_count, error_message, started_at, finished_at, details
	`, targetID, trigger)
	return scanSub2APISyncRun(row)
}

func (s *Store) FinishSub2APISyncRun(ctx context.Context, runID int64, result Sub2APISyncRunResult) (Sub2APISyncRun, error) {
	status := strings.TrimSpace(result.Status)
	if status == "" {
		status = Sub2APISyncStatusCompleted
	}
	row := s.pool.QueryRow(ctx, `
		update sub2api_sync_runs
		set status = $2,
		    remote_count = $3,
		    threshold = $4,
		    needed_count = $5,
		    selected_count = $6,
		    synced_count = $7,
		    failed_count = $8,
		    error_message = nullif($9, ''),
		    finished_at = now(),
		    details = $10
		where id = $1
		returning id, target_id, trigger, status, remote_count, threshold, needed_count, selected_count,
		          synced_count, failed_count, error_message, started_at, finished_at, details
	`, runID, status, result.RemoteCount, result.Threshold, result.NeededCount, result.SelectedCount,
		result.SyncedCount, result.FailedCount, truncate(result.ErrorMessage, 2000), jsonBytes(result.Details))
	run, err := scanSub2APISyncRun(row)
	if err != nil {
		return Sub2APISyncRun{}, err
	}
	if err := s.updateSub2APITargetFromRun(ctx, run); err != nil {
		return run, err
	}
	return run, nil
}

func (s *Store) ListSub2APISyncRuns(ctx context.Context, targetID int64, limit int) ([]Sub2APISyncRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	args := []any{limit}
	where := "true"
	if targetID > 0 {
		args = append(args, targetID)
		where = fmt.Sprintf("target_id = $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, `
		select id, target_id, trigger, status, remote_count, threshold, needed_count, selected_count,
		       synced_count, failed_count, error_message, started_at, finished_at, details
		from sub2api_sync_runs
		where `+where+`
		order by started_at desc, id desc
		limit $1
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Sub2APISyncRun{}
	for rows.Next() {
		item, err := scanSub2APISyncRun(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListSub2APITokenCandidates(ctx context.Context, target Sub2APISyncTarget, opts Sub2APITokenCandidateOptions) ([]Sub2APITokenCandidate, error) {
	if opts.Limit <= 0 || opts.Limit > 500 {
		opts.Limit = 100
	}
	args := []any{target.ID, target.OwnerUserID}
	filters := []string{
		"t.merged_into_token_id is null",
		"t.owner_user_id = $2",
		"coalesce(t.access_token, '') <> ''",
		"coalesce(t.refresh_token, '') <> ''",
		`not exists (
			select 1
			from sub2api_sync_mappings m
			where m.target_id = $1
			  and m.token_id = t.id
			  and m.status = 'synced'
		)`,
	}
	statuses := normalizeTokenStatusFilters(target.TokenStatusFilters)
	filters = append(filters, tokenStatusFilterSQL(statuses))
	plans := normalizePlanFilters(target.PlanFilters)
	if len(plans) > 0 {
		args = append(args, plans)
		placeholder := fmt.Sprintf("$%d::text[]", len(args))
		filters = append(filters, fmt.Sprintf(`(
			case when t.plan_type is null or btrim(t.plan_type) = '' then 'unknown' else lower(btrim(t.plan_type)) end
		) = any(%s)`, placeholder))
	}
	if opts.CreatedAfter != nil && !opts.CreatedAfter.IsZero() {
		args = append(args, opts.CreatedAfter.UTC())
		filters = append(filters, fmt.Sprintf("t.created_at >= $%d", len(args)))
	}
	if ids := postgresIntIDs(opts.ExcludeTokenIDs); len(ids) > 0 {
		args = append(args, ids)
		filters = append(filters, fmt.Sprintf("not (t.id = any($%d::integer[]))", len(args)))
	}
	args = append(args, opts.Limit)
	rawPayloadExpr := sub2APIRawPayloadSelectSQL()
	rows, err := s.pool.Query(ctx, `
		select t.id, coalesce(t.owner_user_id, 0), t.email, t.account_id, t.id_token,
		       t.access_token, t.refresh_token, t.plan_type, `+rawPayloadExpr+`,
		       t.created_at, t.updated_at
		from codex_tokens t
		where `+strings.Join(filters, " and ")+`
		order by t.created_at asc, t.id asc
		limit $`+strconvI(len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Sub2APITokenCandidate{}
	for rows.Next() {
		item, err := scanSub2APITokenCandidate(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) RecordSub2APISyncMapping(ctx context.Context, targetID int64, tokenID int64, remoteAccountID int64, runID int64, status string, message string) error {
	status = strings.TrimSpace(status)
	if status == "" {
		status = "synced"
	}
	_, err := s.pool.Exec(ctx, `
		insert into sub2api_sync_mappings(target_id, token_id, remote_account_id, last_run_id, status, error_message, created_at, updated_at)
		values ($1, $2, nullif($3, 0), nullif($4, 0), $5, nullif($6, ''), now(), now())
		on conflict (target_id, token_id) do update
		set remote_account_id = coalesce(excluded.remote_account_id, sub2api_sync_mappings.remote_account_id),
		    last_run_id = coalesce(excluded.last_run_id, sub2api_sync_mappings.last_run_id),
		    status = excluded.status,
		    error_message = excluded.error_message,
		    updated_at = now()
	`, targetID, tokenID, remoteAccountID, runID, status, truncate(message, 2000))
	return err
}

func (s *Store) MarkSub2APITargetsDueForImportJobs(ctx context.Context, jobIDs []int64) error {
	ids := postgresIntIDs(jobIDs)
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		update sub2api_sync_targets target
		set last_checked_at = null,
		    updated_at = now()
		where target.enabled = true
		  and target.auto_sync_new = true
		  and target.owner_user_id in (
			select distinct owner_user_id
			from token_import_jobs
			where id = any($1::integer[])
			  and owner_user_id is not null
		  )
	`, ids)
	return err
}

func validateSub2APISyncTargetInput(input Sub2APISyncTargetInput, requireAdminKey bool) error {
	if normalizedSub2APIName(input.Name) == "" {
		return errors.New("name is required")
	}
	if normalizeSub2APIBaseURL(input.BaseURL) == "" {
		return errors.New("base_url is required")
	}
	if input.OwnerUserID <= 0 {
		return errors.New("owner_user_id is required")
	}
	if requireAdminKey && (input.AdminKey == nil || strings.TrimSpace(*input.AdminKey) == "") {
		return errors.New("admin_key is required")
	}
	if len(normalizeInt64IDs(input.TargetGroupIDs)) == 0 {
		return errors.New("target_group_ids is required")
	}
	return nil
}

func scanSub2APISyncTarget(row rowScanner) (Sub2APISyncTarget, error) {
	var item Sub2APISyncTarget
	var ownerEmail sql.NullString
	err := row.Scan(
		&item.ID, &item.Name, &item.BaseURL, &item.AdminKey, &item.Enabled, &item.OwnerUserID, &ownerEmail,
		&item.PlanFilters, &item.TokenStatusFilters, &item.TargetGroupIDs, &item.TargetGroupNames,
		&item.CheckIntervalSeconds, &item.MinAvailable, &item.TopUpBatchSize, &item.AutoSyncNew,
		&item.LastCheckedAt, &item.LastSyncedAt, &item.LastStatus, &item.LastError,
		&item.LastRemoteCount, &item.LastSyncedCount, &item.LastRunID,
		&item.CreatedAt, &item.UpdatedAt,
	)
	item.AdminKeySet = strings.TrimSpace(item.AdminKey) != ""
	if ownerEmail.Valid {
		item.OwnerEmail = &ownerEmail.String
	}
	item.PlanFilters = normalizePlanFilters(item.PlanFilters)
	item.TokenStatusFilters = normalizeTokenStatusFilters(item.TokenStatusFilters)
	item.TargetGroupIDs = normalizeInt64IDs(item.TargetGroupIDs)
	item.TargetGroupNames = normalizeStringList(item.TargetGroupNames)
	item.CheckIntervalSeconds = normalizeCheckInterval(item.CheckIntervalSeconds)
	item.MinAvailable = normalizeNonNegative(item.MinAvailable, 10)
	item.TopUpBatchSize = normalizePositive(item.TopUpBatchSize, 10)
	return item, err
}

func scanSub2APISyncRun(row rowScanner) (Sub2APISyncRun, error) {
	var item Sub2APISyncRun
	var details json.RawMessage
	err := row.Scan(
		&item.ID, &item.TargetID, &item.Trigger, &item.Status, &item.RemoteCount, &item.Threshold,
		&item.NeededCount, &item.SelectedCount, &item.SyncedCount, &item.FailedCount,
		&item.ErrorMessage, &item.StartedAt, &item.FinishedAt, &details,
	)
	if len(details) > 0 {
		item.Details = details
	}
	return item, err
}

func scanSub2APITokenCandidate(row rowScanner) (Sub2APITokenCandidate, error) {
	var item Sub2APITokenCandidate
	var email, accountID, idToken, accessToken, refreshToken, planType sql.NullString
	err := row.Scan(
		&item.ID, &item.OwnerUserID, &email, &accountID, &idToken,
		&accessToken, &refreshToken, &planType, &item.RawPayload,
		&item.CreatedAt, &item.UpdatedAt,
	)
	if email.Valid {
		item.Email = email.String
	}
	if accountID.Valid {
		item.AccountID = accountID.String
	}
	if idToken.Valid {
		item.IDToken = idToken.String
	}
	if accessToken.Valid {
		item.AccessToken = accessToken.String
	}
	if refreshToken.Valid {
		item.RefreshToken = refreshToken.String
	}
	if planType.Valid {
		item.PlanType = normalizePlanFilter(planType.String)
	}
	if item.PlanType == "" {
		item.PlanType = planUnknown
	}
	return item, err
}

func (s *Store) updateSub2APITargetFromRun(ctx context.Context, run Sub2APISyncRun) error {
	status := run.Status
	if status == "" {
		status = Sub2APISyncStatusCompleted
	}
	_, err := s.pool.Exec(ctx, `
		update sub2api_sync_targets
		set last_checked_at = coalesce($2, now()),
		    last_synced_at = case when $3 > 0 then coalesce($2, now()) else last_synced_at end,
		    last_status = $4,
		    last_error = $5,
		    last_remote_count = $6,
		    last_synced_count = $3,
		    last_run_id = $7,
		    updated_at = now()
		where id = $1
	`, run.TargetID, run.FinishedAt, run.SyncedCount, status, run.ErrorMessage, run.RemoteCount, run.ID)
	return err
}

func normalizedSub2APIName(value string) string {
	return truncate(value, 128)
}

func normalizeSub2APIBaseURL(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return ""
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func normalizePlanFilters(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		plan := normalizePlanFilter(value)
		if plan == "" || plan == "all" {
			continue
		}
		if _, ok := seen[plan]; ok {
			continue
		}
		seen[plan] = struct{}{}
		out = append(out, plan)
	}
	return out
}

func normalizeTokenStatusFilters(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		status := normalizeTokenStatusFilter(value)
		if status == "" {
			continue
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		out = append(out, status)
	}
	if len(out) == 0 {
		return []string{Sub2APITokenStatusAvailable, Sub2APITokenStatusCooling}
	}
	return out
}

func normalizeTokenStatusFilter(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case Sub2APITokenStatusAvailable, "active", "ready":
		return Sub2APITokenStatusAvailable
	case Sub2APITokenStatusCooling, "cooldown":
		return Sub2APITokenStatusCooling
	case Sub2APITokenStatusDisabled, "inactive":
		return Sub2APITokenStatusDisabled
	default:
		return ""
	}
}

func tokenStatusFilterSQL(statuses []string) string {
	statuses = normalizeTokenStatusFilters(statuses)
	conditions := make([]string, 0, len(statuses))
	for _, status := range statuses {
		switch status {
		case Sub2APITokenStatusAvailable:
			conditions = append(conditions, "(t.is_active = true and t.disabled_at is null and (t.cooldown_until is null or t.cooldown_until <= now()))")
		case Sub2APITokenStatusCooling:
			conditions = append(conditions, "(t.is_active = true and t.disabled_at is null and t.cooldown_until > now())")
		case Sub2APITokenStatusDisabled:
			conditions = append(conditions, "(t.is_active = false or t.disabled_at is not null)")
		}
	}
	if len(conditions) == 0 {
		return "false"
	}
	return "(" + strings.Join(conditions, " or ") + ")"
}

func sub2APIRawPayloadSelectSQL() string {
	return "coalesce(t.raw_payload::jsonb, '{}'::jsonb)"
}

func normalizeInt64IDs(values []int64) []int64 {
	out := make([]int64, 0, len(values))
	seen := map[int64]struct{}{}
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeStringList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = truncate(value, 128)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeCheckInterval(value int) int {
	if value < 60 {
		return 60
	}
	if value > 86400 {
		return 86400
	}
	return value
}

func normalizeNonNegative(value int, fallback int) int {
	if value < 0 {
		return fallback
	}
	if value > 100000 {
		return 100000
	}
	return value
}

func normalizePositive(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 500 {
		return 500
	}
	return value
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func normalizeSub2APITrigger(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "manual", "schedule", "pool_join", "check":
		return value
	default:
		return "manual"
	}
}

func strconvI(value int) string {
	return fmt.Sprintf("%d", value)
}
