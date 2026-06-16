package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type ImportJob struct {
	ID                  int64      `json:"id"`
	Status              string     `json:"status"`
	ImportQueuePosition string     `json:"import_queue_position"`
	TotalCount          int        `json:"total_count"`
	ProcessedCount      int        `json:"processed_count"`
	CreatedCount        int        `json:"created_count"`
	UpdatedCount        int        `json:"updated_count"`
	SkippedCount        int        `json:"skipped_count"`
	FailedCount         int        `json:"failed_count"`
	LastError           *string    `json:"last_error,omitempty"`
	SubmittedAt         time.Time  `json:"submitted_at"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	HeartbeatAt         *time.Time `json:"heartbeat_at,omitempty"`
	FinishedAt          *time.Time `json:"finished_at,omitempty"`
}

type ImportItem struct {
	ID                 int64          `json:"id"`
	JobID              int64          `json:"job_id"`
	ItemIndex          int            `json:"item_index"`
	Status             string         `json:"status"`
	RefreshTokenHash   *string        `json:"refresh_token_hash,omitempty"`
	Payload            map[string]any `json:"payload,omitempty"`
	ValidatedPayload   map[string]any `json:"validated_payload,omitempty"`
	TokenID            *int64         `json:"token_id,omitempty"`
	Action             *string        `json:"action,omitempty"`
	ErrorMessage       *string        `json:"error_message,omitempty"`
	ValidationMS       *int           `json:"validation_ms,omitempty"`
	PublishMS          *int           `json:"publish_ms,omitempty"`
	ValidationStarted  *time.Time     `json:"validation_started_at,omitempty"`
	ValidationFinished *time.Time     `json:"validation_finished_at,omitempty"`
	PublishedAt        *time.Time     `json:"published_at,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

type ImportItemUpdate struct {
	ID               int64
	Status           string
	ValidatedPayload map[string]any
	TokenID          *int64
	Action           string
	ErrorMessage     string
	ValidationMS     *int
	PublishMS        *int
	Published        bool
}

type ImportWorkerMetrics struct {
	Claimed   int64 `json:"claimed"`
	Validated int64 `json:"validated"`
	Published int64 `json:"published"`
	Failed    int64 `json:"failed"`
	Resumed   int64 `json:"resumed"`
}

func (s *Store) CreateCompletedImportJob(ctx context.Context, payloads []map[string]any, queuePosition string, result ImportResult) (ImportJob, error) {
	data, err := json.Marshal(payloads)
	if err != nil {
		return ImportJob{}, err
	}
	row := s.pool.QueryRow(ctx, `
		insert into token_import_jobs (
			status, import_queue_position, payloads, total_count, processed_count,
			created_count, updated_count, skipped_count, failed_count,
			submitted_at, started_at, heartbeat_at, finished_at
		)
		values ('completed', $1, $2, $3, $4, $5, $6, $7, $8, now(), now(), now(), now())
		returning id, status, import_queue_position, total_count, processed_count,
		          created_count, updated_count, skipped_count, failed_count, last_error,
		          submitted_at, started_at, heartbeat_at, finished_at
	`, queuePosition, data, len(payloads), result.Created+result.Updated+result.Skipped+result.Failed, result.Created, result.Updated, result.Skipped, result.Failed)
	return scanImportJob(row)
}

func (s *Store) CreateQueuedImportJob(ctx context.Context, payloads []map[string]any, queuePosition string) (ImportJob, error) {
	if strings.TrimSpace(queuePosition) == "" {
		queuePosition = "front"
	}
	data, err := json.Marshal(payloads)
	if err != nil {
		return ImportJob{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		insert into token_import_jobs (
			status, import_queue_position, payloads, total_count, submitted_at, heartbeat_at
		)
		values ('queued', $1, $2, $3, now(), now())
		returning id, status, import_queue_position, total_count, processed_count,
		          created_count, updated_count, skipped_count, failed_count, last_error,
		          submitted_at, started_at, heartbeat_at, finished_at
	`, queuePosition, data, len(payloads))
	job, err := scanImportJob(row)
	if err != nil {
		return ImportJob{}, err
	}
	batchValues := make([]string, 0, len(payloads))
	args := []any{job.ID}
	for index, payload := range payloads {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return ImportJob{}, err
		}
		hash := refreshHashFromPayload(payload)
		args = append(args, index, encoded, hash)
		base := 2 + index*3
		batchValues = append(batchValues, fmt.Sprintf("($1, $%d, $%d, $%d)", base, base+1, base+2))
	}
	if len(batchValues) > 0 {
		_, err = tx.Exec(ctx, `
			insert into token_import_items(job_id, item_index, payload, refresh_token_hash)
			values `+strings.Join(batchValues, ",")+`
		`, args...)
		if err != nil {
			return ImportJob{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ImportJob{}, err
	}
	return job, nil
}

func (s *Store) GetImportJob(ctx context.Context, id int64) (ImportJob, error) {
	row := s.pool.QueryRow(ctx, `
		select id, status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		where id = $1
	`, id)
	return scanImportJob(row)
}

func (s *Store) CancelImportJob(ctx context.Context, id int64) (ImportJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		update token_import_jobs
		set status = case when status in ('completed', 'failed', 'canceled') then status else 'canceled' end,
		    finished_at = case when finished_at is null then now() else finished_at end,
		    heartbeat_at = now()
		where id = $1
		returning id, status, import_queue_position, total_count, processed_count,
		          created_count, updated_count, skipped_count, failed_count, last_error,
		          submitted_at, started_at, heartbeat_at, finished_at
	`, id)
	job, err := scanImportJob(row)
	if err != nil {
		return ImportJob{}, err
	}
	if _, err := tx.Exec(ctx, `
		update token_import_items
		set status = 'canceled', updated_at = now()
		where job_id = $1 and status not in ('published', 'failed', 'skipped', 'canceled')
	`, id); err != nil {
		return ImportJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ImportJob{}, err
	}
	return job, nil
}

func (s *Store) ListImportJobs(ctx context.Context, limit int) ([]ImportJob, error) {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	rows, err := s.pool.Query(ctx, `
		select id, status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		order by id desc
		limit $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]ImportJob, 0)
	for rows.Next() {
		job, err := scanImportJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) ClaimImportItems(ctx context.Context, limit int) ([]ImportItem, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		with claimed as (
			select i.id
			from token_import_items i
			join token_import_jobs j on j.id = i.job_id
			where i.status = 'queued'
			  and j.status in ('queued', 'running')
			order by
			  case when j.import_queue_position = 'front' then j.id end desc,
			  j.id asc,
			  i.item_index asc
			limit $1
			for update skip locked
		)
		update token_import_items i
		set status = 'validating',
		    validation_started_at = now(),
		    updated_at = now()
		from claimed
		where i.id = claimed.id
		returning i.id, i.job_id, i.item_index, i.status, i.refresh_token_hash, i.payload,
		          i.validated_payload, i.token_id, i.action, i.error_message, i.validation_ms,
		          i.publish_ms, i.validation_started_at, i.validation_finished_at,
		          i.published_at, i.created_at, i.updated_at
	`, limit)
	if err != nil {
		return nil, err
	}
	items, err := scanImportItems(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(items) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	jobIDs := make(map[int64]struct{}, len(items))
	args := make([]any, 0, len(items))
	placeholders := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := jobIDs[item.JobID]; ok {
			continue
		}
		jobIDs[item.JobID] = struct{}{}
		args = append(args, item.JobID)
		placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
	}
	if _, err := tx.Exec(ctx, `
		update token_import_jobs
		set status = 'running',
		    started_at = coalesce(started_at, now()),
		    heartbeat_at = now()
		where id in (`+strings.Join(placeholders, ",")+`)
	`, args...); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) UpdateImportItems(ctx context.Context, updates []ImportItemUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, update := range updates {
		payload := jsonBytes(update.ValidatedPayload)
		status := strings.TrimSpace(update.Status)
		if status == "" {
			status = "validated"
		}
		batch.Queue(`
			update token_import_items
			set status = $2,
			    validated_payload = coalesce($3, validated_payload),
			    token_id = coalesce($4, token_id),
			    action = coalesce(nullif($5, ''), action),
			    error_message = nullif($6, ''),
			    validation_ms = coalesce($7, validation_ms),
			    publish_ms = coalesce($8, publish_ms),
			    validation_finished_at = coalesce(validation_finished_at, now()),
			    published_at = case when $9 then now() else published_at end,
			    updated_at = now()
			where id = $1
		`, update.ID, status, payload, update.TokenID, update.Action, update.ErrorMessage, update.ValidationMS, update.PublishMS, update.Published)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return s.refreshImportJobProgress(ctx)
}

func (s *Store) PublishImportTokens(ctx context.Context, jobID int64, accessTokens []string, source string) (ImportResult, error) {
	result, err := s.UpsertAccessTokens(ctx, accessTokens, source)
	if err != nil {
		return result, err
	}
	_, err = s.pool.Exec(ctx, `
		update token_import_jobs
		set processed_count = processed_count + $2,
		    created_count = created_count + $3,
		    updated_count = updated_count + $4,
		    skipped_count = skipped_count + $5,
		    failed_count = failed_count + $6,
		    heartbeat_at = now(),
		    status = case
		      when processed_count + $2 >= total_count then 'completed'
		      else status
		    end,
		    finished_at = case
		      when processed_count + $2 >= total_count then now()
		      else finished_at
		    end
		where id = $1
	`, jobID, result.Created+result.Updated+result.Skipped+result.Failed, result.Created, result.Updated, result.Skipped, result.Failed)
	return result, err
}

func (s *Store) ResumeStaleImportJobs(ctx context.Context, staleAfter time.Duration) (int64, error) {
	if staleAfter <= 0 {
		staleAfter = 5 * time.Minute
	}
	var resumed int64
	row := s.pool.QueryRow(ctx, `
		with stale_jobs as (
			update token_import_jobs
			set status = 'queued',
			    heartbeat_at = now(),
			    last_error = null
			where status = 'running'
			  and heartbeat_at < now() - make_interval(secs => $1)
			returning id
		),
		reset_items as (
			update token_import_items
			set status = 'queued',
			    validation_started_at = null,
			    updated_at = now()
			where job_id in (select id from stale_jobs)
			  and status in ('validating', 'publishing')
			returning id
		)
		select count(*) from stale_jobs
	`, int(staleAfter.Seconds()))
	if err := row.Scan(&resumed); err != nil {
		return 0, err
	}
	return resumed, nil
}

func (s *Store) refreshImportJobProgress(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		with item_counts as (
			select
				job_id,
				count(*) filter (where status in ('published', 'skipped', 'failed', 'canceled'))::int as processed,
				count(*) filter (where status = 'failed')::int as failed,
				count(*) filter (where status = 'skipped')::int as skipped
			from token_import_items
			group by job_id
		)
		update token_import_jobs j
		set processed_count = greatest(j.processed_count, item_counts.processed),
		    failed_count = greatest(j.failed_count, item_counts.failed),
		    skipped_count = greatest(j.skipped_count, item_counts.skipped),
		    heartbeat_at = now(),
		    status = case
		      when item_counts.processed >= j.total_count and j.status not in ('canceled', 'failed') then 'completed'
		      else j.status
		    end,
		    finished_at = case
		      when item_counts.processed >= j.total_count and j.finished_at is null then now()
		      else j.finished_at
		    end
		from item_counts
		where j.id = item_counts.job_id
	`)
	return err
}

func scanImportJob(row rowScanner) (ImportJob, error) {
	var job ImportJob
	err := row.Scan(
		&job.ID,
		&job.Status,
		&job.ImportQueuePosition,
		&job.TotalCount,
		&job.ProcessedCount,
		&job.CreatedCount,
		&job.UpdatedCount,
		&job.SkippedCount,
		&job.FailedCount,
		&job.LastError,
		&job.SubmittedAt,
		&job.StartedAt,
		&job.HeartbeatAt,
		&job.FinishedAt,
	)
	return job, err
}

func scanImportItems(rows pgx.Rows) ([]ImportItem, error) {
	items := make([]ImportItem, 0)
	for rows.Next() {
		var item ImportItem
		var payload []byte
		var validated []byte
		if err := rows.Scan(
			&item.ID,
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
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			_ = json.Unmarshal(payload, &item.Payload)
		}
		if len(validated) > 0 {
			_ = json.Unmarshal(validated, &item.ValidatedPayload)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func refreshHashFromPayload(payload map[string]any) *string {
	for _, key := range []string{"refresh_token", "refreshToken", "access_token", "accessToken", "token"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			hash := hashString(strings.TrimSpace(value))
			return &hash
		}
	}
	return nil
}
