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
	OwnerUserID         int64      `json:"owner_user_id"`
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

type ImportBatchSummary struct {
	ImportJob
	TokenCount                     int      `json:"token_count"`
	Available                      int      `json:"available"`
	Cooling                        int      `json:"cooling"`
	Disabled                       int      `json:"disabled"`
	Missing                        int      `json:"missing"`
	TokenIDs                       []int64  `json:"token_ids"`
	ObservedCostUSD                *float64 `json:"observed_cost_usd"`
	AverageObservedCostUSD         *float64 `json:"average_observed_cost_usd,omitempty"`
	LocalObservedCostUSD           *float64 `json:"local_observed_cost_usd"`
	Sub2APIObservedCostUSD         *float64 `json:"sub2api_observed_cost_usd"`
	CombinedObservedCostUSD        *float64 `json:"combined_observed_cost_usd"`
	AverageCombinedObservedCostUSD *float64 `json:"average_combined_observed_cost_usd,omitempty"`
}

const importSummaryObservedCostTimeout = 6 * time.Second

type DeletedImportJob struct {
	ImportJob
	TokenIDs      []int64 `json:"token_ids"`
	DeletedTokens int64   `json:"deleted_tokens"`
}

type ImportItem struct {
	ID                         int64          `json:"id"`
	OwnerUserID                int64          `json:"owner_user_id"`
	JobID                      int64          `json:"job_id"`
	ItemIndex                  int            `json:"item_index"`
	Status                     string         `json:"status"`
	RefreshTokenHash           *string        `json:"refresh_token_hash,omitempty"`
	Payload                    map[string]any `json:"payload,omitempty"`
	ValidatedPayload           map[string]any `json:"validated_payload,omitempty"`
	TokenID                    *int64         `json:"token_id,omitempty"`
	Action                     *string        `json:"action,omitempty"`
	ErrorMessage               *string        `json:"error_message,omitempty"`
	ValidationMS               *int           `json:"validation_ms,omitempty"`
	PublishMS                  *int           `json:"publish_ms,omitempty"`
	ValidationStarted          *time.Time     `json:"validation_started_at,omitempty"`
	ValidationFinished         *time.Time     `json:"validation_finished_at,omitempty"`
	PublishedAt                *time.Time     `json:"published_at,omitempty"`
	MatchedExistingTokenID     *int64         `json:"matched_existing_token_id,omitempty"`
	PublishAttempted           bool           `json:"publish_attempted"`
	PublishSkippedReason       *string        `json:"publish_skipped_reason,omitempty"`
	Reactivated                bool           `json:"reactivated"`
	PreviousIsActive           *bool          `json:"previous_is_active,omitempty"`
	NextIsActive               *bool          `json:"next_is_active,omitempty"`
	PreviousDisabledAt         *time.Time     `json:"previous_disabled_at,omitempty"`
	NextDisabledAt             *time.Time     `json:"next_disabled_at,omitempty"`
	RefreshErrorCode           *string        `json:"refresh_error_code,omitempty"`
	RefreshErrorMessageExcerpt *string        `json:"refresh_error_message_excerpt,omitempty"`
	CreatedAt                  time.Time      `json:"created_at"`
	UpdatedAt                  time.Time      `json:"updated_at"`
}

type ImportItemUpdate struct {
	ID                         int64
	Status                     string
	ValidatedPayload           map[string]any
	TokenID                    *int64
	Action                     string
	ErrorMessage               string
	ValidationMS               *int
	PublishMS                  *int
	Published                  bool
	MatchedExistingTokenID     *int64
	PublishAttempted           bool
	PublishSkippedReason       string
	Reactivated                bool
	PreviousIsActive           *bool
	NextIsActive               *bool
	PreviousDisabledAt         *time.Time
	NextDisabledAt             *time.Time
	RefreshErrorCode           string
	RefreshErrorMessageExcerpt string
}

type ImportWorkerMetrics struct {
	Claimed   int64 `json:"claimed"`
	Validated int64 `json:"validated"`
	Published int64 `json:"published"`
	Failed    int64 `json:"failed"`
	Resumed   int64 `json:"resumed"`
}

func (s *Store) CreateCompletedImportJob(ctx context.Context, payloads []map[string]any, queuePosition string, result ImportResult) (ImportJob, error) {
	ownerID, err := s.BootstrapUserID(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	return s.CreateCompletedImportJobForOwner(ctx, ownerID, payloads, queuePosition, result)
}

func (s *Store) CreateCompletedImportJobForOwner(ctx context.Context, ownerUserID int64, payloads []map[string]any, queuePosition string, result ImportResult) (ImportJob, error) {
	if ownerUserID <= 0 {
		return ImportJob{}, fmt.Errorf("owner user id is required")
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
				owner_user_id, status, import_queue_position, payloads, total_count, processed_count,
				created_count, updated_count, skipped_count, failed_count,
				yielded_to_response_traffic_count, response_traffic_timeout_count,
				submitted_at, started_at, heartbeat_at, finished_at
			)
			values ($1, 'completed', $2, $3, $4, $5, $6, $7, $8, $9, 0, 0, now(), now(), now(), now())
			returning id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
			          created_count, updated_count, skipped_count, failed_count, last_error,
			          submitted_at, started_at, heartbeat_at, finished_at
	`, ownerUserID, queuePosition, data, len(payloads), result.Created+result.Updated+result.Skipped+result.Failed, result.Created, result.Updated, result.Skipped, result.Failed)
	job, err := scanImportJob(row)
	if err != nil {
		return ImportJob{}, err
	}
	if err := s.insertCompletedImportItems(ctx, tx, ownerUserID, job.ID, payloads, result); err != nil {
		return ImportJob{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ImportJob{}, err
	}
	return job, nil
}

func (s *Store) CreateQueuedImportJob(ctx context.Context, payloads []map[string]any, queuePosition string) (ImportJob, error) {
	ownerID, err := s.BootstrapUserID(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	return s.CreateQueuedImportJobForOwner(ctx, ownerID, payloads, queuePosition)
}

func (s *Store) CreateQueuedImportJobForOwner(ctx context.Context, ownerUserID int64, payloads []map[string]any, queuePosition string) (ImportJob, error) {
	if ownerUserID <= 0 {
		return ImportJob{}, fmt.Errorf("owner user id is required")
	}
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
				owner_user_id, status, import_queue_position, payloads, total_count,
				processed_count, created_count, updated_count, skipped_count, failed_count,
				yielded_to_response_traffic_count, response_traffic_timeout_count,
				submitted_at, heartbeat_at
			)
			values ($1, 'queued', $2, $3, $4, 0, 0, 0, 0, 0, 0, 0, now(), now())
			returning id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
			          created_count, updated_count, skipped_count, failed_count, last_error,
			          submitted_at, started_at, heartbeat_at, finished_at
	`, ownerUserID, queuePosition, data, len(payloads))
	job, err := scanImportJob(row)
	if err != nil {
		return ImportJob{}, err
	}
	batchValues := make([]string, 0, len(payloads))
	args := []any{job.ID, ownerUserID}
	for index, payload := range payloads {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return ImportJob{}, err
		}
		hash := refreshHashFromPayload(payload)
		args = append(args, index, encoded, hash)
		base := 3 + index*3
		batchValues = append(batchValues, fmt.Sprintf("($1, $2, $%d, 'queued', $%d, $%d)", base, base+1, base+2))
	}
	if len(batchValues) > 0 {
		_, err = tx.Exec(ctx, `
			insert into token_import_items(job_id, owner_user_id, item_index, status, payload, refresh_token_hash)
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
	return s.GetImportJobScoped(ctx, AllResources(), id)
}

func (s *Store) GetImportJobScoped(ctx context.Context, scope ResourceScope, id int64) (ImportJob, error) {
	args := []any{id}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	row := s.pool.QueryRow(ctx, `
		select id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		where id = $1
		  and `+ownerWhere, args...)
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
		returning id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
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

func (s *Store) DeleteImportJobWithTokens(ctx context.Context, id int64) (DeletedImportJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeletedImportJob{}, err
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		select id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		where id = $1
		for update
	`, id)
	job, err := scanImportJob(row)
	if err != nil {
		return DeletedImportJob{}, err
	}
	if importJobDeleteBlocked(job.Status) {
		return DeletedImportJob{}, fmt.Errorf("cannot delete %s import job", job.Status)
	}

	tokenIDs, err := s.importJobTokenIDsForDelete(ctx, tx, id)
	if err != nil {
		return DeletedImportJob{}, err
	}
	var deletedTokens int64
	if len(tokenIDs) > 0 {
		tag, err := tx.Exec(ctx, `
			delete from codex_tokens
			where id = any($1::integer[])
			   or merged_into_token_id = any($1::integer[])
		`, postgresIntIDs(tokenIDs))
		if err != nil {
			return DeletedImportJob{}, err
		}
		deletedTokens = tag.RowsAffected()
	}

	tag, err := tx.Exec(ctx, `delete from token_import_jobs where id = $1`, id)
	if err != nil {
		return DeletedImportJob{}, err
	}
	if tag.RowsAffected() == 0 {
		return DeletedImportJob{}, pgx.ErrNoRows
	}
	if err := tx.Commit(ctx); err != nil {
		return DeletedImportJob{}, err
	}
	return DeletedImportJob{
		ImportJob:     job,
		TokenIDs:      tokenIDs,
		DeletedTokens: deletedTokens,
	}, nil
}

func (s *Store) importJobTokenIDsForDelete(ctx context.Context, tx pgx.Tx, jobID int64) ([]int64, error) {
	tokenIDs, err := s.importJobTokenIDsFromItemsTx(ctx, tx, jobID)
	if err != nil {
		return nil, err
	}
	if len(tokenIDs) > 0 {
		return tokenIDs, nil
	}
	fallbackTokenIDs, err := s.importJobTokenIDsFromPayloadsTx(ctx, tx, jobID)
	if err != nil {
		return nil, err
	}
	return fallbackTokenIDs, nil
}

func (s *Store) importJobTokenIDsFromItemsTx(ctx context.Context, tx pgx.Tx, jobID int64) ([]int64, error) {
	rows, err := tx.Query(ctx, `
		select token_id
		from token_import_items
		where job_id = $1
		  and token_id is not null
		order by item_index asc, id asc
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tokenIDs := make([]int64, 0)
	for rows.Next() {
		var tokenID int64
		if err := rows.Scan(&tokenID); err != nil {
			return nil, err
		}
		tokenIDs = appendUniqueInt64(tokenIDs, tokenID)
	}
	return tokenIDs, rows.Err()
}

func (s *Store) importJobTokenIDsFromPayloadsTx(ctx context.Context, tx pgx.Tx, jobID int64) ([]int64, error) {
	var rawPayloads []byte
	if err := tx.QueryRow(ctx, `select payloads from token_import_jobs where id = $1`, jobID).Scan(&rawPayloads); err != nil {
		return nil, err
	}
	refreshTokens := storedRefreshTokensFromPayloads(rawPayloads)
	if len(refreshTokens) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
		select id
		from codex_tokens
		where refresh_token = any($1::text[])
		  and merged_into_token_id is null
	`, refreshTokens)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tokenIDs := make([]int64, 0)
	for rows.Next() {
		var tokenID int64
		if err := rows.Scan(&tokenID); err != nil {
			return nil, err
		}
		tokenIDs = appendUniqueInt64(tokenIDs, tokenID)
	}
	return tokenIDs, rows.Err()
}

func importJobDeleteBlocked(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued", "running", "validating", "publishing":
		return true
	default:
		return false
	}
}

func (s *Store) ListImportJobs(ctx context.Context, limit int) ([]ImportJob, error) {
	return s.ListImportJobsScoped(ctx, AllResources(), limit)
}

func (s *Store) ListImportJobsScoped(ctx context.Context, scope ResourceScope, limit int) ([]ImportJob, error) {
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	args := []any{limit}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select id, coalesce(owner_user_id, 0), status, import_queue_position, total_count, processed_count,
		       created_count, updated_count, skipped_count, failed_count, last_error,
		       submitted_at, started_at, heartbeat_at, finished_at
		from token_import_jobs
		where `+ownerWhere+`
		order by id desc
		limit $1
	`, args...)
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

func (s *Store) ListImportJobSummaries(ctx context.Context, limit int, includeObservedCost bool) ([]ImportBatchSummary, error) {
	return s.ListImportJobSummariesScoped(ctx, AllResources(), limit, includeObservedCost)
}

func (s *Store) ListImportJobSummariesScoped(ctx context.Context, scope ResourceScope, limit int, includeObservedCost bool) ([]ImportBatchSummary, error) {
	jobs, err := s.ListImportJobsScoped(ctx, scope, limit)
	if err != nil {
		return nil, err
	}
	return s.importJobSummaries(ctx, jobs, includeObservedCost)
}

// ImportJobSummaries summarizes an already authorized and paginated job set in
// bulk. Callers remain responsible for applying the appropriate resource scope
// when loading jobs.
func (s *Store) ImportJobSummaries(ctx context.Context, jobs []ImportJob, includeObservedCost bool) ([]ImportBatchSummary, error) {
	return s.importJobSummaries(ctx, jobs, includeObservedCost)
}

func (s *Store) GetImportJobSummary(ctx context.Context, id int64, includeObservedCost bool) (*ImportBatchSummary, error) {
	return s.GetImportJobSummaryScoped(ctx, AllResources(), id, includeObservedCost)
}

func (s *Store) GetImportJobSummaryScoped(ctx context.Context, scope ResourceScope, id int64, includeObservedCost bool) (*ImportBatchSummary, error) {
	job, err := s.GetImportJobScoped(ctx, scope, id)
	if err != nil {
		return nil, err
	}
	summaries, err := s.importJobSummaries(ctx, []ImportJob{job}, includeObservedCost)
	if err != nil {
		return nil, err
	}
	if len(summaries) == 0 {
		return nil, pgx.ErrNoRows
	}
	return &summaries[0], nil
}

func (s *Store) ListImportJobItems(ctx context.Context, jobID int64) ([]ImportItem, error) {
	return s.ListImportJobItemsScoped(ctx, AllResources(), jobID)
}

func (s *Store) ListImportJobItemsScoped(ctx context.Context, scope ResourceScope, jobID int64) ([]ImportItem, error) {
	args := []any{jobID}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select `+importItemSelectColumns("")+`
		from token_import_items
		where job_id = $1
		  and `+ownerWhere+`
		order by item_index asc, id asc
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanImportItems(rows)
}

func (s *Store) insertCompletedImportItems(ctx context.Context, tx pgx.Tx, ownerUserID int64, jobID int64, payloads []map[string]any, result ImportResult) error {
	if len(payloads) == 0 || len(result.Items) == 0 {
		return nil
	}
	args := []any{jobID, ownerUserID}
	values := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		if item.Index < 0 || item.Index >= len(payloads) {
			continue
		}
		payload := payloads[item.Index]
		status := strings.TrimSpace(item.Status)
		if status == "" {
			if item.TokenID != nil {
				status = "published"
			} else {
				status = "skipped"
			}
		}
		payloadData := jsonBytes(payload)
		hash := refreshHashFromPayload(payload)
		base := len(args) + 1
		args = append(args, item.Index, status, payloadData, payloadData, hash, item.TokenID, item.Action, item.ErrorMessage,
			item.MatchedExistingTokenID, status == "published", item.Reactivated, item.PreviousIsActive, item.NextIsActive,
			item.PreviousDisabledAt, item.NextDisabledAt, item.RefreshErrorCode, item.RefreshErrorMessage)
		values = append(values, fmt.Sprintf(
			"($1, $2, $%d, $%d, $%d, $%d, $%d, $%d, nullif($%d, ''), nullif($%d, ''), now(), $%d, $%d, $%d, $%d, $%d, $%d, $%d, nullif($%d, ''), nullif($%d, ''), now())",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12, base+13, base+14, base+15, base+16,
		))
	}
	if len(values) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		insert into token_import_items(
			job_id, owner_user_id, item_index, status, payload, validated_payload,
			refresh_token_hash, token_id, action, error_message, published_at,
			matched_existing_token_id, publish_attempted, reactivated, previous_is_active, next_is_active,
			previous_disabled_at, next_disabled_at, refresh_error_code, refresh_error_message_excerpt, updated_at
		)
		values `+strings.Join(values, ",")+`
		on conflict (job_id, item_index) do update
		set status = excluded.status,
		    payload = excluded.payload,
		    validated_payload = excluded.validated_payload,
		    refresh_token_hash = excluded.refresh_token_hash,
		    token_id = excluded.token_id,
		    action = excluded.action,
		    error_message = excluded.error_message,
		    published_at = excluded.published_at,
		    matched_existing_token_id = excluded.matched_existing_token_id,
		    publish_attempted = excluded.publish_attempted,
		    reactivated = excluded.reactivated,
		    previous_is_active = excluded.previous_is_active,
		    next_is_active = excluded.next_is_active,
		    previous_disabled_at = excluded.previous_disabled_at,
		    next_disabled_at = excluded.next_disabled_at,
		    refresh_error_code = excluded.refresh_error_code,
		    refresh_error_message_excerpt = excluded.refresh_error_message_excerpt,
		    updated_at = now()
	`, args...)
	return err
}

func (s *Store) importJobSummaries(ctx context.Context, jobs []ImportJob, includeObservedCost bool) ([]ImportBatchSummary, error) {
	if len(jobs) == 0 {
		return []ImportBatchSummary{}, nil
	}
	summaries := make([]ImportBatchSummary, len(jobs))
	jobIDs := make([]int64, 0, len(jobs))
	for index, job := range jobs {
		summaries[index] = ImportBatchSummary{
			ImportJob: job,
			TokenIDs:  []int64{},
		}
		jobIDs = append(jobIDs, job.ID)
	}

	tokenIDsByJob, err := s.importJobTokenIDsFromItems(ctx, jobIDs)
	if err != nil {
		return nil, err
	}
	fallbackJobIDs := make([]int64, 0)
	for _, job := range jobs {
		if len(tokenIDsByJob[job.ID]) == 0 {
			fallbackJobIDs = append(fallbackJobIDs, job.ID)
		}
	}
	if len(fallbackJobIDs) > 0 {
		fallbackTokenIDs, err := s.importJobTokenIDsFromPayloads(ctx, fallbackJobIDs)
		if err != nil {
			return nil, err
		}
		for jobID, tokenIDs := range fallbackTokenIDs {
			tokenIDsByJob[jobID] = tokenIDs
		}
	}

	allTokenIDs := make([]int64, 0)
	seenTokenIDs := make(map[int64]struct{})
	for index := range summaries {
		tokenIDs := tokenIDsByJob[summaries[index].ID]
		summaries[index].TokenIDs = append([]int64(nil), tokenIDs...)
		for _, tokenID := range tokenIDs {
			if tokenID <= 0 {
				continue
			}
			if _, exists := seenTokenIDs[tokenID]; exists {
				continue
			}
			seenTokenIDs[tokenID] = struct{}{}
			allTokenIDs = append(allTokenIDs, tokenID)
		}
	}

	tokens, err := s.listImportSummaryTokensByIDs(ctx, allTokenIDs)
	if err != nil {
		return nil, err
	}
	tokenByID := make(map[int64]Token, len(tokens))
	for _, token := range tokens {
		tokenByID[token.ID] = token
	}

	costByTokenID := map[int64]*float64{}
	remoteUsageByTokenID := map[int64]float64{}
	observedCostLoaded := false
	remoteUsageLoaded := false
	if includeObservedCost && len(tokens) > 0 {
		costByTokenID, observedCostLoaded = s.importSummaryObservedCosts(ctx, tokens)
		remoteUsageByTokenID, remoteUsageLoaded = s.importSummarySub2APIUsage(ctx, tokens)
	}

	now := time.Now()
	for index := range summaries {
		present := 0
		totalCost := 0.0
		remoteCost := 0.0
		for _, tokenID := range summaries[index].TokenIDs {
			token, ok := tokenByID[tokenID]
			if !ok {
				summaries[index].Missing++
				continue
			}
			present++
			switch {
			case !token.IsActive || token.DisabledAt != nil:
				summaries[index].Disabled++
			case token.CooldownUntil != nil && token.CooldownUntil.After(now):
				summaries[index].Cooling++
			default:
				summaries[index].Available++
			}
			if includeObservedCost {
				if value := costByTokenID[tokenID]; value != nil {
					totalCost += *value
				}
				remoteCost += remoteUsageByTokenID[tokenID]
			}
		}
		summaries[index].TokenCount = present
		if includeObservedCost && observedCostLoaded {
			observedCost := totalCost
			summaries[index].ObservedCostUSD = &observedCost
			summaries[index].LocalObservedCostUSD = &observedCost
			if present > 0 {
				average := totalCost / float64(present)
				summaries[index].AverageObservedCostUSD = &average
			}
		}
		if includeObservedCost && remoteUsageLoaded {
			remote := remoteCost
			summaries[index].Sub2APIObservedCostUSD = &remote
		}
		if includeObservedCost && observedCostLoaded && remoteUsageLoaded {
			combined := totalCost + remoteCost
			summaries[index].CombinedObservedCostUSD = &combined
			if present > 0 {
				average := combined / float64(present)
				summaries[index].AverageCombinedObservedCostUSD = &average
			}
		} else if includeObservedCost && observedCostLoaded {
			combined := totalCost
			summaries[index].CombinedObservedCostUSD = &combined
		}
	}
	return summaries, nil
}

func (s *Store) listImportSummaryTokensByIDs(ctx context.Context, tokenIDs []int64) ([]Token, error) {
	ids := postgresIntIDs(tokenIDs)
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		select id, is_active, cooldown_until, disabled_at
		from codex_tokens
		where id = any($1::integer[])
		  and merged_into_token_id is null
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tokens := make([]Token, 0, len(ids))
	for rows.Next() {
		var token Token
		if err := rows.Scan(&token.ID, &token.IsActive, &token.CooldownUntil, &token.DisabledAt); err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
	return tokens, rows.Err()
}

func (s *Store) importSummarySub2APIUsage(ctx context.Context, tokens []Token) (map[int64]float64, bool) {
	if len(tokens) == 0 {
		return map[int64]float64{}, true
	}
	usageCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), importSummaryObservedCostTimeout)
	defer cancel()
	usage, err := s.Sub2APIAccountCostsByTokens(usageCtx, tokens)
	if err != nil {
		return map[int64]float64{}, false
	}
	return usage, true
}

func (s *Store) importSummaryObservedCosts(ctx context.Context, tokens []Token) (map[int64]*float64, bool) {
	if len(tokens) == 0 {
		return map[int64]*float64{}, true
	}
	costCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), importSummaryObservedCostTimeout)
	defer cancel()
	costs, err := s.TokenObservedCostsCurrentSnapshot(costCtx, tokens)
	if err != nil && !IsObservedCostsPartialError(err) {
		return map[int64]*float64{}, false
	}
	return costs, true
}

func (s *Store) importJobTokenIDsFromItems(ctx context.Context, jobIDs []int64) (map[int64][]int64, error) {
	result := make(map[int64][]int64, len(jobIDs))
	ids := postgresIntIDs(jobIDs)
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := s.pool.Query(ctx, `
		select job_id, token_id
		from token_import_items
		where job_id = any($1::integer[])
		  and token_id is not null
		order by job_id asc, item_index asc, id asc
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var jobID int64
		var tokenID int64
		if err := rows.Scan(&jobID, &tokenID); err != nil {
			return nil, err
		}
		result[jobID] = appendUniqueInt64(result[jobID], tokenID)
	}
	return result, rows.Err()
}

func (s *Store) importJobTokenIDsFromPayloads(ctx context.Context, jobIDs []int64) (map[int64][]int64, error) {
	result := make(map[int64][]int64, len(jobIDs))
	ids := postgresIntIDs(jobIDs)
	if len(ids) == 0 {
		return result, nil
	}
	rows, err := s.pool.Query(ctx, `
		select id, payloads
		from token_import_jobs
		where id = any($1::integer[])
	`, ids)
	if err != nil {
		return nil, err
	}
	refreshesByJob := make(map[int64][]string, len(jobIDs))
	allRefreshes := make([]string, 0)
	for rows.Next() {
		var jobID int64
		var rawPayloads []byte
		if err := rows.Scan(&jobID, &rawPayloads); err != nil {
			rows.Close()
			return nil, err
		}
		for _, refreshToken := range storedRefreshTokensFromPayloads(rawPayloads) {
			refreshesByJob[jobID] = appendUniqueString(refreshesByJob[jobID], refreshToken)
			allRefreshes = appendUniqueString(allRefreshes, refreshToken)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(allRefreshes) == 0 {
		return result, nil
	}

	tokenRows, err := s.pool.Query(ctx, `
		select refresh_token, id
		from codex_tokens
		where refresh_token = any($1::text[])
		  and merged_into_token_id is null
	`, allRefreshes)
	if err != nil {
		return nil, err
	}
	defer tokenRows.Close()
	tokenIDByRefresh := make(map[string]int64, len(allRefreshes))
	for tokenRows.Next() {
		var refreshToken string
		var tokenID int64
		if err := tokenRows.Scan(&refreshToken, &tokenID); err != nil {
			return nil, err
		}
		tokenIDByRefresh[refreshToken] = tokenID
	}
	if err := tokenRows.Err(); err != nil {
		return nil, err
	}
	for jobID, refreshTokens := range refreshesByJob {
		for _, refreshToken := range refreshTokens {
			if tokenID, ok := tokenIDByRefresh[refreshToken]; ok {
				result[jobID] = appendUniqueInt64(result[jobID], tokenID)
			}
		}
	}
	return result, nil
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
		returning `+importItemSelectColumns("i")+`
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
			    matched_existing_token_id = coalesce($10, matched_existing_token_id),
			    publish_attempted = publish_attempted or $11,
			    publish_skipped_reason = coalesce(nullif($12, ''), publish_skipped_reason),
			    reactivated = reactivated or $13,
			    previous_is_active = coalesce($14, previous_is_active),
			    next_is_active = coalesce($15, next_is_active),
			    previous_disabled_at = coalesce($16, previous_disabled_at),
			    next_disabled_at = coalesce($17, next_disabled_at),
			    refresh_error_code = coalesce(nullif($18, ''), refresh_error_code),
			    refresh_error_message_excerpt = coalesce(nullif($19, ''), refresh_error_message_excerpt),
			    updated_at = now()
			where id = $1
		`, update.ID, status, payload, update.TokenID, update.Action, update.ErrorMessage, update.ValidationMS, update.PublishMS,
			update.Published, update.MatchedExistingTokenID, update.PublishAttempted, update.PublishSkippedReason, update.Reactivated,
			update.PreviousIsActive, update.NextIsActive, update.PreviousDisabledAt, update.NextDisabledAt,
			update.RefreshErrorCode, update.RefreshErrorMessageExcerpt)
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
	job, err := s.GetImportJob(ctx, jobID)
	if err != nil {
		return ImportResult{}, err
	}
	result, err := s.UpsertAccessTokensForOwner(ctx, job.OwnerUserID, accessTokens, source)
	if err != nil {
		return result, err
	}
	return result, s.recordImportPublishResult(ctx, jobID, result)
}

func (s *Store) PublishImportPayloads(ctx context.Context, jobID int64, payloads []map[string]any, source string) (ImportResult, error) {
	job, err := s.GetImportJob(ctx, jobID)
	if err != nil {
		return ImportResult{}, err
	}
	result, err := s.UpsertTokenPayloadsForOwner(ctx, job.OwnerUserID, payloads, source)
	if err != nil {
		return result, err
	}
	return result, s.recordImportPublishResult(ctx, jobID, result)
}

func (s *Store) recordImportPublishResult(ctx context.Context, jobID int64, result ImportResult) error {
	_, err := s.pool.Exec(ctx, `
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
	return err
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
		&job.OwnerUserID,
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

func importItemSelectColumns(alias string) string {
	return strings.Join(importItemSelectColumnList(alias), ", ")
}

func importItemSelectColumnList(alias string) []string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	return []string{
		prefix + "id",
		"coalesce(" + prefix + "owner_user_id, 0)",
		prefix + "job_id",
		prefix + "item_index",
		prefix + "status",
		prefix + "refresh_token_hash",
		prefix + "payload",
		prefix + "validated_payload",
		prefix + "token_id",
		prefix + "action",
		prefix + "error_message",
		prefix + "validation_ms",
		prefix + "publish_ms",
		prefix + "validation_started_at",
		prefix + "validation_finished_at",
		prefix + "published_at",
		prefix + "matched_existing_token_id",
		prefix + "publish_attempted",
		prefix + "publish_skipped_reason",
		prefix + "reactivated",
		prefix + "previous_is_active",
		prefix + "next_is_active",
		prefix + "previous_disabled_at",
		prefix + "next_disabled_at",
		prefix + "refresh_error_code",
		prefix + "refresh_error_message_excerpt",
		prefix + "created_at",
		prefix + "updated_at",
	}
}

func refreshHashFromPayload(payload map[string]any) *string {
	if value := tokenIdentityFromPayload(payload); value != "" {
		hash := hashString(value)
		return &hash
	}
	return nil
}

func storedRefreshTokensFromPayloads(rawPayloads []byte) []string {
	var values []any
	if len(rawPayloads) == 0 || json.Unmarshal(rawPayloads, &values) != nil {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		switch typed := value.(type) {
		case string:
			if token := strings.TrimSpace(typed); token != "" {
				if looksLikeAccessTokenText(token) {
					out = append(out, accessTokenOnlyRefreshToken(token))
				} else {
					out = append(out, token)
				}
			}
		case map[string]any:
			if token := storedRefreshTokenFromPayload(typed); token != "" {
				out = append(out, token)
			}
		}
	}
	return out
}

func importAccessTokenFromPayload(payload map[string]any) string {
	for _, key := range []string{"access_token", "accessToken"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if value, ok := payload["token"].(string); ok && looksLikeAccessTokenText(value) {
		return strings.TrimSpace(value)
	}
	return ""
}

func storedRefreshTokenFromPayload(payload map[string]any) string {
	for _, key := range []string{"refresh_token", "refreshToken"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if accessToken := importAccessTokenFromPayload(payload); accessToken != "" {
		return accessTokenOnlyRefreshToken(accessToken)
	}
	if value, ok := payload["token"].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return ""
}

func tokenIdentityFromPayload(payload map[string]any) string {
	for _, key := range []string{"refresh_token", "refreshToken", "access_token", "accessToken", "token"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func looksLikeAccessTokenText(value string) bool {
	value = strings.TrimSpace(value)
	return strings.HasPrefix(value, "eyJ")
}

func appendUniqueInt64(values []int64, value int64) []int64 {
	if value <= 0 {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
