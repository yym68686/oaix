package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type RequestLog struct {
	RequestID                     string         `json:"request_id"`
	Endpoint                      string         `json:"endpoint"`
	Model                         *string        `json:"model,omitempty"`
	ModelName                     *string        `json:"model_name,omitempty"`
	IsStream                      bool           `json:"is_stream"`
	StatusCode                    *int           `json:"status_code,omitempty"`
	Success                       *bool          `json:"success,omitempty"`
	AttemptCount                  int            `json:"attempt_count"`
	TokenID                       *int64         `json:"token_id,omitempty"`
	AccountID                     *string        `json:"account_id,omitempty"`
	ClientIP                      *string        `json:"client_ip,omitempty"`
	UserAgent                     *string        `json:"user_agent,omitempty"`
	StartedAt                     time.Time      `json:"started_at"`
	FinishedAt                    *time.Time     `json:"finished_at,omitempty"`
	FirstTokenAt                  *time.Time     `json:"first_token_at,omitempty"`
	TTFTMs                        *int           `json:"ttft_ms,omitempty"`
	DurationMs                    *int           `json:"duration_ms,omitempty"`
	TimingSpans                   map[string]any `json:"timing_spans,omitempty"`
	InputTokens                   *int           `json:"input_tokens,omitempty"`
	CachedInputTokens             *int           `json:"cached_input_tokens,omitempty"`
	OutputTokens                  *int           `json:"output_tokens,omitempty"`
	TotalTokens                   *int           `json:"total_tokens,omitempty"`
	EstimatedCostUSD              *float64       `json:"estimated_cost_usd,omitempty"`
	RequestPayloadHash            *string        `json:"request_payload_hash,omitempty"`
	UpstreamPayloadHash           *string        `json:"upstream_payload_hash,omitempty"`
	PromptTemplateHash            *string        `json:"prompt_template_hash,omitempty"`
	PromptDynamicHash             *string        `json:"prompt_dynamic_hash,omitempty"`
	PromptCacheSource             *string        `json:"prompt_cache_source,omitempty"`
	PromptCacheKeyHash            *string        `json:"prompt_cache_key_hash,omitempty"`
	PromptCacheRetentionRequested *string        `json:"prompt_cache_retention_requested,omitempty"`
	PromptCacheRetentionSent      *string        `json:"prompt_cache_retention_sent,omitempty"`
	SessionIDHash                 *string        `json:"session_id_hash,omitempty"`
	SessionIDSource               *string        `json:"session_id_source,omitempty"`
	PreviousResponseIDHash        *string        `json:"previous_response_id_hash,omitempty"`
	UpstreamResponseID            *string        `json:"upstream_response_id,omitempty"`
	CacheHitRatio                 *float64       `json:"cache_hit_ratio,omitempty"`
	CacheAffinityResult           *string        `json:"cache_affinity_result,omitempty"`
	CacheAffinityLaneIndex        *int           `json:"cache_affinity_lane_index,omitempty"`
	PromptCacheTrace              map[string]any `json:"prompt_cache_trace,omitempty"`
	ErrorMessage                  *string        `json:"error_message,omitempty"`
}

type RequestLogSummary struct {
	Total        int     `json:"total"`
	Success      int     `json:"success"`
	Failure      int     `json:"failure"`
	AverageTTFT  float64 `json:"average_ttft_ms"`
	AverageDurMS float64 `json:"average_duration_ms"`
}

type RequestAnalytics struct {
	Hours   int               `json:"hours"`
	Summary RequestLogSummary `json:"summary"`
	ByModel []ModelStat       `json:"by_model"`
}

type ModelStat struct {
	ModelName string `json:"model_name"`
	Requests  int    `json:"requests"`
	Success   int    `json:"success"`
	Failure   int    `json:"failure"`
}

func (s *Store) UpsertRequestLogs(ctx context.Context, logs []RequestLog) error {
	if len(logs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, item := range logs {
		if strings.TrimSpace(item.RequestID) == "" {
			continue
		}
		modelName := item.ModelName
		if modelName == nil {
			modelName = item.Model
		}
		timing := jsonBytes(item.TimingSpans)
		promptTrace := jsonBytes(item.PromptCacheTrace)
		batch.Queue(`
			insert into gateway_request_logs (
				request_id, endpoint, model, model_name, is_stream, status_code, success,
				attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
				first_token_at, ttft_ms, duration_ms, timing_spans, input_tokens, cached_input_tokens,
				output_tokens, total_tokens, estimated_cost_usd, request_payload_hash, upstream_payload_hash,
				prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
				prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
				session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
				cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message
			)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)
			on conflict (request_id) do update set
				endpoint = excluded.endpoint,
				model = coalesce(excluded.model, gateway_request_logs.model),
				model_name = coalesce(excluded.model_name, gateway_request_logs.model_name, gateway_request_logs.model),
				is_stream = excluded.is_stream,
				status_code = coalesce(excluded.status_code, gateway_request_logs.status_code),
				success = coalesce(excluded.success, gateway_request_logs.success),
				attempt_count = greatest(gateway_request_logs.attempt_count, excluded.attempt_count),
				token_id = coalesce(excluded.token_id, gateway_request_logs.token_id),
				account_id = coalesce(excluded.account_id, gateway_request_logs.account_id),
				client_ip = coalesce(excluded.client_ip, gateway_request_logs.client_ip),
				user_agent = coalesce(excluded.user_agent, gateway_request_logs.user_agent),
				started_at = least(gateway_request_logs.started_at, excluded.started_at),
				finished_at = coalesce(excluded.finished_at, gateway_request_logs.finished_at),
				first_token_at = coalesce(gateway_request_logs.first_token_at, excluded.first_token_at),
				ttft_ms = coalesce(excluded.ttft_ms, gateway_request_logs.ttft_ms),
				duration_ms = coalesce(excluded.duration_ms, gateway_request_logs.duration_ms),
				timing_spans = coalesce(excluded.timing_spans, gateway_request_logs.timing_spans),
				input_tokens = coalesce(excluded.input_tokens, gateway_request_logs.input_tokens),
				cached_input_tokens = coalesce(excluded.cached_input_tokens, gateway_request_logs.cached_input_tokens),
				output_tokens = coalesce(excluded.output_tokens, gateway_request_logs.output_tokens),
				total_tokens = coalesce(excluded.total_tokens, gateway_request_logs.total_tokens),
				estimated_cost_usd = coalesce(excluded.estimated_cost_usd, gateway_request_logs.estimated_cost_usd),
				request_payload_hash = coalesce(excluded.request_payload_hash, gateway_request_logs.request_payload_hash),
				upstream_payload_hash = coalesce(excluded.upstream_payload_hash, gateway_request_logs.upstream_payload_hash),
				prompt_template_hash = coalesce(excluded.prompt_template_hash, gateway_request_logs.prompt_template_hash),
				prompt_dynamic_hash = coalesce(excluded.prompt_dynamic_hash, gateway_request_logs.prompt_dynamic_hash),
				prompt_cache_source = coalesce(excluded.prompt_cache_source, gateway_request_logs.prompt_cache_source),
				prompt_cache_key_hash = coalesce(excluded.prompt_cache_key_hash, gateway_request_logs.prompt_cache_key_hash),
				prompt_cache_retention_requested = coalesce(excluded.prompt_cache_retention_requested, gateway_request_logs.prompt_cache_retention_requested),
				prompt_cache_retention_sent = coalesce(excluded.prompt_cache_retention_sent, gateway_request_logs.prompt_cache_retention_sent),
				session_id_hash = coalesce(excluded.session_id_hash, gateway_request_logs.session_id_hash),
				session_id_source = coalesce(excluded.session_id_source, gateway_request_logs.session_id_source),
				previous_response_id_hash = coalesce(excluded.previous_response_id_hash, gateway_request_logs.previous_response_id_hash),
				upstream_response_id = coalesce(excluded.upstream_response_id, gateway_request_logs.upstream_response_id),
				cache_hit_ratio = coalesce(excluded.cache_hit_ratio, gateway_request_logs.cache_hit_ratio),
				cache_affinity_result = coalesce(excluded.cache_affinity_result, gateway_request_logs.cache_affinity_result),
				cache_affinity_lane_index = coalesce(excluded.cache_affinity_lane_index, gateway_request_logs.cache_affinity_lane_index),
				prompt_cache_trace = coalesce(excluded.prompt_cache_trace, gateway_request_logs.prompt_cache_trace),
				error_message = coalesce(excluded.error_message, gateway_request_logs.error_message)
		`,
			item.RequestID, item.Endpoint, item.Model, modelName, item.IsStream, item.StatusCode, item.Success,
			item.AttemptCount, item.TokenID, item.AccountID, item.ClientIP, item.UserAgent, item.StartedAt,
			item.FinishedAt, item.FirstTokenAt, item.TTFTMs, item.DurationMs, timing, item.InputTokens,
			item.CachedInputTokens, item.OutputTokens, item.TotalTokens, item.EstimatedCostUSD, item.RequestPayloadHash,
			item.UpstreamPayloadHash, item.PromptTemplateHash, item.PromptDynamicHash, item.PromptCacheSource,
			item.PromptCacheKeyHash, item.PromptCacheRetentionRequested, item.PromptCacheRetentionSent,
			item.SessionIDHash, item.SessionIDSource, item.PreviousResponseIDHash, item.UpstreamResponseID,
			item.CacheHitRatio, item.CacheAffinityResult, item.CacheAffinityLaneIndex, promptTrace, item.ErrorMessage,
		)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) EnqueueRequestLogOutbox(ctx context.Context, item RequestLog) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		insert into request_log_outbox(request_id, payload, next_attempt_at)
		values ($1, $2, now())
	`, item.RequestID, payload)
	return err
}

func (s *Store) DrainRequestLogOutbox(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 500
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		select id, payload
		from request_log_outbox
		where next_attempt_at <= now()
		order by id
		limit $1
		for update skip locked
	`, limit)
	if err != nil {
		return 0, err
	}
	type outboxItem struct {
		id      int64
		payload []byte
	}
	var items []outboxItem
	for rows.Next() {
		var item outboxItem
		if err := rows.Scan(&item.id, &item.payload); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, tx.Commit(ctx)
	}
	logs := make([]RequestLog, 0, len(items))
	ids := make([]string, 0, len(items))
	args := make([]any, 0, len(items))
	for index, item := range items {
		var logItem RequestLog
		if err := json.Unmarshal(item.payload, &logItem); err != nil {
			return 0, err
		}
		logs = append(logs, logItem)
		args = append(args, item.id)
		ids = append(ids, fmt.Sprintf("$%d", index+1))
	}
	if err := upsertRequestLogsTx(ctx, tx, logs); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, "delete from request_log_outbox where id in ("+strings.Join(ids, ",")+")", args...); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(items), nil
}

func upsertRequestLogsTx(ctx context.Context, tx pgx.Tx, logs []RequestLog) error {
	for _, item := range logs {
		modelName := item.ModelName
		if modelName == nil {
			modelName = item.Model
		}
		promptTrace := jsonBytes(item.PromptCacheTrace)
		_, err := tx.Exec(ctx, `
			insert into gateway_request_logs (
				request_id, endpoint, model, model_name, is_stream, status_code, success,
				attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
				first_token_at, ttft_ms, duration_ms, timing_spans, input_tokens, cached_input_tokens,
				output_tokens, total_tokens, estimated_cost_usd, request_payload_hash, upstream_payload_hash,
				prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
				prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
				session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
				cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message
			)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)
			on conflict (request_id) do update set
				status_code = coalesce(excluded.status_code, gateway_request_logs.status_code),
				success = coalesce(excluded.success, gateway_request_logs.success),
				attempt_count = greatest(gateway_request_logs.attempt_count, excluded.attempt_count),
				token_id = coalesce(excluded.token_id, gateway_request_logs.token_id),
				finished_at = coalesce(excluded.finished_at, gateway_request_logs.finished_at),
				first_token_at = coalesce(gateway_request_logs.first_token_at, excluded.first_token_at),
				ttft_ms = coalesce(excluded.ttft_ms, gateway_request_logs.ttft_ms),
				duration_ms = coalesce(excluded.duration_ms, gateway_request_logs.duration_ms),
				timing_spans = coalesce(excluded.timing_spans, gateway_request_logs.timing_spans),
				input_tokens = coalesce(excluded.input_tokens, gateway_request_logs.input_tokens),
				cached_input_tokens = coalesce(excluded.cached_input_tokens, gateway_request_logs.cached_input_tokens),
				output_tokens = coalesce(excluded.output_tokens, gateway_request_logs.output_tokens),
				total_tokens = coalesce(excluded.total_tokens, gateway_request_logs.total_tokens),
				estimated_cost_usd = coalesce(excluded.estimated_cost_usd, gateway_request_logs.estimated_cost_usd),
				request_payload_hash = coalesce(excluded.request_payload_hash, gateway_request_logs.request_payload_hash),
				upstream_payload_hash = coalesce(excluded.upstream_payload_hash, gateway_request_logs.upstream_payload_hash),
				prompt_template_hash = coalesce(excluded.prompt_template_hash, gateway_request_logs.prompt_template_hash),
				prompt_dynamic_hash = coalesce(excluded.prompt_dynamic_hash, gateway_request_logs.prompt_dynamic_hash),
				prompt_cache_source = coalesce(excluded.prompt_cache_source, gateway_request_logs.prompt_cache_source),
				prompt_cache_key_hash = coalesce(excluded.prompt_cache_key_hash, gateway_request_logs.prompt_cache_key_hash),
				prompt_cache_retention_requested = coalesce(excluded.prompt_cache_retention_requested, gateway_request_logs.prompt_cache_retention_requested),
				prompt_cache_retention_sent = coalesce(excluded.prompt_cache_retention_sent, gateway_request_logs.prompt_cache_retention_sent),
				session_id_hash = coalesce(excluded.session_id_hash, gateway_request_logs.session_id_hash),
				session_id_source = coalesce(excluded.session_id_source, gateway_request_logs.session_id_source),
				previous_response_id_hash = coalesce(excluded.previous_response_id_hash, gateway_request_logs.previous_response_id_hash),
				upstream_response_id = coalesce(excluded.upstream_response_id, gateway_request_logs.upstream_response_id),
				cache_hit_ratio = coalesce(excluded.cache_hit_ratio, gateway_request_logs.cache_hit_ratio),
				cache_affinity_result = coalesce(excluded.cache_affinity_result, gateway_request_logs.cache_affinity_result),
				cache_affinity_lane_index = coalesce(excluded.cache_affinity_lane_index, gateway_request_logs.cache_affinity_lane_index),
				prompt_cache_trace = coalesce(excluded.prompt_cache_trace, gateway_request_logs.prompt_cache_trace),
				error_message = coalesce(excluded.error_message, gateway_request_logs.error_message)
		`,
			item.RequestID, item.Endpoint, item.Model, modelName, item.IsStream, item.StatusCode, item.Success,
			item.AttemptCount, item.TokenID, item.AccountID, item.ClientIP, item.UserAgent, item.StartedAt,
			item.FinishedAt, item.FirstTokenAt, item.TTFTMs, item.DurationMs, jsonBytes(item.TimingSpans),
			item.InputTokens, item.CachedInputTokens, item.OutputTokens, item.TotalTokens, item.EstimatedCostUSD,
			item.RequestPayloadHash, item.UpstreamPayloadHash, item.PromptTemplateHash, item.PromptDynamicHash,
			item.PromptCacheSource, item.PromptCacheKeyHash, item.PromptCacheRetentionRequested,
			item.PromptCacheRetentionSent, item.SessionIDHash, item.SessionIDSource, item.PreviousResponseIDHash,
			item.UpstreamResponseID, item.CacheHitRatio, item.CacheAffinityResult, item.CacheAffinityLaneIndex,
			promptTrace, item.ErrorMessage,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) RequestLogSummary(ctx context.Context, hours int) (RequestLogSummary, error) {
	if hours <= 0 {
		hours = 24
	}
	var summary RequestLogSummary
	err := s.pool.QueryRow(ctx, `
		select
			coalesce(sum(request_count), 0)::int,
			coalesce(sum(success_count), 0)::int,
			coalesce(sum(failure_count), 0)::int,
			case when coalesce(sum(ttft_count), 0) > 0
				then coalesce(sum(ttft_ms_sum), 0)::float8 / sum(ttft_count)
				else 0
			end,
			case when coalesce(sum(duration_count), 0) > 0
				then coalesce(sum(duration_ms_sum), 0)::float8 / sum(duration_count)
				else 0
			end
		from gateway_request_hourly_stats
		where bucket_start >= date_trunc('hour', now() - make_interval(hours => $1))
	`, hours).Scan(&summary.Total, &summary.Success, &summary.Failure, &summary.AverageTTFT, &summary.AverageDurMS)
	return summary, err
}

func (s *Store) AggregateRequestHourlyStats(ctx context.Context) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		with pending as (
			select
				id,
				date_trunc('hour', started_at) as bucket_start,
				coalesce(model_name, model, '') as model_name,
				success,
				is_stream,
				coalesce(input_tokens, 0) as input_tokens,
				coalesce(output_tokens, 0) as output_tokens,
				coalesce(total_tokens, 0) as total_tokens,
				coalesce(estimated_cost_usd, 0) as estimated_cost_usd,
				ttft_ms,
				duration_ms
			from gateway_request_logs
			where analytics_recorded_at is null
			  and finished_at is not null
			order by id
			limit 5000
			for update skip locked
		),
		agg as (
			select
				bucket_start,
				model_name,
				count(*)::int as request_count,
				count(*) filter (where success = true)::int as success_count,
				count(*) filter (where success = false)::int as failure_count,
				count(*) filter (where is_stream = true)::int as streaming_count,
				sum(input_tokens)::int as input_tokens,
				sum(output_tokens)::int as output_tokens,
				sum(total_tokens)::int as total_tokens,
				sum(estimated_cost_usd)::float8 as estimated_cost_usd,
				coalesce(sum(ttft_ms) filter (where ttft_ms is not null), 0)::int as ttft_ms_sum,
				count(ttft_ms)::int as ttft_count,
				coalesce(sum(duration_ms) filter (where duration_ms is not null), 0)::int as duration_ms_sum,
				count(duration_ms)::int as duration_count
			from pending
			group by bucket_start, model_name
		)
		insert into gateway_request_hourly_stats (
			bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
			input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, updated_at
		)
		select bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
		       input_tokens, output_tokens, total_tokens, estimated_cost_usd,
		       ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, now()
		from agg
		on conflict (bucket_start, model_name) do update set
			request_count = gateway_request_hourly_stats.request_count + excluded.request_count,
			success_count = gateway_request_hourly_stats.success_count + excluded.success_count,
			failure_count = gateway_request_hourly_stats.failure_count + excluded.failure_count,
			streaming_count = gateway_request_hourly_stats.streaming_count + excluded.streaming_count,
			input_tokens = gateway_request_hourly_stats.input_tokens + excluded.input_tokens,
			output_tokens = gateway_request_hourly_stats.output_tokens + excluded.output_tokens,
			total_tokens = gateway_request_hourly_stats.total_tokens + excluded.total_tokens,
			estimated_cost_usd = gateway_request_hourly_stats.estimated_cost_usd + excluded.estimated_cost_usd,
			ttft_ms_sum = gateway_request_hourly_stats.ttft_ms_sum + excluded.ttft_ms_sum,
			ttft_count = gateway_request_hourly_stats.ttft_count + excluded.ttft_count,
			duration_ms_sum = gateway_request_hourly_stats.duration_ms_sum + excluded.duration_ms_sum,
			duration_count = gateway_request_hourly_stats.duration_count + excluded.duration_count,
			updated_at = now()
	`)
	if err != nil {
		return 0, err
	}
	updated, err := tx.Exec(ctx, `
		update gateway_request_logs
		set analytics_recorded_at = now()
		where id in (
			select id from gateway_request_logs
			where analytics_recorded_at is null
			  and finished_at is not null
			order by id
			limit 5000
		)
	`)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 0 {
		return updated.RowsAffected(), nil
	}
	return updated.RowsAffected(), nil
}

func (s *Store) DeleteOldRequestLogs(ctx context.Context, retentionDays int) (int64, error) {
	if retentionDays <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		delete from gateway_request_logs
		where id in (
			select id from gateway_request_logs
			where started_at < now() - make_interval(days => $1)
			order by id
			limit 5000
		)
	`, retentionDays)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ListRequestLogs(ctx context.Context, limit int) ([]RequestLog, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		select request_id, endpoint, model, model_name, is_stream, status_code, success,
		       attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
		       first_token_at, ttft_ms, duration_ms, input_tokens, cached_input_tokens, output_tokens,
		       total_tokens, estimated_cost_usd, request_payload_hash, upstream_payload_hash,
		       prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
		       prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
		       session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
		       cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message
		from gateway_request_logs
		order by started_at desc
		limit $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []RequestLog
	for rows.Next() {
		var item RequestLog
		var promptTrace []byte
		if err := rows.Scan(
			&item.RequestID, &item.Endpoint, &item.Model, &item.ModelName, &item.IsStream, &item.StatusCode,
			&item.Success, &item.AttemptCount, &item.TokenID, &item.AccountID, &item.ClientIP, &item.UserAgent,
			&item.StartedAt, &item.FinishedAt, &item.FirstTokenAt, &item.TTFTMs, &item.DurationMs,
			&item.InputTokens, &item.CachedInputTokens, &item.OutputTokens, &item.TotalTokens, &item.EstimatedCostUSD,
			&item.RequestPayloadHash, &item.UpstreamPayloadHash, &item.PromptTemplateHash, &item.PromptDynamicHash,
			&item.PromptCacheSource, &item.PromptCacheKeyHash, &item.PromptCacheRetentionRequested,
			&item.PromptCacheRetentionSent, &item.SessionIDHash, &item.SessionIDSource, &item.PreviousResponseIDHash,
			&item.UpstreamResponseID, &item.CacheHitRatio, &item.CacheAffinityResult, &item.CacheAffinityLaneIndex,
			&promptTrace, &item.ErrorMessage,
		); err != nil {
			return nil, err
		}
		if len(promptTrace) > 0 {
			_ = json.Unmarshal(promptTrace, &item.PromptCacheTrace)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) RequestAnalytics(ctx context.Context, hours int) (RequestAnalytics, error) {
	if hours <= 0 {
		hours = 24
	}
	summary, err := s.RequestLogSummary(ctx, hours)
	if err != nil {
		return RequestAnalytics{}, err
	}
	rows, err := s.pool.Query(ctx, `
		select model_name,
		       coalesce(sum(request_count), 0)::int,
		       coalesce(sum(success_count), 0)::int,
		       coalesce(sum(failure_count), 0)::int
		from gateway_request_hourly_stats
		where bucket_start >= date_trunc('hour', now() - make_interval(hours => $1))
		group by model_name
		order by coalesce(sum(request_count), 0) desc
		limit 20
	`, hours)
	if err != nil {
		return RequestAnalytics{}, err
	}
	defer rows.Close()
	items := make([]ModelStat, 0)
	for rows.Next() {
		var item ModelStat
		if err := rows.Scan(&item.ModelName, &item.Requests, &item.Success, &item.Failure); err != nil {
			return RequestAnalytics{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return RequestAnalytics{}, err
	}
	return RequestAnalytics{Hours: hours, Summary: summary, ByModel: items}, nil
}
