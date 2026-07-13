package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type RequestLog struct {
	RequestID                     string               `json:"request_id"`
	OwnerUserID                   *int64               `json:"owner_user_id,omitempty"`
	APIKeyID                      *int64               `json:"api_key_id,omitempty"`
	TokenOwnerUserID              *int64               `json:"token_owner_user_id,omitempty"`
	SelectionMode                 *string              `json:"selection_mode,omitempty"`
	CallerOwnerUserID             *int64               `json:"caller_owner_user_id,omitempty"`
	Endpoint                      string               `json:"endpoint"`
	Model                         *string              `json:"model,omitempty"`
	ModelName                     *string              `json:"model_name,omitempty"`
	IsStream                      bool                 `json:"is_stream"`
	StatusCode                    *int                 `json:"status_code,omitempty"`
	Success                       *bool                `json:"success,omitempty"`
	AttemptCount                  int                  `json:"attempt_count"`
	TokenID                       *int64               `json:"token_id,omitempty"`
	AccountID                     *string              `json:"account_id,omitempty"`
	ClientIP                      *string              `json:"client_ip,omitempty"`
	UserAgent                     *string              `json:"user_agent,omitempty"`
	StartedAt                     time.Time            `json:"started_at"`
	FinishedAt                    *time.Time           `json:"finished_at,omitempty"`
	FirstTokenAt                  *time.Time           `json:"first_token_at,omitempty"`
	TTFTMs                        *int                 `json:"ttft_ms,omitempty"`
	DurationMs                    *int                 `json:"duration_ms,omitempty"`
	TimingSpans                   map[string]any       `json:"timing_spans,omitempty"`
	StreamDeliveryState           *string              `json:"stream_delivery_state,omitempty"`
	DownstreamConnectionID        *string              `json:"downstream_connection_id,omitempty"`
	StreamDeliveryTrace           *StreamDeliveryTrace `json:"stream_delivery_trace,omitempty"`
	InputTokens                   *int                 `json:"input_tokens,omitempty"`
	CacheWriteInputTokens         *int                 `json:"cache_write_input_tokens,omitempty"`
	CacheWriteTokensSource        *string              `json:"cache_write_tokens_source,omitempty"`
	CachedInputTokens             *int                 `json:"cached_input_tokens,omitempty"`
	OutputTokens                  *int                 `json:"output_tokens,omitempty"`
	TotalTokens                   *int                 `json:"total_tokens,omitempty"`
	EstimatedCostUSD              *float64             `json:"estimated_cost_usd,omitempty"`
	RequestPayloadHash            *string              `json:"request_payload_hash,omitempty"`
	UpstreamPayloadHash           *string              `json:"upstream_payload_hash,omitempty"`
	PromptTemplateHash            *string              `json:"prompt_template_hash,omitempty"`
	PromptDynamicHash             *string              `json:"prompt_dynamic_hash,omitempty"`
	PromptCacheSource             *string              `json:"prompt_cache_source,omitempty"`
	PromptCacheKeyHash            *string              `json:"prompt_cache_key_hash,omitempty"`
	PromptCacheRetentionRequested *string              `json:"prompt_cache_retention_requested,omitempty"`
	PromptCacheRetentionSent      *string              `json:"prompt_cache_retention_sent,omitempty"`
	SessionIDHash                 *string              `json:"session_id_hash,omitempty"`
	SessionIDSource               *string              `json:"session_id_source,omitempty"`
	PreviousResponseIDHash        *string              `json:"previous_response_id_hash,omitempty"`
	UpstreamResponseID            *string              `json:"upstream_response_id,omitempty"`
	CacheHitRatio                 *float64             `json:"cache_hit_ratio,omitempty"`
	CacheAffinityResult           *string              `json:"cache_affinity_result,omitempty"`
	CacheAffinityLaneIndex        *int                 `json:"cache_affinity_lane_index,omitempty"`
	PromptCacheTrace              map[string]any       `json:"prompt_cache_trace,omitempty"`
	ErrorMessage                  *string              `json:"error_message,omitempty"`
}

type RequestLogSummary struct {
	Total        int64   `json:"total"`
	Success      int64   `json:"success"`
	Failure      int64   `json:"failure"`
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
	Requests  int64  `json:"requests"`
	Success   int64  `json:"success"`
	Failure   int64  `json:"failure"`
}

const requestTokenCostReconcileSettingKey = "request_token_costs_go_recorded_reconciled_at"

const aggregateRequestTokenCostsSQL = `
	with pending as (
		select
			token_id,
			coalesce(token_owner_user_id, owner_user_id) as owner_user_id,
			coalesce(estimated_cost_usd, 0) as estimated_cost_usd
		from gateway_request_logs
		where analytics_recorded_at is null
		  and finished_at is not null
		  and token_id is not null
		  and estimated_cost_usd is not null
		order by id
		limit 5000
		for update skip locked
	),
	agg as (
		select
			token_id,
			max(owner_user_id) as owner_user_id,
			count(*)::bigint as request_count,
			sum(estimated_cost_usd)::float8 as estimated_cost_usd
		from pending
		group by token_id
	)
	insert into gateway_request_token_costs(token_id, owner_user_id, request_count, estimated_cost_usd, updated_at)
	select token_id, owner_user_id, request_count, estimated_cost_usd, now()
	from agg
	on conflict (token_id) do update set
		owner_user_id = coalesce(excluded.owner_user_id, gateway_request_token_costs.owner_user_id),
		request_count = gateway_request_token_costs.request_count + excluded.request_count,
		estimated_cost_usd = gateway_request_token_costs.estimated_cost_usd + excluded.estimated_cost_usd,
		updated_at = now()
`

type observedCostsPartialError struct {
	step string
	err  error
}

func (e *observedCostsPartialError) Error() string {
	return fmt.Sprintf("partial observed costs load failed at %s: %v", e.step, e.err)
}

func (e *observedCostsPartialError) Unwrap() error {
	return e.err
}

func IsObservedCostsPartialError(err error) bool {
	var partial *observedCostsPartialError
	return errors.As(err, &partial)
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
		streamDeliveryTrace := streamDeliveryTraceBytes(item.StreamDeliveryTrace)
		batch.Queue(`
			insert into gateway_request_logs (
				request_id, owner_user_id, api_key_id, token_owner_user_id, endpoint, model, model_name, is_stream, status_code, success,
				attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
				first_token_at, ttft_ms, duration_ms, timing_spans, input_tokens, cache_write_input_tokens,
				cache_write_tokens_source, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
				request_payload_hash, upstream_payload_hash,
				prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
				prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
				session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
				cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message,
				selection_mode, caller_owner_user_id, stream_delivery_state, downstream_connection_id,
				stream_delivery_trace
			)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50)
			on conflict (request_id) do update set
				owner_user_id = coalesce(excluded.owner_user_id, gateway_request_logs.owner_user_id),
				api_key_id = coalesce(excluded.api_key_id, gateway_request_logs.api_key_id),
				token_owner_user_id = coalesce(excluded.token_owner_user_id, gateway_request_logs.token_owner_user_id),
				selection_mode = coalesce(excluded.selection_mode, gateway_request_logs.selection_mode),
				caller_owner_user_id = coalesce(excluded.caller_owner_user_id, gateway_request_logs.caller_owner_user_id),
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
				cache_write_input_tokens = coalesce(excluded.cache_write_input_tokens, gateway_request_logs.cache_write_input_tokens),
				cache_write_tokens_source = coalesce(excluded.cache_write_tokens_source, gateway_request_logs.cache_write_tokens_source),
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
				error_message = coalesce(excluded.error_message, gateway_request_logs.error_message),
				stream_delivery_state = coalesce(excluded.stream_delivery_state, gateway_request_logs.stream_delivery_state),
				downstream_connection_id = coalesce(excluded.downstream_connection_id, gateway_request_logs.downstream_connection_id),
				stream_delivery_trace = coalesce(excluded.stream_delivery_trace, gateway_request_logs.stream_delivery_trace)
		`,
			item.RequestID, item.OwnerUserID, item.APIKeyID, item.TokenOwnerUserID, item.Endpoint, item.Model, modelName, item.IsStream, item.StatusCode, item.Success,
			item.AttemptCount, item.TokenID, item.AccountID, item.ClientIP, item.UserAgent, item.StartedAt,
			item.FinishedAt, item.FirstTokenAt, item.TTFTMs, item.DurationMs, timing, item.InputTokens,
			item.CacheWriteInputTokens, item.CacheWriteTokensSource, item.CachedInputTokens, item.OutputTokens,
			item.TotalTokens, item.EstimatedCostUSD, item.RequestPayloadHash, item.UpstreamPayloadHash,
			item.PromptTemplateHash, item.PromptDynamicHash, item.PromptCacheSource,
			item.PromptCacheKeyHash, item.PromptCacheRetentionRequested, item.PromptCacheRetentionSent,
			item.SessionIDHash, item.SessionIDSource, item.PreviousResponseIDHash, item.UpstreamResponseID,
			item.CacheHitRatio, item.CacheAffinityResult, item.CacheAffinityLaneIndex, promptTrace, item.ErrorMessage,
			item.SelectionMode, item.CallerOwnerUserID, item.StreamDeliveryState, item.DownstreamConnectionID,
			streamDeliveryTrace,
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
		streamDeliveryTrace := streamDeliveryTraceBytes(item.StreamDeliveryTrace)
		_, err := tx.Exec(ctx, `
			insert into gateway_request_logs (
				request_id, owner_user_id, api_key_id, token_owner_user_id, endpoint, model, model_name, is_stream, status_code, success,
				attempt_count, token_id, account_id, client_ip, user_agent, started_at, finished_at,
				first_token_at, ttft_ms, duration_ms, timing_spans, input_tokens, cache_write_input_tokens,
				cache_write_tokens_source, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
				request_payload_hash, upstream_payload_hash,
				prompt_template_hash, prompt_dynamic_hash, prompt_cache_source, prompt_cache_key_hash,
				prompt_cache_retention_requested, prompt_cache_retention_sent, session_id_hash,
				session_id_source, previous_response_id_hash, upstream_response_id, cache_hit_ratio,
				cache_affinity_result, cache_affinity_lane_index, prompt_cache_trace, error_message,
				selection_mode, caller_owner_user_id, stream_delivery_state, downstream_connection_id,
				stream_delivery_trace
			)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50)
			on conflict (request_id) do update set
				owner_user_id = coalesce(excluded.owner_user_id, gateway_request_logs.owner_user_id),
				api_key_id = coalesce(excluded.api_key_id, gateway_request_logs.api_key_id),
				token_owner_user_id = coalesce(excluded.token_owner_user_id, gateway_request_logs.token_owner_user_id),
				selection_mode = coalesce(excluded.selection_mode, gateway_request_logs.selection_mode),
				caller_owner_user_id = coalesce(excluded.caller_owner_user_id, gateway_request_logs.caller_owner_user_id),
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
				cache_write_input_tokens = coalesce(excluded.cache_write_input_tokens, gateway_request_logs.cache_write_input_tokens),
				cache_write_tokens_source = coalesce(excluded.cache_write_tokens_source, gateway_request_logs.cache_write_tokens_source),
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
				error_message = coalesce(excluded.error_message, gateway_request_logs.error_message),
				stream_delivery_state = coalesce(excluded.stream_delivery_state, gateway_request_logs.stream_delivery_state),
				downstream_connection_id = coalesce(excluded.downstream_connection_id, gateway_request_logs.downstream_connection_id),
				stream_delivery_trace = coalesce(excluded.stream_delivery_trace, gateway_request_logs.stream_delivery_trace)
		`,
			item.RequestID, item.OwnerUserID, item.APIKeyID, item.TokenOwnerUserID, item.Endpoint, item.Model, modelName, item.IsStream, item.StatusCode, item.Success,
			item.AttemptCount, item.TokenID, item.AccountID, item.ClientIP, item.UserAgent, item.StartedAt,
			item.FinishedAt, item.FirstTokenAt, item.TTFTMs, item.DurationMs, jsonBytes(item.TimingSpans),
			item.InputTokens, item.CacheWriteInputTokens, item.CacheWriteTokensSource, item.CachedInputTokens,
			item.OutputTokens, item.TotalTokens, item.EstimatedCostUSD,
			item.RequestPayloadHash, item.UpstreamPayloadHash, item.PromptTemplateHash, item.PromptDynamicHash,
			item.PromptCacheSource, item.PromptCacheKeyHash, item.PromptCacheRetentionRequested,
			item.PromptCacheRetentionSent, item.SessionIDHash, item.SessionIDSource, item.PreviousResponseIDHash,
			item.UpstreamResponseID, item.CacheHitRatio, item.CacheAffinityResult, item.CacheAffinityLaneIndex,
			promptTrace, item.ErrorMessage, item.SelectionMode, item.CallerOwnerUserID,
			item.StreamDeliveryState, item.DownstreamConnectionID, streamDeliveryTrace,
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
			coalesce(sum(request_count), 0)::bigint,
			coalesce(sum(success_count), 0)::bigint,
			coalesce(sum(failure_count), 0)::bigint,
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
				owner_user_id,
				date_trunc('hour', started_at) as bucket_start,
				coalesce(model_name, model, '') as model_name,
				success,
				is_stream,
				coalesce(input_tokens, 0) as input_tokens,
				coalesce(cache_write_input_tokens, 0) as cache_write_input_tokens,
				coalesce(cached_input_tokens, 0) as cached_input_tokens,
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
				owner_user_id,
				model_name,
				count(*)::bigint as request_count,
				count(*) filter (where success = true)::bigint as success_count,
				count(*) filter (where success = false)::bigint as failure_count,
				count(*) filter (where is_stream = true)::bigint as streaming_count,
				coalesce(sum(input_tokens), 0)::bigint as input_tokens,
				coalesce(sum(cache_write_input_tokens), 0)::bigint as cache_write_input_tokens,
				coalesce(sum(cached_input_tokens), 0)::bigint as cached_input_tokens,
				coalesce(sum(output_tokens), 0)::bigint as output_tokens,
				coalesce(sum(total_tokens), 0)::bigint as total_tokens,
				sum(estimated_cost_usd)::float8 as estimated_cost_usd,
				coalesce(sum(ttft_ms) filter (where ttft_ms is not null), 0)::bigint as ttft_ms_sum,
				count(ttft_ms)::bigint as ttft_count,
				coalesce(sum(duration_ms) filter (where duration_ms is not null), 0)::bigint as duration_ms_sum,
				count(duration_ms)::bigint as duration_count
			from pending
			where owner_user_id is not null
			group by owner_user_id, bucket_start, model_name
		)
		insert into gateway_request_hourly_stats (
			owner_user_id, bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
			input_tokens, cache_write_input_tokens, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, updated_at
		)
		select owner_user_id, bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
		       input_tokens, cache_write_input_tokens, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
		       ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, now()
		from agg
		on conflict (owner_user_id, bucket_start, model_name) do update set
			request_count = gateway_request_hourly_stats.request_count + excluded.request_count,
			success_count = gateway_request_hourly_stats.success_count + excluded.success_count,
			failure_count = gateway_request_hourly_stats.failure_count + excluded.failure_count,
			streaming_count = gateway_request_hourly_stats.streaming_count + excluded.streaming_count,
			input_tokens = gateway_request_hourly_stats.input_tokens + excluded.input_tokens,
			cache_write_input_tokens = gateway_request_hourly_stats.cache_write_input_tokens + excluded.cache_write_input_tokens,
			cached_input_tokens = gateway_request_hourly_stats.cached_input_tokens + excluded.cached_input_tokens,
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
	_, err = tx.Exec(ctx, aggregateRequestTokenCostsSQL)
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

func (s *Store) TokenObservedCosts(ctx context.Context, tokens []Token) (map[int64]*float64, error) {
	return s.tokenObservedCosts(ctx, tokens, true)
}

func (s *Store) TokenObservedCostsSnapshot(ctx context.Context, tokens []Token) (map[int64]*float64, error) {
	costs, err := s.tokenObservedCosts(ctx, tokens, false)
	if costs != nil && (err == nil || IsObservedCostsPartialError(err)) {
		fillMissingObservedCosts(tokens, costs)
	}
	return costs, err
}

func (s *Store) tokenObservedCosts(ctx context.Context, tokens []Token, includePendingLogs bool) (map[int64]*float64, error) {
	result := make(map[int64]*float64, len(tokens))
	requestedIDs := make([]int64, 0, len(tokens))
	requested := map[int64]struct{}{}
	tokenByID := map[int64]Token{}
	for _, token := range tokens {
		if token.ID <= 0 {
			continue
		}
		if _, ok := requested[token.ID]; ok {
			continue
		}
		requested[token.ID] = struct{}{}
		requestedIDs = append(requestedIDs, token.ID)
		tokenByID[token.ID] = token
	}
	if len(requestedIDs) == 0 {
		return result, nil
	}

	canonicalByLogTokenID, err := s.canonicalTokenMap(ctx, requestedIDs, requested)
	if err != nil {
		return nil, err
	}
	if len(canonicalByLogTokenID) == 0 {
		return result, nil
	}

	reconciled, err := s.RequestTokenCostsReconciled(ctx)
	if err != nil {
		return nil, err
	}
	if reconciled {
		if err := s.addRequestCostsByTokenAggregate(ctx, canonicalByLogTokenID, result); err != nil {
			return nil, err
		}
		if includePendingLogs {
			if err := s.addRequestCostsByTokenLogs(ctx, canonicalByLogTokenID, result, true); err != nil {
				return result, &observedCostsPartialError{step: "pending_token_logs", err: err}
			}
		}
	} else if err := s.addRequestCostsByTokenLogs(ctx, canonicalByLogTokenID, result, false); err != nil {
		return nil, err
	}

	if err := s.addRequestCostsByUniqueAccountFallback(ctx, tokenByID, result); err != nil {
		return result, &observedCostsPartialError{step: "unique_account_fallback", err: err}
	}
	return result, nil
}

func fillMissingObservedCosts(tokens []Token, result map[int64]*float64) {
	seen := map[int64]struct{}{}
	for _, token := range tokens {
		if token.ID <= 0 {
			continue
		}
		if _, ok := seen[token.ID]; ok {
			continue
		}
		seen[token.ID] = struct{}{}
		if result[token.ID] != nil {
			continue
		}
		value := 0.0
		result[token.ID] = &value
	}
}

func (s *Store) addRequestCostsByUniqueAccountFallback(ctx context.Context, tokenByID map[int64]Token, result map[int64]*float64) error {
	accountIDs := make([]string, 0)
	accountSeen := map[string]struct{}{}
	for _, token := range tokenByID {
		if result[token.ID] != nil || token.AccountID == nil {
			continue
		}
		accountID := strings.TrimSpace(*token.AccountID)
		if accountID == "" {
			continue
		}
		if _, ok := accountSeen[accountID]; ok {
			continue
		}
		accountSeen[accountID] = struct{}{}
		accountIDs = append(accountIDs, accountID)
	}
	if len(accountIDs) == 0 {
		return nil
	}

	tokenCountsByAccount, err := s.tokenCountsByAccount(ctx, accountIDs)
	if err != nil {
		return err
	}
	costsByAccount, err := s.requestCostsByAccount(ctx, accountIDs)
	if err != nil {
		return err
	}
	for _, token := range tokenByID {
		if result[token.ID] != nil || token.AccountID == nil {
			continue
		}
		accountID := strings.TrimSpace(*token.AccountID)
		if accountID == "" || tokenCountsByAccount[accountID] != 1 {
			continue
		}
		cost, ok := costsByAccount[accountID]
		if !ok {
			continue
		}
		value := roundCostUSD(cost)
		result[token.ID] = &value
	}
	return nil
}

func (s *Store) addRequestCostsByTokenAggregate(ctx context.Context, canonicalByLogTokenID map[int64]int64, result map[int64]*float64) error {
	logTokenIDs := make([]int64, 0, len(canonicalByLogTokenID))
	for tokenID := range canonicalByLogTokenID {
		logTokenIDs = append(logTokenIDs, tokenID)
	}
	ids := postgresIntIDs(logTokenIDs)
	if len(ids) == 0 {
		return nil
	}
	rows, err := s.pool.Query(ctx, `
		select token_id, estimated_cost_usd
		from gateway_request_token_costs
		where token_id = any($1::integer[])
	`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var logTokenID int64
		var cost float64
		if err := rows.Scan(&logTokenID, &cost); err != nil {
			return err
		}
		addCanonicalCost(canonicalByLogTokenID, result, logTokenID, cost)
	}
	return rows.Err()
}

func (s *Store) addRequestCostsByTokenLogs(ctx context.Context, canonicalByLogTokenID map[int64]int64, result map[int64]*float64, pendingOnly bool) error {
	logTokenIDs := make([]int64, 0, len(canonicalByLogTokenID))
	for tokenID := range canonicalByLogTokenID {
		logTokenIDs = append(logTokenIDs, tokenID)
	}
	ids := postgresIntIDs(logTokenIDs)
	if len(ids) == 0 {
		return nil
	}
	pendingClause := ""
	if pendingOnly {
		pendingClause = "and analytics_recorded_at is null"
	}
	rows, err := s.pool.Query(ctx, `
		select token_id, coalesce(sum(estimated_cost_usd), 0)::float8
		from gateway_request_logs
		where token_id = any($1::integer[])
		  and estimated_cost_usd is not null
		  `+pendingClause+`
		group by token_id
	`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var logTokenID int64
		var cost float64
		if err := rows.Scan(&logTokenID, &cost); err != nil {
			return err
		}
		addCanonicalCost(canonicalByLogTokenID, result, logTokenID, cost)
	}
	return rows.Err()
}

func addCanonicalCost(canonicalByLogTokenID map[int64]int64, result map[int64]*float64, logTokenID int64, cost float64) {
	canonicalID, ok := canonicalByLogTokenID[logTokenID]
	if !ok {
		return
	}
	current := 0.0
	if result[canonicalID] != nil {
		current = *result[canonicalID]
	}
	total := roundCostUSD(current + cost)
	result[canonicalID] = &total
}

func (s *Store) canonicalTokenMap(ctx context.Context, requestedIDs []int64, requested map[int64]struct{}) (map[int64]int64, error) {
	rows, err := s.pool.Query(ctx, `
		select id, merged_into_token_id
		from codex_tokens
		where id = any($1::integer[])
		   or merged_into_token_id = any($1::integer[])
	`, postgresIntIDs(requestedIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var id int64
		var mergedInto *int64
		if err := rows.Scan(&id, &mergedInto); err != nil {
			return nil, err
		}
		canonicalID := id
		if mergedInto != nil {
			if _, ok := requested[*mergedInto]; ok {
				canonicalID = *mergedInto
			}
		}
		if _, ok := requested[canonicalID]; ok {
			out[id] = canonicalID
		}
	}
	return out, rows.Err()
}

func (s *Store) tokenCountsByAccount(ctx context.Context, accountIDs []string) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `
		select account_id, count(*)::int
		from codex_tokens
		where account_id = any($1::text[])
		  and merged_into_token_id is null
		group by account_id
	`, accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var accountID string
		var count int
		if err := rows.Scan(&accountID, &count); err != nil {
			return nil, err
		}
		out[accountID] = count
	}
	return out, rows.Err()
}

func (s *Store) requestCostsByAccount(ctx context.Context, accountIDs []string) (map[string]float64, error) {
	rows, err := s.pool.Query(ctx, `
		select account_id, coalesce(sum(estimated_cost_usd), 0)::float8
		from gateway_request_logs
		where account_id = any($1::text[])
		  and estimated_cost_usd is not null
		group by account_id
	`, accountIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]float64{}
	for rows.Next() {
		var accountID string
		var cost float64
		if err := rows.Scan(&accountID, &cost); err != nil {
			return nil, err
		}
		out[accountID] = roundCostUSD(cost)
	}
	return out, rows.Err()
}

func roundCostUSD(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value < 0 {
		return 0
	}
	return math.Round(value*1_000_000) / 1_000_000
}

func (s *Store) RequestTokenCostsReconciled(ctx context.Context) (bool, error) {
	var completed bool
	err := s.pool.QueryRow(ctx, `
		select coalesce((value->>'completed')::boolean, false)
		from gateway_settings
		where key = $1
	`, requestTokenCostReconcileSettingKey).Scan(&completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return completed, err
}

func (s *Store) ReconcileRecordedTokenCosts(ctx context.Context) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var completed bool
	err = tx.QueryRow(ctx, `
		select coalesce((value->>'completed')::boolean, false)
		from gateway_settings
		where key = $1
		for update
	`, requestTokenCostReconcileSettingKey).Scan(&completed)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if completed {
		return false, tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `
		insert into gateway_request_token_costs(token_id, request_count, estimated_cost_usd, updated_at)
		select
			token_id,
			count(*)::bigint as request_count,
			coalesce(sum(estimated_cost_usd), 0)::float8 as estimated_cost_usd,
			now()
		from gateway_request_logs
		where analytics_recorded_at is not null
		  and token_id is not null
		  and estimated_cost_usd is not null
		group by token_id
		on conflict (token_id) do update set
			request_count = excluded.request_count,
			estimated_cost_usd = excluded.estimated_cost_usd,
			updated_at = excluded.updated_at
	`); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `
		insert into gateway_settings(key, value, updated_at)
		values (
			$1,
			jsonb_build_object('completed', true, 'completed_at', now()),
			now()
		)
		on conflict (key) do update set
			value = excluded.value,
			updated_at = excluded.updated_at
	`, requestTokenCostReconcileSettingKey); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
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
	items, _, err := s.ListRequestLogsFiltered(ctx, RequestLogListOptions{Limit: limit})
	return items, err
}

func (s *Store) RequestAnalytics(ctx context.Context, hours int) (RequestAnalytics, error) {
	return s.RequestAnalyticsScoped(ctx, AllResources(), hours)
}

func (s *Store) RequestAnalyticsScoped(ctx context.Context, scope ResourceScope, hours int) (RequestAnalytics, error) {
	if hours <= 0 {
		hours = 24
	}
	usage, err := s.RequestUsageSummaryScoped(ctx, scope, hours)
	if err != nil {
		return RequestAnalytics{}, err
	}
	summary := RequestLogSummary{
		Total:        usage.Total,
		Success:      usage.Success,
		Failure:      usage.Failure,
		AverageTTFT:  usage.AverageTTFTMS,
		AverageDurMS: usage.AverageDurationMS,
	}
	args := []any{hours}
	ownerWhere := scope.ownerFilter("owner_user_id", &args)
	rows, err := s.pool.Query(ctx, `
		select model_name,
		       coalesce(sum(request_count), 0)::bigint,
		       coalesce(sum(success_count), 0)::bigint,
		       coalesce(sum(failure_count), 0)::bigint
		from gateway_request_hourly_stats
		where bucket_start >= date_trunc('hour', now() - make_interval(hours => $1))
		  and `+ownerWhere+`
		group by model_name
		order by coalesce(sum(request_count), 0) desc
		limit 20
	`, args...)
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
