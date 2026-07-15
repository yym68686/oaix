package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	fastCostRepriceSettingKey        = "request_costs_fast_multiplier_v1"
	fastCostRepriceFinalizationDelay = 15 * time.Minute
	fastCostRepriceBatchSize         = 500
)

// fastCostTrackingStartedAt is the commit time of c6fcdd6, the first OAIX
// release that persisted normalized Fast intent in timing_spans. Earlier
// priority requests cannot be distinguished from standard requests with full
// confidence and must never be repriced by inference.
var fastCostTrackingStartedAt = time.Date(2026, time.July, 13, 22, 30, 12, 0, time.UTC)

type FastCostRepriceResult struct {
	ScannedLogs        int64
	UpdatedLogs        int64
	UpdatedTokenCosts  int64
	UpdatedHourlyCosts int64
	Phase              string
	Completed          bool
}

// RepriceFastRequestCosts applies the documented Fast multipliers to request
// logs that carry the normalized, persisted Fast marker. Each call locks and
// repairs at most one small batch, applies aggregate deltas in the same
// transaction, and records a durable cursor. A delayed final pass catches
// writes from old processes during a rolling deployment.
func (s *Store) RepriceFastRequestCosts(ctx context.Context) (FastCostRepriceResult, error) {
	result := FastCostRepriceResult{}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	// Never wait behind business traffic. A later maintenance cycle safely
	// retries the same cursor when either row or aggregate locks are busy.
	if _, err := tx.Exec(ctx, `set local lock_timeout = '2s'`); err != nil {
		return result, err
	}
	if _, err := tx.Exec(ctx, `set local statement_timeout = '20s'`); err != nil {
		return result, err
	}

	// The older GPT-5.6 base-price repair rewrites the same request costs and
	// runs before this repair. Do not create or advance Fast state until that
	// repair is durably complete; otherwise a later GPT-5.6 batch could restore
	// a repaired Fast row to its 1x base cost.
	var gpt56RepriceCompleted bool
	err = tx.QueryRow(ctx, `
		select coalesce((value->>'completed')::boolean, false)
		from gateway_settings
		where key = $1
	`, gpt56CostRepriceSettingKey).Scan(&gpt56RepriceCompleted)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !gpt56RepriceCompleted) {
		result.Phase = "waiting_gpt56"
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		return result, nil
	}
	if err != nil {
		return result, err
	}

	var locked bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(hashtext($1))`, fastCostRepriceSettingKey).Scan(&locked); err != nil {
		return result, err
	}
	if !locked {
		result.Phase = "busy"
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		return result, nil
	}

	var completed bool
	var firstSeenAt time.Time
	var phase string
	var cursor int64
	firstRun := false
	err = tx.QueryRow(ctx, `
		select
			coalesce((value->>'completed')::boolean, false),
			coalesce((value->>'first_seen_at')::timestamptz, now()),
			coalesce(nullif(value->>'phase', ''), 'initial'),
			coalesce((value->>'cursor')::bigint, 0)
		from gateway_settings
		where key = $1
		for update
	`, fastCostRepriceSettingKey).Scan(&completed, &firstSeenAt, &phase, &cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		firstRun = true
		phase = "initial"
		if err := tx.QueryRow(ctx, `select now()`).Scan(&firstSeenAt); err != nil {
			return result, err
		}
	} else if err != nil {
		return result, err
	}
	if completed {
		result.Phase = "completed"
		result.Completed = true
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		return result, nil
	}

	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `select now()`).Scan(&databaseNow); err != nil {
		return result, err
	}

	saveState := func(done bool) error {
		_, err := tx.Exec(ctx, `
			insert into gateway_settings(key, value, updated_at)
			values (
				$1,
				jsonb_build_object(
					'completed', $2::boolean,
					'first_seen_at', $3::timestamptz,
					'phase', $4::text,
					'cursor', $5::bigint,
					'last_step_at', now(),
					'scanned_log_count', $6::bigint,
					'updated_log_count', $7::bigint,
					'updated_token_count', $8::bigint,
					'updated_hourly_count', $9::bigint
				),
				now()
			)
			on conflict (key) do update set
				value = excluded.value,
				updated_at = excluded.updated_at
		`, fastCostRepriceSettingKey, done, firstSeenAt, phase, cursor, result.ScannedLogs, result.UpdatedLogs, result.UpdatedTokenCosts, result.UpdatedHourlyCosts)
		return err
	}

	result.Phase = phase
	switch phase {
	case "initial", "final":
		// Re-scan the complete precisely identifiable window in the final
		// phase. A request can start before the rolling window and only finish
		// after the first pass; limiting by first_seen_at would miss it.
		scanSince := fastCostTrackingStartedAt
		var nextCursor int64
		var expectedTokenCosts, expectedHourlyCosts int64
		if err := tx.QueryRow(
			ctx,
			repriceFastRequestLogsBatchSQL,
			cursor,
			scanSince,
			fastCostRepriceBatchSize,
		).Scan(
			&nextCursor,
			&result.ScannedLogs,
			&result.UpdatedLogs,
			&result.UpdatedTokenCosts,
			&expectedTokenCosts,
			&result.UpdatedHourlyCosts,
			&expectedHourlyCosts,
		); err != nil {
			return result, err
		}
		if result.UpdatedTokenCosts != expectedTokenCosts || result.UpdatedHourlyCosts != expectedHourlyCosts {
			return result, fmt.Errorf(
				"Fast request cost aggregate invariant failed: token %d/%d, hourly %d/%d",
				result.UpdatedTokenCosts,
				expectedTokenCosts,
				result.UpdatedHourlyCosts,
				expectedHourlyCosts,
			)
		}
		if result.ScannedLogs > 0 {
			cursor = nextCursor
			if err := saveState(false); err != nil {
				return result, err
			}
			break
		}
		if phase == "initial" {
			if databaseNow.Before(firstSeenAt.Add(fastCostRepriceFinalizationDelay)) {
				if firstRun {
					if err := saveState(false); err != nil {
						return result, err
					}
				}
				break
			}
			phase = "final"
			cursor = 0
			result.Phase = phase
			if err := saveState(false); err != nil {
				return result, err
			}
			break
		}
		phase = "completed"
		cursor = 0
		result.Phase = phase
		result.Completed = true
		if err := saveState(true); err != nil {
			return result, err
		}
	default:
		return result, errors.New("unknown Fast request cost reprice phase: " + phase)
	}

	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

const repriceFastRequestLogsBatchSQL = `
	with candidates as materialized (
		select
			id,
			lower(btrim(coalesce(model_name, model, ''))) as normalized_model,
			coalesce(model_name, model, '') as aggregate_model,
			coalesce(input_tokens, 0)::numeric as input_tokens,
			coalesce(cache_write_input_tokens, 0)::numeric as cache_write_input_tokens,
			coalesce(cached_input_tokens, 0)::numeric as cached_input_tokens,
			coalesce(output_tokens, 0)::numeric as output_tokens,
			estimated_cost_usd::numeric as previous_cost_usd,
			case when jsonb_typeof(prompt_cache_trace::jsonb) = 'object' then prompt_cache_trace::jsonb else '{}'::jsonb end as request_trace,
			analytics_recorded_at,
			token_id,
			owner_user_id,
			date_trunc('hour', started_at) as bucket_start
		from gateway_request_logs
		where id > $1::bigint
		  and started_at >= $2::timestamptz
		  and finished_at is not null
		  and estimated_cost_usd is not null
		  and timing_spans->>'fast_mode_requested' = 'true'
		  and timing_spans->>'service_tier' = 'priority'
		  and lower(btrim(coalesce(model_name, model, ''))) >= 'gpt-5.4'
		  and lower(btrim(coalesce(model_name, model, ''))) < 'gpt-5.7'
		order by id
		limit $3::integer
		for update
	),
	classified as (
		select
			*,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 'gpt-5.6-sol'
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 'gpt-5.6-terra'
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 'gpt-5.6-luna'
				when normalized_model = 'gpt-5.5' or normalized_model like 'gpt-5.5-%' then 'gpt-5.5'
				when normalized_model = 'gpt-5.4-mini' or normalized_model like 'gpt-5.4-mini-%' or normalized_model = 'gpt-5.4-compact' then 'gpt-5.4-mini'
				when normalized_model = 'gpt-5.4-nano' or normalized_model like 'gpt-5.4-nano-%' then 'gpt-5.4-nano'
				when normalized_model = 'gpt-5.4' or normalized_model like 'gpt-5.4-%' then 'gpt-5.4'
			end as pricing_model
		from candidates
	),
	priced as (
		select
			*,
			case pricing_model
				when 'gpt-5.6-sol' then 5.0::numeric
				when 'gpt-5.6-terra' then 2.5::numeric
				when 'gpt-5.6-luna' then 1.0::numeric
				when 'gpt-5.5' then 5.0::numeric
				when 'gpt-5.4' then 2.5::numeric
				when 'gpt-5.4-mini' then 0.75::numeric
				when 'gpt-5.4-nano' then 0.2::numeric
			end as input_rate,
			case pricing_model
				when 'gpt-5.6-sol' then 6.25::numeric
				when 'gpt-5.6-terra' then 3.125::numeric
				when 'gpt-5.6-luna' then 1.25::numeric
			end as cache_write_rate,
			case pricing_model
				when 'gpt-5.6-sol' then 0.5::numeric
				when 'gpt-5.6-terra' then 0.25::numeric
				when 'gpt-5.6-luna' then 0.1::numeric
				when 'gpt-5.5' then 0.5::numeric
				when 'gpt-5.4' then 0.25::numeric
				when 'gpt-5.4-mini' then 0.075::numeric
				when 'gpt-5.4-nano' then 0.02::numeric
			end as cached_rate,
			case pricing_model
				when 'gpt-5.6-sol' then 30.0::numeric
				when 'gpt-5.6-terra' then 15.0::numeric
				when 'gpt-5.6-luna' then 6.0::numeric
				when 'gpt-5.5' then 30.0::numeric
				when 'gpt-5.4' then 15.0::numeric
				when 'gpt-5.4-mini' then 4.5::numeric
				when 'gpt-5.4-nano' then 1.25::numeric
			end as output_rate,
			case when pricing_model like 'gpt-5.6-%' then 2.5::numeric when pricing_model = 'gpt-5.5' then 2.5::numeric else 2.0::numeric end as multiplier,
			case when pricing_model like 'gpt-5.6-%' then 'openai_prompt_cache' else 'openai_cached_input' end as billing_mode
		from classified
		where pricing_model is not null
	),
	base_values as (
		select
			*,
			greatest(input_tokens - cached_input_tokens - case when cache_write_rate is null then 0 else cache_write_input_tokens end, 0) as non_cached_input_tokens,
			(
				greatest(input_tokens - cached_input_tokens - case when cache_write_rate is null then 0 else cache_write_input_tokens end, 0) * input_rate
				+ cache_write_input_tokens * coalesce(cache_write_rate, 0)
				+ cached_input_tokens * cached_rate
				+ output_tokens * output_rate
			) / 1000000.0 as base_cost_unrounded
		from priced
	),
	expected as (
		select
			*,
			round(base_cost_unrounded, 8) as base_cost_usd,
			round(base_cost_unrounded * multiplier, 8) as final_cost_usd
		from base_values
	),
	trace_parts as (
		select
			*,
			case when jsonb_typeof(request_trace->'usage') = 'object' then request_trace->'usage' else '{}'::jsonb end as usage_trace,
			case when jsonb_typeof(request_trace#>'{usage,billing}') = 'object' then request_trace#>'{usage,billing}' else '{}'::jsonb end as billing_trace,
			case when jsonb_typeof(request_trace->'billing') = 'object' then request_trace->'billing' else null end as legacy_billing_trace,
			jsonb_build_object(
				'pricing_model', pricing_model,
				'mode', billing_mode,
				'non_cached_input_tokens', non_cached_input_tokens,
				'cache_write_input_tokens', cache_write_input_tokens,
				'cached_input_tokens', cached_input_tokens,
				'input_per_million_usd', input_rate,
				'cached_input_per_million_usd', cached_rate,
				'output_per_million_usd', output_rate,
				'base_cost_usd', base_cost_usd,
				'multiplier', multiplier,
				'final_cost_usd', final_cost_usd,
				'estimated_cost_usd', final_cost_usd,
				'fast_mode', true,
				'service_tier', 'priority'
			) || case when cache_write_rate is null then '{}'::jsonb else jsonb_build_object('cache_write_per_million_usd', cache_write_rate) end as billing_patch
		from expected
	),
	patched as (
		select
			*,
			case when legacy_billing_trace is null then
				jsonb_set(request_trace, '{usage}', usage_trace || jsonb_build_object('billing', billing_trace || billing_patch), true)
			else
				jsonb_set(
					jsonb_set(request_trace, '{usage}', usage_trace || jsonb_build_object('billing', billing_trace || billing_patch), true),
					'{billing}', legacy_billing_trace || billing_patch, true
				)
			end as patched_trace
		from trace_parts
	),
	updated as (
		update gateway_request_logs logs
		set
			estimated_cost_usd = patched.final_cost_usd::float8,
			prompt_cache_trace = patched.patched_trace
		from patched
		where logs.id = patched.id
		  and (
			abs(patched.previous_cost_usd - patched.final_cost_usd) > 0.000000005
			or not (patched.billing_trace @> patched.billing_patch)
			or (patched.legacy_billing_trace is not null and not (patched.legacy_billing_trace @> patched.billing_patch))
		  )
		returning
			logs.id,
			(patched.final_cost_usd - patched.previous_cost_usd)::float8 as cost_delta,
			patched.analytics_recorded_at,
			patched.token_id,
			patched.owner_user_id,
			patched.bucket_start,
			patched.aggregate_model
	),
	token_deltas as materialized (
		select token_id, sum(cost_delta)::float8 as cost_delta
		from updated
		where analytics_recorded_at is not null
		  and token_id is not null
		  and cost_delta <> 0
		group by token_id
	),
	token_updates as (
		update gateway_request_token_costs costs
		set
			estimated_cost_usd = costs.estimated_cost_usd + deltas.cost_delta,
			updated_at = now()
		from token_deltas deltas
		where costs.token_id = deltas.token_id
		returning costs.token_id
	),
	missing_token_deltas as materialized (
		select deltas.token_id
		from token_deltas deltas
		left join token_updates applied on applied.token_id = deltas.token_id
		where applied.token_id is null
	),
	missing_token_exact as (
		select
			logs.token_id,
			max(coalesce(logs.token_owner_user_id, logs.owner_user_id)) as owner_user_id,
			count(*)::bigint as request_count,
			coalesce(sum(coalesce(logs.estimated_cost_usd, 0) + coalesce(revised.cost_delta, 0)), 0)::float8 as estimated_cost_usd
		from gateway_request_logs logs
		join missing_token_deltas missing on missing.token_id = logs.token_id
		left join updated revised on revised.id = logs.id
		where logs.analytics_recorded_at is not null
		  and logs.estimated_cost_usd is not null
		group by logs.token_id
	),
	token_inserts as (
		insert into gateway_request_token_costs(token_id, owner_user_id, request_count, estimated_cost_usd, updated_at)
		select token_id, owner_user_id, request_count, estimated_cost_usd, now()
		from missing_token_exact
		on conflict (token_id) do nothing
		returning token_id
	),
	hourly_deltas as materialized (
		select owner_user_id, bucket_start, aggregate_model, sum(cost_delta)::float8 as cost_delta
		from updated
		where analytics_recorded_at is not null
		  and owner_user_id is not null
		  and cost_delta <> 0
		group by owner_user_id, bucket_start, aggregate_model
	),
	hourly_updates as (
		update gateway_request_hourly_stats hourly
		set
			estimated_cost_usd = hourly.estimated_cost_usd + deltas.cost_delta,
			updated_at = now()
		from hourly_deltas deltas
		where hourly.owner_user_id = deltas.owner_user_id
		  and hourly.bucket_start = deltas.bucket_start
		  and hourly.model_name = deltas.aggregate_model
		returning hourly.id, hourly.owner_user_id, hourly.bucket_start, hourly.model_name
	),
	missing_hourly_deltas as materialized (
		select deltas.owner_user_id, deltas.bucket_start, deltas.aggregate_model
		from hourly_deltas deltas
		left join hourly_updates applied
		  on applied.owner_user_id = deltas.owner_user_id
		 and applied.bucket_start = deltas.bucket_start
		 and applied.model_name = deltas.aggregate_model
		where applied.id is null
	),
	missing_hourly_exact as (
		select
			missing.owner_user_id,
			missing.bucket_start,
			missing.aggregate_model as model_name,
			count(*)::bigint as request_count,
			count(*) filter (where logs.success = true)::bigint as success_count,
			count(*) filter (where logs.success = false)::bigint as failure_count,
			count(*) filter (where logs.is_stream = true)::bigint as streaming_count,
			coalesce(sum(coalesce(logs.input_tokens, 0)), 0)::bigint as input_tokens,
			coalesce(sum(coalesce(logs.cache_write_input_tokens, 0)), 0)::bigint as cache_write_input_tokens,
			coalesce(sum(coalesce(logs.cached_input_tokens, 0)), 0)::bigint as cached_input_tokens,
			coalesce(sum(coalesce(logs.output_tokens, 0)), 0)::bigint as output_tokens,
			coalesce(sum(coalesce(logs.total_tokens, 0)), 0)::bigint as total_tokens,
			coalesce(sum(coalesce(logs.estimated_cost_usd, 0) + coalesce(revised.cost_delta, 0)), 0)::float8 as estimated_cost_usd,
			coalesce(sum(logs.ttft_ms) filter (where logs.ttft_ms is not null), 0)::bigint as ttft_ms_sum,
			count(logs.ttft_ms)::bigint as ttft_count,
			coalesce(sum(logs.duration_ms) filter (where logs.duration_ms is not null), 0)::bigint as duration_ms_sum,
			count(logs.duration_ms)::bigint as duration_count
		from missing_hourly_deltas missing
		join gateway_request_logs logs
		  on logs.owner_user_id = missing.owner_user_id
		 and logs.started_at >= missing.bucket_start
		 and logs.started_at < missing.bucket_start + interval '1 hour'
		 and coalesce(logs.model_name, logs.model, '') = missing.aggregate_model
		left join updated revised on revised.id = logs.id
		where logs.analytics_recorded_at is not null
		group by missing.owner_user_id, missing.bucket_start, missing.aggregate_model
	),
	hourly_inserts as (
		insert into gateway_request_hourly_stats (
			owner_user_id, bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
			input_tokens, cache_write_input_tokens, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, updated_at
		)
		select
			owner_user_id, bucket_start, model_name, request_count, success_count, failure_count, streaming_count,
			input_tokens, cache_write_input_tokens, cached_input_tokens, output_tokens, total_tokens, estimated_cost_usd,
			ttft_ms_sum, ttft_count, duration_ms_sum, duration_count, now()
		from missing_hourly_exact
		on conflict (owner_user_id, bucket_start, model_name) do nothing
		returning id
	)
	select
		coalesce((select max(id) from candidates), $1::bigint) as next_cursor,
		(select count(*)::bigint from candidates) as scanned_logs,
		(select count(*)::bigint from updated) as updated_logs,
		((select count(*)::bigint from token_updates) + (select count(*)::bigint from token_inserts)) as updated_token_costs,
		(select count(*)::bigint from token_deltas) as expected_token_costs,
		((select count(*)::bigint from hourly_updates) + (select count(*)::bigint from hourly_inserts)) as updated_hourly_costs,
		(select count(*)::bigint from hourly_deltas) as expected_hourly_costs
`
