package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	gpt56CostRepriceSettingKey        = "request_costs_gpt56_official_pricing_v2"
	gpt56CostRepriceFinalizationDelay = 15 * time.Minute
	gpt56CostRepriceBatchSize         = 20_000
)

var gpt56IncorrectPricingStartedAt = time.Date(2026, time.July, 9, 0, 0, 0, 0, time.UTC)

type GPT56CostRepriceResult struct {
	ScannedLogs        int64
	UpdatedLogs        int64
	RebuiltTokenCosts  int64
	RebuiltHourlyCosts int64
	Phase              string
	Completed          bool
}

// RepriceGPT56RequestCosts repairs the duplicated GPT-5.6 price table that was
// introduced with the initial cache-write billing support. It stays pending
// through one rolling-deployment window so requests completed by an old
// process are also corrected before the repair is marked complete.
func (s *Store) RepriceGPT56RequestCosts(ctx context.Context) (GPT56CostRepriceResult, error) {
	result := GPT56CostRepriceResult{}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `select pg_advisory_xact_lock(hashtext($1))`, gpt56CostRepriceSettingKey); err != nil {
		return result, err
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
	`, gpt56CostRepriceSettingKey).Scan(&completed, &firstSeenAt, &phase, &cursor)
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

	saveState := func(completed bool) error {
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
					'rebuilt_token_count', $8::bigint,
					'rebuilt_hourly_count', $9::bigint
				),
				now()
			)
			on conflict (key) do update set
				value = excluded.value,
				updated_at = excluded.updated_at
		`, gpt56CostRepriceSettingKey, completed, firstSeenAt, phase, cursor, result.ScannedLogs, result.UpdatedLogs, result.RebuiltTokenCosts, result.RebuiltHourlyCosts)
		return err
	}

	result.Phase = phase
	switch phase {
	case "initial", "final":
		scanSince := gpt56IncorrectPricingStartedAt
		if phase == "final" {
			scanSince = firstSeenAt.Add(-time.Hour)
		}
		var nextCursor int64
		if err := tx.QueryRow(
			ctx,
			repriceGPT56RequestLogsBatchSQL,
			cursor,
			scanSince,
			gpt56CostRepriceBatchSize,
		).Scan(&nextCursor, &result.ScannedLogs, &result.UpdatedLogs); err != nil {
			return result, err
		}
		if result.ScannedLogs > 0 {
			cursor = nextCursor
			if err := saveState(false); err != nil {
				return result, err
			}
			break
		}
		if phase == "initial" {
			if databaseNow.Before(firstSeenAt.Add(gpt56CostRepriceFinalizationDelay)) {
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
		phase = "token_rebuild"
		cursor = 0
		result.Phase = phase
		if err := saveState(false); err != nil {
			return result, err
		}
	case "token_rebuild":
		tag, err := tx.Exec(ctx, rebuildGPT56TokenCostsSQL, gpt56IncorrectPricingStartedAt)
		if err != nil {
			return result, err
		}
		result.RebuiltTokenCosts = tag.RowsAffected()
		phase = "hourly_rebuild"
		result.Phase = phase
		if err := saveState(false); err != nil {
			return result, err
		}
	case "hourly_rebuild":
		tag, err := tx.Exec(ctx, rebuildGPT56HourlyCostsSQL, gpt56IncorrectPricingStartedAt)
		if err != nil {
			return result, err
		}
		result.RebuiltHourlyCosts = tag.RowsAffected()
		phase = "completed"
		result.Phase = phase
		result.Completed = true
		if err := saveState(true); err != nil {
			return result, err
		}
	default:
		return result, errors.New("unknown GPT-5.6 request cost reprice phase: " + phase)
	}

	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

const repriceGPT56RequestLogsBatchSQL = `
	with candidates as materialized (
		select
			id,
			lower(btrim(coalesce(model_name, model, ''))) as normalized_model,
			coalesce(input_tokens, 0)::numeric as input_tokens,
			coalesce(cache_write_input_tokens, 0)::numeric as cache_write_input_tokens,
			coalesce(cached_input_tokens, 0)::numeric as cached_input_tokens,
			coalesce(output_tokens, 0)::numeric as output_tokens,
			estimated_cost_usd::numeric as previous_cost_usd
		from gateway_request_logs
		where id > $1::bigint
		  and started_at >= $2::timestamptz
		  and finished_at is not null
		  and estimated_cost_usd is not null
		  and (
			(model_name >= 'gpt-5.6-' and model_name < 'gpt-5.7')
			or (model_name is null and model >= 'gpt-5.6-' and model < 'gpt-5.7')
		  )
		order by id
		limit $3::integer
	),
	priced as (
		select
			id,
			input_tokens,
			cache_write_input_tokens,
			cached_input_tokens,
			output_tokens,
			previous_cost_usd,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 'gpt-5.6-sol'
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 'gpt-5.6-terra'
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 'gpt-5.6-luna'
			end as pricing_model,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 5.0::numeric
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 2.5::numeric
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 1.0::numeric
			end as input_rate,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 6.25::numeric
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 3.125::numeric
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 1.25::numeric
			end as cache_write_rate,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 0.5::numeric
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 0.25::numeric
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 0.1::numeric
			end as cached_rate,
			case
				when normalized_model = 'gpt-5.6-sol' or normalized_model like 'gpt-5.6-sol-%' then 30.0::numeric
				when normalized_model = 'gpt-5.6-terra' or normalized_model like 'gpt-5.6-terra-%' then 15.0::numeric
				when normalized_model = 'gpt-5.6-luna' or normalized_model like 'gpt-5.6-luna-%' then 6.0::numeric
			end as output_rate
		from candidates
	),
	expected as (
		select
			id,
			pricing_model,
			input_rate,
			cache_write_rate,
			cached_rate,
			output_rate,
			previous_cost_usd,
			round((
				greatest(input_tokens - cache_write_input_tokens - cached_input_tokens, 0) * input_rate
				+ cache_write_input_tokens * cache_write_rate
				+ cached_input_tokens * cached_rate
				+ output_tokens * output_rate
			) / 1000000.0, 8) as estimated_cost_usd
		from priced
		where pricing_model is not null
	),
	updated as (
		update gateway_request_logs logs
		set estimated_cost_usd = expected.estimated_cost_usd::float8
		from expected
		where logs.id = expected.id
		  and abs(expected.previous_cost_usd - expected.estimated_cost_usd) > 0.000000005
		returning logs.id
	)
	select
		coalesce((select max(id) from candidates), $1::bigint) as next_cursor,
		(select count(*)::bigint from candidates) as scanned_logs,
		(select count(*)::bigint from updated) as updated_logs
`

const rebuildGPT56TokenCostsSQL = `
	with affected_tokens as (
		select distinct token_id
		from gateway_request_logs
		where started_at >= $1::timestamptz
		  and token_id is not null
		  and (
			(model_name >= 'gpt-5.6-' and model_name < 'gpt-5.7')
			or (model_name is null and model >= 'gpt-5.6-' and model < 'gpt-5.7')
		  )
	),
	exact as (
		select
			logs.token_id,
			max(coalesce(logs.token_owner_user_id, logs.owner_user_id)) as owner_user_id,
			count(*)::bigint as request_count,
			coalesce(sum(logs.estimated_cost_usd), 0)::float8 as estimated_cost_usd
		from gateway_request_logs logs
		join affected_tokens on affected_tokens.token_id = logs.token_id
		where logs.analytics_recorded_at is not null
		  and logs.estimated_cost_usd is not null
		group by logs.token_id
	)
	insert into gateway_request_token_costs(token_id, owner_user_id, request_count, estimated_cost_usd, updated_at)
	select token_id, owner_user_id, request_count, estimated_cost_usd, now()
	from exact
	on conflict (token_id) do update set
		owner_user_id = coalesce(excluded.owner_user_id, gateway_request_token_costs.owner_user_id),
		request_count = excluded.request_count,
		estimated_cost_usd = excluded.estimated_cost_usd,
		updated_at = excluded.updated_at
`

const rebuildGPT56HourlyCostsSQL = `
	with exact as (
		select
			owner_user_id,
			date_trunc('hour', started_at) as bucket_start,
			coalesce(model_name, model, '') as model_name,
			coalesce(sum(estimated_cost_usd), 0)::float8 as estimated_cost_usd
		from gateway_request_logs
		where started_at >= $1::timestamptz
		  and analytics_recorded_at is not null
		  and owner_user_id is not null
		  and estimated_cost_usd is not null
		  and (
			(model_name >= 'gpt-5.6-' and model_name < 'gpt-5.7')
			or (model_name is null and model >= 'gpt-5.6-' and model < 'gpt-5.7')
		  )
		group by owner_user_id, date_trunc('hour', started_at), coalesce(model_name, model, '')
	)
	update gateway_request_hourly_stats hourly
	set
		estimated_cost_usd = exact.estimated_cost_usd,
		updated_at = now()
	from exact
	where hourly.owner_user_id = exact.owner_user_id
	  and hourly.bucket_start = exact.bucket_start
	  and hourly.model_name = exact.model_name
	  and hourly.estimated_cost_usd is distinct from exact.estimated_cost_usd
`
