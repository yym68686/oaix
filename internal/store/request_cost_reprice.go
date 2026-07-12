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
)

type GPT56CostRepriceResult struct {
	UpdatedLogs        int64
	RebuiltTokenCosts  int64
	RebuiltHourlyCosts int64
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
	firstRun := false
	err = tx.QueryRow(ctx, `
		select
			coalesce((value->>'completed')::boolean, false),
			coalesce((value->>'first_seen_at')::timestamptz, now())
		from gateway_settings
		where key = $1
		for update
	`, gpt56CostRepriceSettingKey).Scan(&completed, &firstSeenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		firstRun = true
		if err := tx.QueryRow(ctx, `select now()`).Scan(&firstSeenAt); err != nil {
			return result, err
		}
	} else if err != nil {
		return result, err
	}
	if completed {
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
	result.Completed = !databaseNow.Before(firstSeenAt.Add(gpt56CostRepriceFinalizationDelay))
	if !firstRun && !result.Completed {
		if err := tx.Commit(ctx); err != nil {
			return result, err
		}
		return result, nil
	}

	var repairSince *time.Time
	if !firstRun {
		value := firstSeenAt.Add(-time.Hour)
		repairSince = &value
	}
	tag, err := tx.Exec(ctx, repriceGPT56RequestLogsSQL, repairSince)
	if err != nil {
		return result, err
	}
	result.UpdatedLogs = tag.RowsAffected()

	if firstRun || result.UpdatedLogs > 0 {
		tag, err = tx.Exec(ctx, rebuildGPT56TokenCostsSQL)
		if err != nil {
			return result, err
		}
		result.RebuiltTokenCosts = tag.RowsAffected()

		tag, err = tx.Exec(ctx, rebuildGPT56HourlyCostsSQL)
		if err != nil {
			return result, err
		}
		result.RebuiltHourlyCosts = tag.RowsAffected()
	}

	if firstRun || result.UpdatedLogs > 0 || result.Completed {
		if _, err := tx.Exec(ctx, `
			insert into gateway_settings(key, value, updated_at)
			values (
				$1,
				jsonb_build_object(
					'completed', $2::boolean,
					'first_seen_at', $3::timestamptz,
					'last_repaired_at', now(),
					'updated_log_count', $4::bigint,
					'rebuilt_token_count', $5::bigint,
					'rebuilt_hourly_count', $6::bigint
				),
				now()
			)
			on conflict (key) do update set
				value = excluded.value,
				updated_at = excluded.updated_at
		`, gpt56CostRepriceSettingKey, result.Completed, firstSeenAt, result.UpdatedLogs, result.RebuiltTokenCosts, result.RebuiltHourlyCosts); err != nil {
			return result, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

const repriceGPT56RequestLogsSQL = `
	with candidates as (
		select
			id,
			lower(btrim(coalesce(model_name, model, ''))) as normalized_model,
			coalesce(input_tokens, 0)::numeric as input_tokens,
			coalesce(cache_write_input_tokens, 0)::numeric as cache_write_input_tokens,
			coalesce(cached_input_tokens, 0)::numeric as cached_input_tokens,
			coalesce(output_tokens, 0)::numeric as output_tokens
		from gateway_request_logs
		where finished_at is not null
		  and estimated_cost_usd is not null
		  and ($1::timestamptz is null or started_at >= $1::timestamptz)
		  and (
			lower(btrim(coalesce(model_name, model, ''))) in ('gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna')
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-sol-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-terra-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-luna-%'
		  )
	),
	priced as (
		select
			id,
			input_tokens,
			cache_write_input_tokens,
			cached_input_tokens,
			output_tokens,
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
			round((
				greatest(input_tokens - cache_write_input_tokens - cached_input_tokens, 0) * input_rate
				+ cache_write_input_tokens * cache_write_rate
				+ cached_input_tokens * cached_rate
				+ output_tokens * output_rate
			) / 1000000.0, 8) as estimated_cost_usd
		from priced
		where pricing_model is not null
	)
	update gateway_request_logs logs
	set estimated_cost_usd = expected.estimated_cost_usd::float8
	from expected
	where logs.id = expected.id
	  and abs(logs.estimated_cost_usd::numeric - expected.estimated_cost_usd) > 0.000000005
`

const rebuildGPT56TokenCostsSQL = `
	with affected_tokens as (
		select distinct token_id
		from gateway_request_logs
		where token_id is not null
		  and (
			lower(btrim(coalesce(model_name, model, ''))) in ('gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna')
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-sol-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-terra-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-luna-%'
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
		where analytics_recorded_at is not null
		  and owner_user_id is not null
		  and estimated_cost_usd is not null
		  and (
			lower(btrim(coalesce(model_name, model, ''))) in ('gpt-5.6-sol', 'gpt-5.6-terra', 'gpt-5.6-luna')
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-sol-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-terra-%'
			or lower(btrim(coalesce(model_name, model, ''))) like 'gpt-5.6-luna-%'
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
