create table if not exists gateway_current_token_costs (
    token_id integer primary key,
    estimated_cost_usd numeric not null,
    initialized_at timestamptz not null default now()
) with (fillfactor=80);

-- Private initialization state: never used as a user-visible cost. Capturing
-- deltas here lets the historical read run without holding the writer lock.
create table if not exists gateway_token_cost_seed_deltas (
    token_id integer primary key references codex_tokens(id) on delete cascade,
    delta_usd numeric not null default 0
) with (fillfactor=80);

-- The log itself is the request-ID ledger. OLD/NEW deltas cover retries,
-- reassignment and repricing without creating a second copy of all logs.
-- Retention deletes do not subtract lifetime costs.
create or replace function oaix_current_token_cost_changed() returns trigger language plpgsql as $$
declare
    previous_token integer;
    previous_cost numeric := 0;
    next_token integer;
    next_cost numeric := 0;
    item record;
begin
    if tg_op='UPDATE' then
        previous_token := old.token_id;
        previous_cost := coalesce(old.estimated_cost_usd::numeric,0);
    end if;
    next_token := new.token_id;
    next_cost := coalesce(new.estimated_cost_usd::numeric,0);
    for item in
        select token_id, sum(cost) as delta from (
            values (previous_token,-previous_cost),(next_token,next_cost)
        ) d(token_id,cost)
        where token_id is not null
        group by token_id having sum(cost)<>0 order by token_id
    loop
        -- Keep the existing lock for compatibility with older initializers.
        -- New initializers only take it to prepare and publish, never to scan.
        perform pg_advisory_xact_lock(17984321,item.token_id);
        update gateway_current_token_costs
        set estimated_cost_usd=estimated_cost_usd+item.delta
        where token_id=item.token_id;
        if not found then
            update gateway_token_cost_seed_deltas
            set delta_usd=delta_usd+item.delta where token_id=item.token_id;
        end if;
    end loop;
    return new;
end $$;

create or replace trigger oaix_current_token_cost_insert after insert on gateway_request_logs
for each row when (new.token_id is not null and new.estimated_cost_usd is not null)
execute function oaix_current_token_cost_changed();
create or replace trigger oaix_current_token_cost_update after update on gateway_request_logs
for each row when (old.token_id is distinct from new.token_id or old.estimated_cost_usd is distinct from new.estimated_cost_usd)
execute function oaix_current_token_cost_changed();
