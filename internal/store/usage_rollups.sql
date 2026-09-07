create table if not exists sub2api_usage_rollups (
    target_id bigint not null,
    remote_account_id bigint not null,
    ready boolean not null default false,
    account_cost_usd numeric not null default 0,
    standard_cost_usd numeric not null default 0,
    user_cost_usd numeric not null default 0,
    total_requests bigint not null default 0,
    total_tokens bigint not null default 0,
    synced_at timestamptz,
    failed boolean not null default false,
    missing_days boolean not null default true,
    finalized_dates datemultirange not null default '{}',
    primary key(target_id,remote_account_id)
) with (fillfactor=80);

create or replace view sub2api_usage_daily_current as
select d.target_id,d.remote_account_id,d.token_id,d.usage_date,
       d.account_cost_usd,d.standard_cost_usd,d.user_cost_usd,d.total_requests,d.total_tokens,
       case when s.synced_at >= d.synced_at or d.synced_at is null then s.source_computed_at else d.source_computed_at end as source_computed_at,
       greatest(s.synced_at,d.synced_at) as synced_at,
       d.finalized_at,d.status,d.error_message,d.created_at,
       greatest(s.synced_at,d.updated_at) as updated_at
from sub2api_usage_daily_snapshots d
left join sub2api_usage_daily_sync_state s using(target_id,remote_account_id,usage_date);

create or replace function oaix_refresh_usage_rollup(p_target bigint,p_account bigint)
returns void language plpgsql as $$
begin
    insert into sub2api_usage_rollups(target_id,remote_account_id)
    values(p_target,p_account) on conflict do nothing;
    perform 1 from sub2api_usage_rollups
    where target_id=p_target and remote_account_id=p_account for update;
    update sub2api_usage_rollups r set
        account_cost_usd=x.account_cost_usd,standard_cost_usd=x.standard_cost_usd,
        user_cost_usd=x.user_cost_usd,total_requests=x.total_requests,total_tokens=x.total_tokens,
        synced_at=x.synced_at,failed=x.failed,missing_days=x.missing_days,
        finalized_dates=x.finalized_dates,ready=true
    from (
        select coalesce(sum(d.account_cost_usd) filter(where d.status='synced'),0) as account_cost_usd,
               coalesce(sum(d.standard_cost_usd) filter(where d.status='synced'),0) as standard_cost_usd,
               coalesce(sum(d.user_cost_usd) filter(where d.status='synced'),0) as user_cost_usd,
               coalesce(sum(d.total_requests) filter(where d.status='synced'),0) as total_requests,
               coalesce(sum(d.total_tokens) filter(where d.status='synced'),0) as total_tokens,
               max(d.synced_at) filter(where d.status='synced') as synced_at,
               coalesce(bool_or(d.status<>'synced'),false) as failed,
               coalesce(max(d.usage_date)-b.through_date > count(d.usage_date) filter(where d.status='synced'),true) as missing_days,
               coalesce(range_agg(daterange(d.usage_date,d.usage_date+1,'[)')) filter(where d.finalized_at is not null),'{}'::datemultirange) as finalized_dates
        from sub2api_usage_snapshots b
        left join sub2api_usage_daily_current d
          on d.target_id=b.target_id and d.remote_account_id=b.remote_account_id and d.usage_date>b.through_date
        where b.target_id=p_target and b.remote_account_id=p_account
        group by b.through_date
    ) x where r.target_id=p_target and r.remote_account_id=p_account;
end $$;

create or replace function oaix_usage_rollup_lock() returns trigger language plpgsql as $$
declare t bigint; a bigint;
begin
    if tg_op='DELETE' then t:=old.target_id; a:=old.remote_account_id;
    else t:=new.target_id; a:=new.remote_account_id; end if;
    insert into sub2api_usage_rollups(target_id,remote_account_id) values(t,a) on conflict do nothing;
    perform 1 from sub2api_usage_rollups where target_id=t and remote_account_id=a for update;
    if tg_op='DELETE' then return old; end if;
    return new;
end $$;

create or replace function oaix_usage_rollup_changed() returns trigger language plpgsql as $$
begin
    if tg_op='DELETE' then
        if tg_table_name='sub2api_usage_daily_snapshots' then
            delete from sub2api_usage_daily_sync_state
            where target_id=old.target_id and remote_account_id=old.remote_account_id and usage_date=old.usage_date;
        end if;
        update sub2api_usage_rollups set ready=false
        where target_id=old.target_id and remote_account_id=old.remote_account_id;
        return old;
    end if;
    update sub2api_usage_rollups set ready=false
    where target_id=new.target_id and remote_account_id=new.remote_account_id;
    if tg_op='UPDATE' and (old.target_id,old.remote_account_id) is distinct from (new.target_id,new.remote_account_id) then
        update sub2api_usage_rollups set ready=false
        where target_id=old.target_id and remote_account_id=old.remote_account_id;
    end if;
    return new;
end $$;

create or replace function oaix_usage_checked() returns trigger language plpgsql as $$
begin
    update sub2api_usage_rollups r set synced_at=greatest(r.synced_at,new.synced_at)
    from sub2api_usage_snapshots b,sub2api_usage_daily_snapshots d
    where r.target_id=new.target_id and r.remote_account_id=new.remote_account_id
      and b.target_id=r.target_id and b.remote_account_id=r.remote_account_id
      and d.target_id=r.target_id and d.remote_account_id=r.remote_account_id
      and d.usage_date=new.usage_date and d.usage_date>b.through_date and d.status='synced';
    return new;
end $$;

create or replace trigger oaix_usage_baseline_lock before insert or update or delete on sub2api_usage_snapshots
for each row execute function oaix_usage_rollup_lock();
create or replace trigger oaix_usage_baseline_changed after insert or update or delete on sub2api_usage_snapshots
for each row execute function oaix_usage_rollup_changed();
create or replace trigger oaix_usage_daily_lock before insert or update or delete on sub2api_usage_daily_snapshots
for each row execute function oaix_usage_rollup_lock();
create or replace trigger oaix_usage_daily_changed after insert or update or delete on sub2api_usage_daily_snapshots
for each row execute function oaix_usage_rollup_changed();
create or replace trigger oaix_usage_check_changed after insert or update on sub2api_usage_daily_sync_state
for each row execute function oaix_usage_checked();

-- Keep the rare exact fallback behind a bounded function estimate. Inlining
-- its historical scan for every already-ready account made PostgreSQL 18 JIT
-- compile a large unused plan on each poll (verified with EXPLAIN ANALYZE).
create or replace function oaix_usage_exact_fallback(p_target bigint,p_account bigint,p_through date,p_ready boolean)
returns table(account_cost_usd numeric,standard_cost_usd numeric,user_cost_usd numeric,
              total_requests numeric,total_tokens numeric,synced_at timestamptz,
              failed boolean,missing_days boolean,finalized_dates datemultirange)
language plpgsql stable rows 1 cost 100 as $$
begin
    if p_ready then return; end if;
    return query
    select coalesce(sum(d.account_cost_usd) filter(where d.status='synced'),0),
           coalesce(sum(d.standard_cost_usd) filter(where d.status='synced'),0),
           coalesce(sum(d.user_cost_usd) filter(where d.status='synced'),0),
           coalesce(sum(d.total_requests) filter(where d.status='synced'),0),
           coalesce(sum(d.total_tokens) filter(where d.status='synced'),0),
           max(d.synced_at) filter(where d.status='synced'),
           coalesce(bool_or(d.status<>'synced'),false),
           coalesce(max(d.usage_date)-p_through > count(*) filter(where d.status='synced'),true),
           range_agg(daterange(d.usage_date,d.usage_date+1,'[)')) filter(where d.finalized_at is not null)
    from sub2api_usage_daily_current d
    where d.target_id=p_target and d.remote_account_id=p_account and d.usage_date>p_through;
end $$;

-- The usual result is empty, not the 100 ranges x 1000 days assumed by two
-- nested SRFs. Encapsulate expansion so the outer join has a realistic bound.
create or replace function oaix_usage_unsettled_dates(p_through date,p_before date,p_finalized datemultirange)
returns setof date language plpgsql immutable rows 1 cost 5 as $$
declare remaining datemultirange;
begin
    if p_through is null or p_through>=p_before-1 then return; end if;
    remaining:=datemultirange(daterange(p_through+1,p_before,'[)'))-coalesce(p_finalized,'{}'::datemultirange);
    if remaining='{}'::datemultirange then return; end if;
    return query
    select lower(gaps.days)+d.day_offset
    from unnest(remaining) gaps(days)
    cross join lateral generate_series(0,upper(gaps.days)-lower(gaps.days)-1) d(day_offset);
end $$;

create or replace view sub2api_usage_account_current as
select b.target_id,b.remote_account_id,b.token_id,b.through_date,b.status,
       b.account_cost_usd+case when b.through_date is null then 0 else coalesce(r.account_cost_usd,x.account_cost_usd,0) end as account_cost_usd,
       b.standard_cost_usd+case when b.through_date is null then 0 else coalesce(r.standard_cost_usd,x.standard_cost_usd,0) end as standard_cost_usd,
       b.user_cost_usd+case when b.through_date is null then 0 else coalesce(r.user_cost_usd,x.user_cost_usd,0) end as user_cost_usd,
       b.total_requests+case when b.through_date is null then 0 else coalesce(r.total_requests,x.total_requests,0) end as total_requests,
       b.total_tokens+case when b.through_date is null then 0 else coalesce(r.total_tokens,x.total_tokens,0) end as total_tokens,
       greatest(b.synced_at,coalesce(r.synced_at,x.synced_at)) as synced_at,
       coalesce(r.synced_at,x.synced_at) as daily_synced_at,
       coalesce(r.failed,x.failed,false) as daily_failed,
       coalesce(r.missing_days,x.missing_days,true) as missing_days,
       coalesce(r.finalized_dates,x.finalized_dates,'{}'::datemultirange) as finalized_dates
from sub2api_usage_snapshots b
left join sub2api_usage_rollups r on r.target_id=b.target_id and r.remote_account_id=b.remote_account_id and r.ready
left join lateral oaix_usage_exact_fallback(b.target_id,b.remote_account_id,b.through_date,r.target_id is not null) x on r.target_id is null;
