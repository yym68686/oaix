-- Run with psql, outside a transaction, after reviewing pg_stat_user_indexes.
-- These legacy Python indexes have no matching predicate in the Go gateway.
-- Guard the exact single-column non-unique shape and zero observed scans.
-- No tables, request payloads, or accounting data are removed.
\set ON_ERROR_STOP on
set lock_timeout='500ms';
set statement_timeout='30s';
select format('drop index concurrently %I.%I',n.nspname,c.relname)
from (values
 ('ix_gateway_request_logs_request_payload_hash','request_payload_hash'),
 ('ix_gateway_request_logs_upstream_payload_hash','upstream_payload_hash'),
 ('ix_gateway_request_logs_prompt_dynamic_hash','prompt_dynamic_hash')
) expected(index_name,column_name)
join pg_class c on c.relname=expected.index_name
join pg_namespace n on n.oid=c.relnamespace and n.nspname='public'
join pg_index i on i.indexrelid=c.oid
join pg_class t on t.oid=i.indrelid and t.relname='gateway_request_logs'
join pg_stat_user_indexes s on s.indexrelid=c.oid
where not i.indisunique and not i.indisprimary and i.indnatts=1
  and i.indpred is null and i.indexprs is null and s.idx_scan=0
  and pg_get_indexdef(i.indexrelid,1,true)=expected.column_name
\gexec
