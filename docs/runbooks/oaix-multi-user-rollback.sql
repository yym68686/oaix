-- oaix 多用户平台回滚辅助脚本
-- 使用前必须先备份数据库。该脚本只移除新增 owner/user 结构，不会自动合并或恢复业务数据。

begin;

-- 先移除依赖 owner 维度的新索引。
drop index if exists ux_gateway_request_hourly_stats_owner_bucket_model;
drop index if exists ix_codex_tokens_owner_active_ready;
drop index if exists ix_token_import_jobs_owner_status;
drop index if exists ix_gateway_request_logs_owner_started;
drop index if exists ix_user_settings_owner_updated;

-- 恢复旧 hourly stats 唯一约束形态。
alter table gateway_request_hourly_stats drop column if exists owner_user_id;
alter table gateway_request_hourly_stats drop column if exists cached_input_tokens;
create unique index if not exists gateway_request_hourly_stats_bucket_start_model_name_key
	on gateway_request_hourly_stats(bucket_start, model_name);

-- 删除 owner/API key 字段。注意：这会丢失多用户隔离信息。
alter table codex_tokens drop column if exists owner_user_id;
alter table token_secrets drop column if exists owner_user_id;
alter table token_scopes drop column if exists owner_user_id;
alter table token_refresh_history drop column if exists owner_user_id;
alter table codex_token_scoped_cooldowns drop column if exists owner_user_id;
alter table token_runtime_state drop column if exists owner_user_id;
alter table token_state_events drop column if exists owner_user_id;
alter table token_import_jobs drop column if exists owner_user_id;
alter table token_import_items drop column if exists owner_user_id;
alter table openai_oauth_sessions drop column if exists owner_user_id;
alter table token_quota_snapshots drop column if exists owner_user_id;
alter table gateway_request_logs drop column if exists owner_user_id;
alter table gateway_request_logs drop column if exists api_key_id;
alter table gateway_request_logs drop column if exists token_owner_user_id;
alter table gateway_request_token_costs drop column if exists owner_user_id;
alter table prompt_affinity_lanes drop column if exists owner_user_id;
alter table response_owner_bindings drop column if exists owner_user_id;
alter table admin_audit_logs drop column if exists actor_user_id;
alter table admin_audit_logs drop column if exists actor_api_key_id;
alter table admin_audit_logs drop column if exists actor_role;
alter table admin_audit_logs drop column if exists target_user_id;
alter table admin_audit_logs drop column if exists ip;
alter table admin_audit_logs drop column if exists user_agent;

-- 删除多用户新增表。
drop table if exists user_settings;
drop table if exists api_keys;
drop table if exists platform_users;

update schema_migrations set version = 6, updated_at = now() where name = 'oaix_go';

commit;
