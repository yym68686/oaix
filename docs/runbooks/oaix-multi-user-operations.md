# oaix 多用户平台运维 Runbook

更新时间：2026-06-19

## 多用户迁移

1. 确认当前版本已经包含 schema version 8。
2. 备份 PostgreSQL：至少包含 `platform_users`、`api_keys`、`codex_tokens`、`token_import_jobs`、`gateway_request_logs`、`gateway_request_hourly_stats`、`gateway_settings`。
3. 部署新版本并运行 `go run ./cmd/oaix-migrate`，或确认容器启动时 `OAIX_AUTO_MIGRATE_ON_STARTUP=true`。
4. 核对 owner 回填：
   - `select count(*) from codex_tokens where owner_user_id is null;` 应为 0。
   - `select count(*) from token_import_jobs where owner_user_id is null;` 应为 0。
   - `select count(*) from gateway_request_hourly_stats where owner_user_id is null;` 应为 0。
5. 用 Service API Key 调用 `GET /api/admin/pool-summary` 和 `GET /api/admin/users`，确认全局号池和 bootstrap 用户可见。

## Bootstrap User 创建

迁移会幂等创建 `platform@oaix.local`：

```sql
insert into platform_users(email, display_name, role, status)
values ('platform@oaix.local', 'Platform Bootstrap', 'admin', 'active')
on conflict (email) do update
set role = case when platform_users.role = '' then excluded.role else platform_users.role end,
    status = case when platform_users.status in ('deleted', '') then excluded.status else platform_users.status end,
    updated_at = now();
```

旧数据会回填到该用户。Service API Key 未传 `X-OAIX-Act-As-User` 时，代理流量默认使用该用户池。

## API Key 轮换

1. 创建新 key：
   - 用户自助：`POST /api/me/api-keys`。
   - 管理员为用户创建：`POST /api/admin/users/{user_id}/api-keys`。
2. 在调用方切换到新 key。
3. 观察 `api_keys.last_used_at`，确认新 key 有流量。
4. 撤销旧 key：
   - 用户自助：`DELETE /api/me/api-keys/{key_id}`。
   - 管理员：`DELETE /api/admin/users/{user_id}/api-keys/{key_id}`。

撤销后 `revoked_at` 非空，后端鉴权会立即拒绝该 key。

## 用户禁用和恢复

禁用：

```bash
curl -X PATCH "$OAIX_URL/api/admin/users/$USER_ID" \
  -H "Authorization: Bearer $SERVICE_API_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"status":"disabled"}'
```

恢复：

```bash
curl -X PATCH "$OAIX_URL/api/admin/users/$USER_ID" \
  -H "Authorization: Bearer $SERVICE_API_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"status":"active"}'
```

禁用后，用户 API Key 的读取、写入和 `/v1/*` 代理都会返回 403。恢复后原 API Key 继续有效，除非 key 已单独撤销。

## 故障排查

用户看不到账号：

1. `GET /api/me` 确认当前 API Key 对应的 `user.id`。
2. `GET /api/tokens?include_total=true` 看用户自助视图。
3. 管理员调用 `GET /api/admin/users/{user_id}/tokens` 对照 owner。
4. SQL 核对：`select id, owner_user_id from codex_tokens where id = $1;`。

请求没有可用 key：

1. `GET /api/me/pool-summary` 或 `GET /api/admin/users/{user_id}/usage` 查看有效/冷却/禁用数量。
2. 检查 key 是否 `access_token` 为空、`cooldown_until` 在未来、`disabled_at` 非空。
3. 管理员可调用 `POST /admin/token-pool/refresh` 强制刷新全局快照；用户请求会按 owner 懒加载快照。

导入卡住：

1. `GET /api/import/jobs/{job_id}` 查看 job 状态。
2. `GET /api/import/jobs/{job_id}/items` 查看失败 item。
3. 管理员可用旧 API `POST /admin/import/jobs/{job_id}/retry` 重试。
4. 检查 worker 日志和 `IMPORT_JOB_MAX_CONCURRENCY`。

缓存率异常：

1. `GET /admin/analytics/cache?hours=1` 看全局缓存率。
2. `GET /api/admin/pool-summary/by-user?hours=1` 看用户维度缓存率。
3. 检查 `/v1/responses` 请求体是否被保留 `prompt_cache_key` / `previous_response_id`。
4. 检查 owner-scoped prompt affinity：不同用户不会共享同一个 affinity key。

## 上线后核对

```sql
select count(*) as tokens_total, count(owner_user_id) as tokens_owned from codex_tokens where merged_into_token_id is null;
select count(*) as jobs_total, count(owner_user_id) as jobs_owned from token_import_jobs;
select owner_user_id, count(*) from codex_tokens group by owner_user_id order by count(*) desc limit 10;
select owner_user_id, sum(request_count) from gateway_request_hourly_stats group by owner_user_id order by sum(request_count) desc limit 10;
```

`tokens_total` 应等于 `tokens_owned`，`jobs_total` 应等于 `jobs_owned`。如不一致，先停止新导入，再按 bootstrap user 回填缺失 owner。
