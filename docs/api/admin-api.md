# oaix Admin API

更新时间：2026-06-19

oaix 管理面以 `/admin/*` 为稳定 API 前缀，前端只是这些 API 的一个客户端。所有管理请求继续兼容 `Authorization: Bearer <SERVICE_API_KEY>` 和 `X-API-Key`。

多用户平台新增两类前缀：

- `/api/*`：普通用户自助 API，只能访问当前 API Key 所属用户的资源。
- `/api/admin/*`：平台管理员 API，可查看和管理所有用户、所有用户的号池、请求和审计。

旧 `/admin/*` 继续保留为 Service/Admin 兼容前缀。新前端优先使用 `/api/*` 和 `/api/admin/*`，便于在同一个系统内区分“我的资源”和“全局资源”。

## 契约

- 错误响应保留旧字段 `detail`，并新增稳定字段 `error.code`、`error.message`、`error.retryable`、`request_id`。
- mutating API 接受 `Idempotency-Key` header。导入 job 创建会记录该 header 到审计日志，后续可扩展为严格重放语义。
- 列表 API 统一返回 `pagination.limit`、`pagination.offset`、`pagination.returned`、`pagination.total`、`pagination.has_previous`、`pagination.has_next`。
- OpenAPI 快照可通过 `GET /admin/openapi.json` 获取。

## 用户 API

- Auth：`POST /api/auth/register`、`POST /api/auth/login`。注册只需要邮箱和密码。
- 我的账号：`GET /api/me`、`GET /api/me/usage`、`GET /api/me/pool-summary`。
- 我的 API Key：`GET /api/me/api-keys`、`POST /api/me/api-keys`、`DELETE /api/me/api-keys/{key_id}`。
- 我的设置：`GET /api/me/settings`、`GET /api/me/settings/{key}`、`POST /api/me/settings/{key}`、`DELETE /api/me/settings/{key}`。这些设置写入 `user_settings`，不会污染全局 `gateway_settings`。
- 我的 Key：`GET /api/tokens`、`GET /api/tokens/{token_id}`、`PATCH /api/tokens/{token_id}`、`DELETE /api/tokens/{token_id}`、`POST /api/tokens/{token_id}/probe`。
- 我的导入：`POST /api/import/parse`、`POST /api/import/upload`、`POST /api/import/jobs`、`GET /api/import/jobs`、`GET /api/import/jobs/{job_id}`、`POST /api/import/jobs/{job_id}/cancel`、`DELETE /api/import/jobs/{job_id}`、`GET /api/import/jobs/{job_id}/items`、`GET /api/import/jobs/{job_id}/tokens`。
- 我的请求：`GET /api/requests`。

## 平台管理员 API

- 用户：`GET /api/admin/users`、`POST /api/admin/users`、`GET /api/admin/users/{user_id}`、`PATCH /api/admin/users/{user_id}`。
- 用户 API Key：`GET /api/admin/users/{user_id}/api-keys`、`POST /api/admin/users/{user_id}/api-keys`、`DELETE /api/admin/users/{user_id}/api-keys/{key_id}`。
- 用户资源：`GET /api/admin/users/{user_id}/tokens`、`GET /api/admin/users/{user_id}/import/jobs`、`GET /api/admin/users/{user_id}/requests`、`GET /api/admin/users/{user_id}/usage`。
- 号池：`GET /api/admin/pool-summary`、`GET /api/admin/pool-summary/by-user`、`GET /api/admin/analytics/users`。
- 请求：`GET /api/admin/requests`、`GET /api/admin/requests/export`。支持 `user_id`、`api_key_id`、`model`、`endpoint`、`status_code`、`success`、`stream`、`from`、`to` 等筛选。
- 审计：`GET /api/admin/audit-logs`、`GET /api/admin/audit-logs/{audit_id}`。

## 主要资源

- Key：`GET /admin/tokens`、`PATCH /admin/tokens/{id}`、`POST /admin/tokens/{id}/cooldown`、`DELETE /admin/token-cooldown/{id}`、`DELETE /admin/token-last-error/{id}`、`POST /admin/tokens/{id}/secrets`、`POST /admin/tokens/{id}/refresh`、`GET /admin/token-refresh-history/{id}`、`POST /admin/tokens/{id}/merge`、`POST /admin/tokens/{id}/unmerge`、`GET /admin/tokens/export`。
- Quota/成本：`GET /admin/tokens/quota?force_refresh=true`、`POST /admin/tokens/quota-refresh`、`GET /admin/tokens/quota-refresh/{job_id}`、`GET /admin/token-quota-history/{id}`、`GET /admin/costs/by-token`、`GET /admin/costs/by-account`。
- 导入：`POST /admin/import/parse`、`POST /admin/import/upload`、`POST /admin/import/jobs`、`GET /admin/import/jobs`、`GET /admin/import/jobs/{id}`、`GET /admin/import/jobs/{id}/items`、`GET /admin/import/jobs/{id}/tokens`、`GET /admin/import/jobs/{id}/failed-items`、`POST /admin/import/jobs/{id}/retry`、`POST /admin/import/items/{id}/retry`、`POST /admin/import/items/{id}/skip`、`GET /admin/import/jobs/{id}/events`。
- OAuth：`POST /admin/oauth/openai/sessions`、`GET /admin/oauth/openai/sessions/{id}`、`DELETE /admin/oauth/openai/sessions/{id}`、`POST /admin/oauth/openai/sessions/{id}/exchange`。旧 `start/exchange` 路由保留。
- 请求日志/分析：`GET /admin/requests`、`GET /admin/requests/{request_id}`、`GET /admin/requests/export`、`GET /admin/analytics/models`、`GET /admin/analytics/cache`、`GET /admin/analytics/errors`、`GET /admin/analytics/latency`、`GET /admin/inflight`。`GET /admin/requests` 默认不做大表精确总数统计，传 `include_total=true` 才返回精确总数。
- 运维：`GET /admin/workers`、`GET /admin/request-log-outbox`、`POST /admin/request-log-outbox/drain`、`POST /admin/maintenance/hourly-stats`、`POST /admin/maintenance/cleanup-request-logs`、`POST /admin/token-pool/refresh`、`GET /admin/token-pool/snapshot`、`POST /admin/transport/reset-idle`。
- Prompt cache：`GET /admin/prompt-cache/lanes`、`GET /admin/prompt-cache/lanes/{hash}`、`DELETE /admin/prompt-cache/lanes/{hash}`、`GET /admin/response-owners`、`DELETE /admin/response-owners/{hash}`、`DELETE /admin/token-response-owners/{id}`、`GET /admin/prompt-cache/stats`。
- Settings/Audit/Auth：`GET /admin/settings/{key}`、`DELETE /admin/settings/{key}`、`POST /admin/settings/{key}/validate`、`GET /admin/settings/schema`、`GET /admin/audit-logs`、`GET /admin/audit-logs/{id}`、`GET /admin/api-keys`、`POST /admin/api-keys`、`POST /admin/api-keys/{id}/rotate`、`DELETE /admin/api-keys/{id}`。

## 兼容路由

旧路由仍保留：

- `POST /admin/tokens/import`
- `GET /admin/tokens/import-batches`
- `GET /admin/tokens/import-jobs/{id}`
- `POST /admin/tokens/import-jobs/{id}/cancel`
- `DELETE /admin/tokens/import-jobs/{id}`
- `POST /admin/oauth/openai/start`
- `POST /admin/oauth/openai/exchange`
