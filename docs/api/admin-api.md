# oaix Admin API

更新时间：2026-06-19

oaix 管理面以 `/admin/*` 为稳定 API 前缀，前端只是这些 API 的一个客户端。所有管理请求继续兼容 `Authorization: Bearer <SERVICE_API_KEY>` 和 `X-API-Key`。

## 契约

- 错误响应保留旧字段 `detail`，并新增稳定字段 `error.code`、`error.message`、`error.retryable`、`request_id`。
- mutating API 接受 `Idempotency-Key` header。导入 job 创建会记录该 header 到审计日志，后续可扩展为严格重放语义。
- 列表 API 统一返回 `pagination.limit`、`pagination.offset`、`pagination.returned`、`pagination.total`、`pagination.has_previous`、`pagination.has_next`。
- OpenAPI 快照可通过 `GET /admin/openapi.json` 获取。

## 主要资源

- Key：`GET /admin/tokens`、`PATCH /admin/tokens/{id}`、`POST /admin/tokens/{id}/cooldown`、`DELETE /admin/token-cooldown/{id}`、`DELETE /admin/token-last-error/{id}`、`POST /admin/tokens/{id}/secrets`、`POST /admin/tokens/{id}/refresh`、`GET /admin/token-refresh-history/{id}`、`POST /admin/tokens/{id}/merge`、`POST /admin/tokens/{id}/unmerge`、`GET /admin/tokens/export`。
- Quota/成本：`GET /admin/tokens/quota?force_refresh=true`、`POST /admin/tokens/quota-refresh`、`GET /admin/tokens/quota-refresh/{job_id}`、`GET /admin/token-quota-history/{id}`、`GET /admin/costs/by-token`、`GET /admin/costs/by-account`。
- 导入：`POST /admin/import/parse`、`POST /admin/import/upload`、`POST /admin/import/jobs`、`GET /admin/import/jobs`、`GET /admin/import/jobs/{id}`、`GET /admin/import/jobs/{id}/items`、`GET /admin/import/jobs/{id}/tokens`、`GET /admin/import/jobs/{id}/failed-items`、`POST /admin/import/jobs/{id}/retry`、`POST /admin/import/items/{id}/retry`、`POST /admin/import/items/{id}/skip`、`GET /admin/import/jobs/{id}/events`。
- OAuth：`POST /admin/oauth/openai/sessions`、`GET /admin/oauth/openai/sessions/{id}`、`DELETE /admin/oauth/openai/sessions/{id}`、`POST /admin/oauth/openai/sessions/{id}/exchange`。旧 `start/exchange` 路由保留。
- 请求日志/分析：`GET /admin/requests`、`GET /admin/requests/{request_id}`、`GET /admin/requests/export`、`GET /admin/analytics/models`、`GET /admin/analytics/cache`、`GET /admin/analytics/errors`、`GET /admin/analytics/latency`、`GET /admin/inflight`。
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
