# oaix API 化覆盖梳理与 TODO

> 调研时间：2026-06-19
> 范围：仅 `oaix` 当前 Go 版实现。旧 Python 代码只作为行为参考。
> 目标：让 oaix 的所有业务能力都可以通过稳定、可文档化、可测试、可自动化调用的 API 完成，前端只作为这些 API 的一个客户端。

## 结论

当前 oaix **没有完全 API 化**。

核心代理能力和主要管理面已经有 API：

- OpenAI-compatible 代理：`/v1/responses`、`/v1/chat/completions`、`/v1/images/generations`、`/v1/images/edits`、responses get/delete/cancel/compact。
- Key 管理：列表、详情、启用/禁用、备注、删除、批量启停/删除、测试、额度查询、已用金额查询。
- 导入：粘贴/结构化导入、ChatGPT OAuth start/exchange、批次列表、批次详情、取消、删除批次及 key。
- 观测：最近请求、24h 聚合、runtime、settings、health、metrics。
- 调度设置：`token-selection` 和 `active_stream_cap` 更新。

但距离“所有功能都可 API 调用”还有明显缺口：

1. API 契约没有产品化：没有 OpenAPI schema、版本号、稳定错误模型、幂等语义、生成客户端和覆盖测试矩阵。
2. 多个列表 API 只有粗略 `limit`，缺分页、过滤、排序、详情查询和导出。
3. 前端仍承担部分业务输入解析，例如导入文件/粘贴文本解析，服务端没有对应 parse/preview/upload API。
4. 部分后台能力有存储表或内部函数，但没有外部 API，例如 audit log 查询、prompt cache affinity 查看/清理、request log outbox/聚合/reconcile 运维动作。
5. 导入 job API 还不完整：当前 Go 管理接口会同步完成导入并创建 completed job；worker/staging 状态机存在，但外部 API 没有完整暴露“创建异步 job、查看 item 分页、重试、重排、失败项”等能力。
6. 管理凭证仍是环境变量 + 前端 localStorage 模式，没有服务端 API 管理 key 轮换、撤销、角色和审计读取。

## 当前暴露的 API 面

### 健康与静态资源

- `GET /`
- `GET /assets/*`
- `GET /livez`
- `GET /healthz`
- `GET /metrics`

### 管理 API

- `GET /admin/token-selection`
- `POST /admin/token-selection`
- `GET /admin/tokens`
- `GET /admin/tokens/{token_id}`
- `GET /admin/tokens/costs`
- `GET /admin/tokens/quota`
- `POST /admin/tokens/{token_id}/activation`
- `POST /admin/tokens/{token_id}/remark`
- `POST /admin/tokens/{token_id}/probe`
- `DELETE /admin/tokens/{token_id}`
- `POST /admin/tokens/batch`
- `POST /admin/tokens/import`
- `POST /admin/oauth/openai/start`
- `POST /admin/oauth/openai/exchange`
- `GET /admin/tokens/import-batches`
- `GET /admin/tokens/import-jobs/{job_id}`
- `POST /admin/tokens/import-jobs/{job_id}/cancel`
- `DELETE /admin/tokens/import-jobs/{job_id}`
- `GET /admin/requests`
- `GET /admin/analytics`
- `GET /admin/runtime`
- `GET /admin/settings`
- `POST /admin/settings/{key}`

### OpenAI-compatible 代理 API

- `POST /v1/responses`
- `GET /v1/responses/{response_id}`
- `DELETE /v1/responses/{response_id}`
- `POST /v1/responses/{response_id}/cancel`
- `POST /v1/responses/compact`
- `POST /v1/chat/completions`
- `POST /v1/images/generations`
- `POST /v1/images/edits`

## 已 API 化但不完整的能力

### Key 列表与筛选

现状：

- `GET /admin/tokens` 支持 `limit`、`offset`、`q`、`status`、`plan`、`sort`。
- 返回 `counts`、`plan_counts`、`pagination`、`items`。

缺口：

- README 里仍提到 `plan_type` / `import_batch_id`，但当前 Go handler 接收的是 `plan`，且 `TokenListOptions` 没有 `import_batch_id`。文档和实现不一致。
- 不支持按导入批次、来源、最近错误类型、冷却原因、创建时间范围、最近使用时间范围、余额状态、并发状态筛选。
- 不支持稳定 cursor 分页；offset 在大量数据和并发写入下不稳定。
- 不支持字段选择、导出和长任务导出。

### Key 操作

现状：

- 单个 key 可启用/禁用、更新备注、删除、测试。
- 批量 API 支持启用、禁用、删除。
- 列表可附带额度、并发、已用金额。

缺口：

- 没有 API 设置/清除 cooldown、last_error、disabled_reason。
- 没有 API 更新 plan、email、account_id、source、source_file 等非 secret 元数据。
- 没有 API 轮换/更新 refresh token、access token、id token 的安全入口。
- 没有 API 合并重复 key、拆分 merged key、查看 refresh token 历史链。
- 没有批量测试、批量刷新额度、批量清冷却、批量设置备注/计划/来源。
- 没有导出 key 元数据的 API。

### 额度与已用金额

现状：

- `GET /admin/tokens/quota?ids=...` 可查询指定 key 的 quota。
- `GET /admin/tokens/costs?ids=...` 可查询已用金额；`observed_cost_usd` 保持 OAIX 本地口径，`sub2api_observed_cost_usd` 为已同步到 Sub2API 的账号用量，`combined_observed_cost_usd` 为两者合计。
- Sub2API 用量由后台限速同步并持久保存最后一次成功快照；远端查询失败不会把已有金额覆盖为 0，`sub2api_usage_synced_at` 和 `sub2api_usage_stale` 用于判断新鲜度。
- `include_quota=true` 可让 token/detail/import job API 附带额度。

缺口：

- 没有 `force_refresh` / `cache_policy` 参数。
- 没有异步额度刷新 job 和进度查询。
- 没有批量刷新额度 API。
- 没有按 quota 错误类型筛选 key 的 API。
- 没有 quota 快照持久化查询 API；当前 quota 主要是内存缓存 + 同步到 plan/type。

### 导入批次与导入 job

现状：

- `POST /admin/tokens/import` 接收结构化 payload，当前 Go handler 会 `completeTokenImport`，导入完成后返回 job/result。
- `GET /admin/tokens/import-batches` 返回最近批次摘要。
- `GET /admin/tokens/import-jobs/{id}` 返回 job、items、tokens。
- 支持取消和删除导入批次。
- 前端支持粘贴、文件和 ChatGPT OAuth 三种导入入口。

缺口：

- `POST /admin/tokens/import` 当前不是标准异步 job API；后台 staging/worker 机制存在，但没有完整暴露为 API 契约。
- 批次列表只有 `limit`，没有分页、状态筛选、计划筛选、来源筛选、时间范围、排序。
- 批次详情一次性返回所有 items/tokens，缺 item/token 的分页、筛选和排序。
- 没有导入 parse/preview API；前端本地解析文本和文件，CLI/第三方调用者需要自己复制解析规则。
- 没有 multipart 文件上传导入 API。
- 没有失败项单独查询 API。
- 没有重试 job、重试 item、跳过 item、重排队列、取消单个 item、恢复 stale job 的 API。
- 没有 import job progress/SSE 或 polling 标准字段。
- 没有幂等键，重复提交大批量导入时无法安全重试。
- README 的“只写 staging，worker 验证发布”描述和当前 Go 管理 API 行为不一致，需要先修契约。

### ChatGPT OAuth 导入

现状：

- `POST /admin/oauth/openai/start` 创建内存 session 并返回 `auth_url`。
- `POST /admin/oauth/openai/exchange` 支持用 code/callback URL 兑换 token 并导入。
- `GET /auth/callback` 支持浏览器回调并重定向回前端结果页。

缺口：

- OAuth session 存在进程内内存，重启/多副本会丢失或不一致。
- 没有 OAuth session 查询、取消、过期清理和审计 API。
- start 阶段没有创建 pending import job；失败无法在批次页作为正式 job 追踪。
- 没有纯 API 轮询状态模型；浏览器回调和前端解析结果仍耦合页面体验。
- 没有对 OAuth flow 的稳定 OpenAPI 示例和错误码定义。

### 请求日志与分析

现状：

- `GET /admin/requests?limit=N` 返回最近请求和 24h summary；默认不做全表精确 count，避免大表超时，确需精确总数时传 `include_total=true`。
- `GET /admin/analytics?hours=N` 返回模型聚合。

缺口：

- 没有 cursor/offset 分页。
- 没有按 request_id、trace/session hash、model、endpoint、status、success、token_id、account_id、stream、时间范围、错误关键字筛选。
- 没有单条 request detail API。
- 没有导出请求日志 API。
- 没有 cache hit ratio、prompt cache source、attempt_count、TTFT/duration 分桶的专门分析 API。
- 没有 in-flight 请求 API，只能从 runtime 粗略看。
- 没有按 token/account/model 聚合成本和成功率 API。

### Runtime 与后台运维

现状：

- `GET /admin/runtime` 返回 token pool、request log、transport、state commit、DB pool、config 摘要。

缺口：

- 没有触发 token snapshot refresh 的 API。
- 没有 request log outbox drain、hourly stats aggregation、request token cost reconcile、retention cleanup 的手动触发 API。
- 没有 worker 状态和 import worker metrics 的专用 API。
- 没有 upstream transport pool 详细连接状态 API。
- 没有 feature flags/runtime toggles 的安全管理 API。
- 没有 read-only 和 mutating 运维 API 的权限隔离。

### Settings

现状：

- `GET /admin/settings` 列出 gateway_settings。
- `POST /admin/settings/{key}` 可写任意 JSON。
- `GET/POST /admin/token-selection` 对 `token_selection` 做了 typed wrapper。

缺口：

- 除 `token_selection` 外，其他设置没有 typed endpoint 和 schema validation。
- 没有删除 setting API。
- 没有 setting history/version API。
- 没有 dry-run/validate API。
- 环境变量配置只能通过部署环境改，不是 API 管理。

### Prompt Cache / Affinity

现状：

- prompt cache affinity、response owner binding 有持久化表和运行逻辑。
- 请求日志里记录了一部分 prompt cache trace 字段。

缺口：

- 没有 API 查看 `prompt_affinity_lanes`。
- 没有 API 查看/清理 `response_owner_bindings`。
- 没有按 prompt cache key、session、token、model 查询命中率和 lane 状态的 API。
- 没有手动清理某个 token 的 owner binding / lane binding API。

### Audit

现状：

- 多个管理操作会写 `admin_audit_logs`。

缺口：

- 没有 `GET /admin/audit-logs`。
- 没有按 actor/action/target/time 查询。
- 没有审计导出。
- 没有把请求 id / trace id 贯穿到 audit payload 的统一规则。

### 管理凭证与权限

现状：

- `SERVICE_API_KEYS` 来自环境变量。
- 前端保存 Service API Key 到 localStorage。
- 后端只做 bearer 或 `X-API-Key` 对比。

缺口：

- 没有 API 创建、轮换、撤销、列出管理 key。
- 没有 key hash 存储和 last_used_at。
- 没有角色/权限，例如 read-only、operator、admin。
- 没有按 endpoint/action 做权限校验。
- 没有服务端 session 或短期 token。

### API 契约、错误模型与客户端

现状：

- 前端手写 `frontend/src/lib/api.ts`。
- 后端直接返回 `map[string]any` 或结构体。

缺口：

- 没有 OpenAPI 3.1 schema。
- 没有 endpoint versioning。
- 没有统一 error envelope、error code、retryable 字段和 request_id。
- 没有幂等 header 契约。
- 没有生成 TypeScript client / Go client。
- 没有 contract tests 确保前端类型和后端返回一致。

## API 化终局原则

- 前端不得持有唯一业务规则。粘贴/文件导入解析、筛选、分页、批量操作、导出都应有服务端 API。
- 所有列表 API 必须有明确分页语义，优先 cursor，允许 limit 上限。
- 所有 mutating API 必须有幂等策略、审计记录、稳定错误码。
- 所有长任务必须返回 job id，并有 progress/detail/retry/cancel API。
- 所有运维动作必须分 read-only 和 mutating 权限。
- 所有 API 必须能从 OpenAPI schema 生成客户端并通过 contract test。
- 不通过 API 暴露 secret 明文；只提供写入、轮换、校验和脱敏摘要。

## 详细 TODO List

### P0：API 契约基线

- [x] 定义 API 化完成标准：页面功能、CLI 能力、后台任务、运维动作、观测查询都必须有 API。
- [x] 建立 `docs/api/` 目录，存放 OpenAPI、示例和状态机文档。
- [x] 生成当前 Go 路由清单，并与 `frontend/src/lib/api.ts` 自动比对。
- [x] 修正 README 中过期 API 描述：`plan_type`、`import_batch_id`、导入 staging/worker 行为等。
- [x] 定义统一响应 envelope：`data`、`error.code`、`error.message`、`error.retryable`、`request_id`。
- [x] 定义所有 mutating API 的幂等策略：`Idempotency-Key`、重复提交返回语义、幂等记录保留时间。
- [x] 定义管理 API 权限模型：`read`, `operate`, `admin`, `secret_write`。
- [x] 为现有所有 `/admin/*` 增加 contract tests，锁住当前行为。
- [x] 生成 TypeScript API client，替换前端手写散落调用。

### P1：列表 API 标准化

- [x] `GET /admin/tokens` 增加 cursor 分页。
- [x] `GET /admin/tokens` 增加 `import_job_id` / `import_batch_id` 筛选。
- [x] `GET /admin/tokens` 增加 `source` / `source_file` 筛选。
- [x] `GET /admin/tokens` 增加 `created_from` / `created_to` / `last_used_from` / `last_used_to`。
- [x] `GET /admin/tokens` 增加 `has_error` / `error_code` / `cooldown_reason` 筛选。
- [x] `GET /admin/tokens` 增加 `quota_state` 筛选，例如 `ok`, `low_5h`, `low_7d`, `error`, `unknown`。
- [x] `GET /admin/tokens` 增加稳定 sort 枚举和默认排序文档。
- [x] `GET /admin/tokens/export` 支持按同一筛选条件导出 CSV/JSON。
- [x] 所有列表 API 返回统一 `pagination` 对象。

### P2：Key 生命周期 API 补齐

- [x] `PATCH /admin/tokens/{id}` 支持更新非 secret 元数据：remark、plan_type、email、account_id、source。
- [x] `POST /admin/tokens/{id}/cooldown` 支持设置 cooldown_until 和 reason。
- [x] `DELETE /admin/tokens/{id}/cooldown` 支持清除 cooldown。
- [x] `DELETE /admin/tokens/{id}/last-error` 支持清除 last_error。
- [x] `POST /admin/tokens/{id}/secrets` 支持安全写入/轮换 access/refresh/id token。
- [x] `POST /admin/tokens/{id}/refresh` 支持主动用 refresh token 刷新 access token。
- [x] `GET /admin/tokens/{id}/refresh-history` 返回脱敏 RT 历史链。
- [x] `POST /admin/tokens/{id}/merge` 支持合并重复 key。
- [x] `POST /admin/tokens/{id}/unmerge` 支持撤销错误合并。
- [x] `POST /admin/tokens/batch` 扩展动作：probe、quota_refresh、clear_cooldown、set_plan、set_remark、export。
- [x] 所有 key mutating API 写入 audit log 并带 request_id。

### P3：额度与金额 API

- [x] `GET /admin/tokens/quota` 增加 `force_refresh=true`。
- [x] `POST /admin/tokens/quota-refresh` 创建异步刷新 job。
- [x] `GET /admin/tokens/quota-refresh/{job_id}` 查询刷新进度。
- [x] `GET /admin/tokens/{id}/quota-history` 查询持久化 quota 快照。
- [x] `GET /admin/tokens/costs` 支持按时间范围、模型、endpoint 计算。
- [x] `GET /admin/costs/by-token` 支持分页聚合。
- [x] `GET /admin/costs/by-account` 支持分页聚合。
- [x] `POST /admin/request-costs/reconcile` 触发 request token cost reconcile。
- [x] `GET /admin/request-costs/reconcile` 查询 reconcile 状态。

### P4：导入 API 终局化

- [x] 明确定义唯一 import job 状态机和 item 状态机。
- [x] `POST /admin/import/parse` 支持粘贴文本解析预览。
- [x] `POST /admin/import/upload` 支持 multipart 文件上传解析预览。
- [x] `POST /admin/import/jobs` 创建异步导入 job，返回 job id。
- [x] `POST /admin/import/jobs` 支持 `Idempotency-Key`。
- [x] `GET /admin/import/jobs` 支持分页、状态、来源、时间范围、排序。
- [x] `GET /admin/import/jobs/{id}` 返回 job summary。
- [x] `GET /admin/import/jobs/{id}/items` 支持 item 分页、状态、action、错误筛选。
- [x] `GET /admin/import/jobs/{id}/tokens` 支持 token 分页、状态、计划筛选。
- [x] `GET /admin/import/jobs/{id}/failed-items` 返回失败项摘要。
- [x] `POST /admin/import/jobs/{id}/retry` 重试整个 job。
- [x] `POST /admin/import/items/{item_id}/retry` 重试单项。
- [x] `POST /admin/import/items/{item_id}/skip` 跳过单项。
- [x] `POST /admin/import/jobs/{id}/cancel` 语义明确为取消未完成 job。
- [x] `DELETE /admin/import/jobs/{id}` 支持 `dry_run=true`，先返回将删除的 key 数。
- [x] `GET /admin/import/jobs/{id}/events` 提供 SSE 进度流。
- [x] 旧 `/admin/tokens/import*` 路由保留兼容或迁移到新路由，并写清 deprecation。
- [x] 导入页面只调用新 import API，不在前端复制解析规则。

### P5：OAuth API 终局化

- [x] OAuth session 持久化到数据库，支持多副本和重启恢复。
- [x] `POST /admin/oauth/openai/sessions` 创建 OAuth session，并可选择同时创建 pending import job。
- [x] `GET /admin/oauth/openai/sessions/{id}` 查询 session 状态。
- [x] `DELETE /admin/oauth/openai/sessions/{id}` 取消 session。
- [x] `POST /admin/oauth/openai/sessions/{id}/exchange` 兑换 code/callback URL。
- [x] OAuth callback 只完成状态写入，并关联 import job。
- [x] OAuth 失败必须产生可查询 job/session 错误，不只依赖前端 toast。
- [x] OAuth API 示例覆盖浏览器跳转、CLI 手动粘贴 callback URL、服务端回调三种模式。

### P6：请求日志与分析 API

- [x] `GET /admin/requests` 增加 cursor 分页。
- [x] `GET /admin/requests` 增加 `request_id` / `model` / `endpoint` / `status_code` / `success` / `token_id` / `account_id` / `stream` / `from` / `to` 筛选。
- [x] `GET /admin/requests/{request_id}` 返回完整 request detail。
- [x] `GET /admin/requests/export` 支持 CSV/JSON 导出。
- [x] `GET /admin/analytics/models` 支持时间范围、bucket 粒度。
- [x] `GET /admin/analytics/cache` 返回 cache hit ratio、cached token ratio、cache source 分布。
- [x] `GET /admin/analytics/errors` 返回错误类型、模型、endpoint、token 维度聚合。
- [x] `GET /admin/analytics/latency` 返回 TTFT/duration 分位数。
- [x] `GET /admin/inflight` 返回当前活跃请求和等待队列摘要。

### P7：Runtime / Worker / 运维 API

- [x] `GET /admin/workers` 返回 embedded worker 和独立 worker 状态。
- [x] `GET /admin/request-log-outbox` 返回队列深度和失败重试统计。
- [x] `POST /admin/request-log-outbox/drain` 手动 drain。
- [x] `POST /admin/maintenance/hourly-stats` 手动聚合小时统计。
- [x] `POST /admin/maintenance/cleanup-request-logs` 手动清理请求日志。
- [x] `POST /admin/token-pool/refresh` 手动刷新内存 snapshot。
- [x] `GET /admin/token-pool/snapshot` 返回脱敏 snapshot 和选择器状态。
- [x] `POST /admin/transport/reset-idle` 清理上游 idle 连接。
- [x] 所有 mutating 运维 API 增加权限和审计。

### P8：Prompt Cache / Affinity API

- [x] `GET /admin/prompt-cache/lanes` 查询 prompt affinity lanes。
- [x] `GET /admin/prompt-cache/lanes/{prompt_key_hash}` 查询单个 lane 状态。
- [x] `DELETE /admin/prompt-cache/lanes/{prompt_key_hash}` 清理指定 cache key。
- [x] `GET /admin/response-owners` 查询 response owner bindings。
- [x] `DELETE /admin/response-owners/{response_id_hash}` 删除指定绑定。
- [x] `DELETE /admin/tokens/{id}/response-owners` 删除某 key 的所有 owner bindings。
- [x] `GET /admin/prompt-cache/stats` 返回命中率、lane spillover、fallback 次数。

### P9：Settings / Audit / Auth API

- [x] `GET /admin/settings/{key}` 查询单个 setting。
- [x] `DELETE /admin/settings/{key}` 删除 setting。
- [x] `POST /admin/settings/{key}/validate` dry-run 校验。
- [x] 为已知 setting 提供 typed endpoints 和 JSON schema。
- [x] `GET /admin/audit-logs` 支持分页和筛选。
- [x] `GET /admin/audit-logs/{id}` 查询审计详情。
- [x] `GET /admin/api-keys` 列出管理 key 脱敏摘要。
- [x] `POST /admin/api-keys` 创建管理 key。
- [x] `POST /admin/api-keys/{id}/rotate` 轮换管理 key。
- [x] `DELETE /admin/api-keys/{id}` 撤销管理 key。
- [x] API key 增加角色、last_used_at、created_by、revoked_at。

### P10：前端收敛为纯 API 客户端

- [x] 前端不再自行解析导入文本，改调 `POST /admin/import/parse`。
- [x] 前端文件导入改调 `POST /admin/import/upload`。
- [x] 前端所有列表使用后端 cursor/pagination。
- [x] 前端所有筛选项来自后端 schema/options API。
- [x] 前端不再手写 API 返回类型，改用 OpenAPI 生成类型。
- [x] 前端错误展示直接使用统一 error code/message。
- [x] 增加 Playwright smoke test：每个页面只通过 API 渲染，不依赖隐藏本地状态。

### P11：测试与发布门禁

- [x] 每个 `/admin/*` endpoint 有 handler integration test。
- [x] 每个 `/v1/*` endpoint 有兼容 fixture test。
- [x] 每个列表 API 有分页一致性测试。
- [x] 每个 mutating API 有幂等测试。
- [x] 每个权限角色有访问控制测试。
- [x] OpenAPI schema 在 CI 中校验。
- [x] 生成 TypeScript client 后 `npm run build:web` 必须通过。
- [x] 线上部署后自动跑 health、admin read-only、关键 mutating dry-run smoke。

## 建议实施顺序

1. 先补 OpenAPI、错误模型、路由/前端调用自动比对，避免继续靠人工记忆维护 API。
2. 先修文档与实现不一致，尤其是导入 job 和 token list 查询参数。
3. 补齐导入 API，因为导入是当前前端和运维最复杂、最容易出错的功能。
4. 补齐请求日志和 analytics API，因为线上排障频繁依赖这些数据。
5. 补齐 runtime/worker/prompt-cache/audit/auth 这些非前端核心但对自动化运维必要的 API。

## 风险点

- 不要为了 API 化暴露 access token / refresh token 明文读取接口。只能提供写入、轮换、验证和脱敏摘要。
- 不要把重型分析直接挂在同步 5 秒管理接口上；长查询必须 job 化或读聚合表。
- 不要让前端和 API 使用不同筛选语义；筛选条件必须后端定义，前端只渲染。
- 不要在多副本部署前继续依赖内存 OAuth session。
- 不要让 arbitrary `POST /admin/settings/{key}` 成为绕过 typed validation 的长期方案。
