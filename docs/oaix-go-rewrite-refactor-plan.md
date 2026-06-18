# oaix Go 版终局架构收敛重构方案

> 状态：待审阅  
> 最后更新：2026-06-18  
> 范围：仅 `oaix` 项目。  
> 当前基线：项目已经完成从旧 Python 单体到 Go 版主体实现的迁移，本方案不再描述“从 Python 重写到 Go”，而是描述当前 Go 版如何收敛到终局架构。  
> 目标：系统性提升性能、降低冗余、提升灵活性、提升鲁棒性，并建立可验证的性能证据。  
> 非目标：不再继续扩展旧 Python 运行时；旧 Python 仅作为行为参考、兼容 fixture 和历史回归依据。

## 1. 执行摘要

当前 Go 版已经解决了 Python 单体最核心的问题：语言运行时、HTTP transport、基本 token 调度、request log、管理面和导入功能都已在 Go 中落地。但系统还没有完全达到终局状态，主要问题集中在：

- 热路径仍然偏宽，`proxy` 主流程同时处理请求体兼容、token claim、retry、stream、日志、状态提交。
- 协议兼容层过大，`responses/chat/images/compact/SSE` 多种行为集中在少数文件中。
- 导入链路存在同步导入和后台 job 两套路径，容易产生行为分叉。
- gateway/admin/worker 的代码边界和运行边界还不够清晰。
- prompt cache affinity、quota、request log、token selection 的性能和可靠性还有明确优化空间。
- 前端已经使用 COSS 风格重构过，但 Key 页面、导入页面仍然偏大，组件边界需要继续收敛。

结论：需要重构，但不是推倒当前 Go 版重写。终局方案是保留当前 Go 核心，做一次架构收敛型重构。

## 2. 调研范围

本轮调研覆盖以下路径：

- Go gateway/proxy：`internal/proxy/pipeline.go`、`internal/proxy/compat_payload.go`、`internal/proxy/prompt_cache.go`
- token 调度：`internal/tokens/snapshot.go`、`internal/tokens/selector.go`、`internal/tokens/affinity.go`
- affinity 持久化：`internal/affinity/postgres.go`
- store 层：`internal/store/tokens.go`、`internal/store/request_logs.go`、`internal/store/import_jobs.go`
- runtime/worker：`internal/runtime/gateway.go`、`internal/runtime/maintenance.go`
- admin API：`internal/httpapi/server.go`、`internal/httpapi/admin_quota.go`
- frontend：`frontend/src/features/keys/KeysPage.tsx`、`frontend/src/features/imports/ImportsPage.tsx`
- 旧 Python 行为参考：`oaix_gateway/*`
- 现有文档：`README.md`、本文件旧版本

检测到的主要技术栈：

- Go 1.24，`pgx/v5`，`redis/go-redis/v9`
- React 19，Vite 8，Tailwind 4，本地 COSS 风格组件
- PostgreSQL 为主状态存储
- 旧 Python FastAPI 代码保留为行为参考

建议保留的验证命令：

```bash
go test ./...
npm run build:web
go run ./cmd/oaix-migrate
go run ./cmd/oaix-bench
```

如需旧行为兼容回归：

```bash
uv run pytest -q
```

复杂度扫描注意事项：

- `analyze_complexity.py` 对仓库做过首轮扫描，但 `.codex-test-venv` 里的第三方依赖产生了大量噪声。
- 后续使用扫描工具时必须排除虚拟环境、构建产物和 vendor/cache 目录。

## 3. 当前主要热点与风险

### 3.1 运行边界不够清晰

位置：

- `internal/runtime/gateway.go`
- `internal/runtime/maintenance.go`
- `internal/httpapi/server.go`

当前模式：

- gateway 启动时同时组装 proxy、admin API、静态前端、token manager、log writer、embedded worker 和维护任务。
- 小规模部署方便，但热路径、管理面和后台任务共享同一进程资源。

风险：

- 导入、额度刷新、request log outbox、统计聚合可能和 `/v1/*` 转发争抢 DB pool、CPU、goroutine。
- 线上问题定位时，很难判断是热路径问题还是后台任务问题。
- 多实例部署时，哪些任务应该单实例、哪些任务可以多实例，边界不够显式。

终局改法：

- `oaix-gateway`：只承载 `/v1/*` 热路径和最小健康检查。
- `oaix-admin`：只承载管理 API 和前端静态资源。
- `oaix-worker`：只承载导入、refresh、quota refresh、log outbox、统计、清理任务。
- 本地或单容器部署可以用同一个二进制的不同 mode 启动，但代码包和职责必须拆开。

预期收益：

- 降低热路径延迟抖动。
- 线上排障更清晰。
- 后续可以单独扩 gateway 或 worker。

风险等级：高收益，中等风险。

验证方式：

- 分离前后分别压测 `/v1/responses` 并观察 p50/p95/p99、DB pool wait、log queue depth。
- worker 高负载导入时，gateway p95 TTFT 不应明显上升。

### 3.2 协议兼容层过大

位置：

- `internal/proxy/compat_payload.go`

当前模式：

- `responses`、`chat`、`images generations`、`images edits`、`compact`、`instructions`、`store=false`、`reasoning_content` 清理、SSE 兼容等逻辑集中在同一大文件。

风险：

- 过去线上多个 bug 都与请求体映射、image endpoint、SSE keepalive、failed retry、`instructions/store` 对齐有关。
- 文件过大导致新增兼容规则时容易误伤其它 endpoint。
- 旧 Python 行为没有被系统化成 golden fixture，依赖人工记忆对齐。

终局改法：

```text
internal/protocol/openai/
  responses/
  chat/
  images/
  compact/
  sse/
  fixtures/
```

每个 codec 提供：

- `NormalizeClientRequest`
- `BuildUpstreamRequest`
- `ParseUpstreamResponse`
- `BuildClientResponse`
- `ClassifyProtocolError`

必须建立 fixture：

- client request JSON/multipart
- 旧 Python 上游请求体
- Go 上游请求体
- 上游 SSE input
- 客户端 SSE output

预期收益：

- 兼容规则可测试、可审阅。
- 修 image 不会影响 responses。
- 修 stream keepalive 不会影响 non-stream。

风险等级：高收益，高风险。必须先补 fixture。

验证方式：

- 每个历史线上 bug 对应一组 fixture。
- `go test ./internal/protocol/openai/...` 必须独立覆盖。

### 3.3 Proxy pipeline 仍然过宽

位置：

- `internal/proxy/pipeline.go`

当前模式：

- `Proxy` 主流程同时负责 body 读取、payload 处理、prompt cache、token claim、retry loop、上游请求、stream/non-stream 转发、错误分类、日志和状态提交。

风险：

- 单个改动的 blast radius 大。
- retry、token release、final log、client cancel 的一致性难以局部验证。
- 后续支持新 endpoint 时容易复制逻辑。

终局改法：

拆成以下内部模块：

```text
IntentParser
PayloadPreparer
PromptCachePlanner
TokenClaimer
AttemptRunner
ResponseStreamer
FailureClassifier
TokenOutcomeCommitter
RequestLogRecorder
```

热路径统一结构：

```text
auth
-> parse intent
-> normalize payload
-> claim token
-> run upstream attempt
-> bridge response
-> classify outcome
-> commit token state
-> write final log
```

预期收益：

- endpoint 逻辑只关心 codec。
- 状态提交和日志写入可以单测。
- retry/failover 行为可复用。

风险等级：中高收益，中等风险。

验证方式：

- 现有 `/v1/responses`、`/v1/chat/completions`、`/v1/images/*` 黑盒测试全部通过。
- client cancel 后 `active_streams=0`。
- final log loss count 为 0。

### 3.4 导入链路存在双路径

位置：

- `internal/httpapi/server.go`
- `internal/runtime/maintenance.go`
- `internal/importer/worker.go`
- `internal/store/import_jobs.go`

当前模式：

- Admin 同步导入可直接 prepare/refresh/publish。
- import job/worker 也有自己的 validation/publish 路径。

风险：

- `account_id,refresh_token`、纯 `refresh_token`、OAuth callback、批量导入可能走到不同路径。
- 同步导入请求可能长时间占用 admin handler。
- 导入错误容易出现前端状态、job 状态、token 状态不一致。

终局改法：

- 所有导入都创建 `token_import_jobs`。
- Admin API 只做解析、校验、落 staging item。
- Worker 负责 refresh、校验、publish、quota 初始化、错误归档。
- 小批量导入也走同一状态机，只是 job 很快完成。

预期收益：

- 导入行为唯一。
- 前端批次页、job 状态、token 状态天然一致。
- worker 可独立扩容和重试。

风险等级：高收益，中高风险。

验证方式：

- 同一组输入通过粘贴、文件、OAuth 三种入口导入，最终 job/item/token 状态一致。
- worker 重启后 job 可恢复。
- 重复 refresh token 不产生脏数据。

### 3.5 Token snapshot 与 selector 未来会遇到规模瓶颈

位置：

- `internal/tokens/snapshot.go`
- `internal/tokens/selector.go`

当前模式：

- snapshot refresh 会排序并构建若干索引。
- selector 对 ready tokens 做线性扫描。
- 当前线上可用 key 数量小，这不是当前主要瓶颈。

风险：

- 如果数千个 token 同时可用，refresh 和 select 的 O(n) / O(n log n) 成本会变明显。
- `Stats` 之类全量 ready tokens 汇总会放大高频读取成本。

终局改法：

- snapshot 构建时预生成按 `status/plan/model/scope/capacity` 的候选队列。
- selector 从局部候选集取值，避免每次扫描全体。
- active streams 聚合计数在 claim/release 时维护，不在查询时循环汇总。
- 对 fill-first、quota-aware、latency-aware 建独立 selector index。

预期收益：

- 10k token 下 claim 延迟稳定。
- snapshot refresh 成本可控。

风险等级：中等收益，中等风险。当前不是第一优先级。

验证方式：

- 1k、10k、50k token benchmark。
- 指标包括 claim p50/p95/p99、snapshot refresh duration、allocs/op。

### 3.6 Prompt cache affinity 需要更强的工程化

位置：

- `internal/tokens/affinity.go`
- `internal/affinity/postgres.go`

当前模式：

- 新 lane 创建可能扫描并排序 ready tokens。
- Postgres LaneStore 删除 token 时会扫描未过期 lanes。
- 部分 affinity 持久化错误被 fail-open 处理。

风险：

- prompt cache key 高基数时，lane 创建成本上升。
- token 被禁用/删除时，lane 清理可能变慢。
- fail-open 可以接受，但没有指标会导致线上缓存率下降时难以定位。

终局改法：

- gateway 本地 LRU cache + Postgres/Redis 持久化。
- `prompt_affinity_lanes` 增加反向索引或独立 `prompt_affinity_token_index`。
- lane 候选用 rendezvous top-k，不对全量 ready token 排序。
- 所有 fail-open 都打 metrics 和 structured log。

预期收益：

- prompt cache 命中率稳定。
- lane 清理成本下降。
- 缓存率异常可追踪。

风险等级：中高收益，中等风险。

验证方式：

- 构造 10k prompt cache keys、10k tokens 的 affinity benchmark。
- 线上监控 cache hit rate、lane lookup latency、affinity fail-open count。

### 3.7 Request log 与统计仍可提升

位置：

- `internal/logs/writer.go`
- `internal/store/request_logs.go`

当前模式：

- 已有内存队列、batch upsert、outbox fallback。
- 但 start/final log 的可靠性等级、统计聚合、outbox drain、批量 upsert 还可以进一步明确。

风险：

- DB 抖动时 final log 同步 fallback 可能影响热路径。
- 大量日志写入和统计查询可能争抢同一表。
- 管理面统计如果扫明细表，长期会变慢。

终局改法：

- final log durable 优先，必要时落 outbox，不静默丢。
- request logs 按时间分区。
- 统计由 worker 异步聚合到 hourly/materialized 表。
- batch upsert 优先使用 `COPY` staging 或 `unnest`。

预期收益：

- 审计和计费相关日志可靠。
- Admin 统计查询稳定。

风险等级：中高收益，中等风险。

验证方式：

- DB 延迟/不可用故障注入。
- final log loss count 必须为 0。
- outbox backlog 可恢复到 0。

### 3.8 Admin quota 状态应该持久化

位置：

- `internal/httpapi/admin_quota.go`

当前模式：

- Admin token list 可以触发 quota 查询。
- quota cache 主要是进程内 cache/pending。
- 额度错误可能影响 UI 状态判断。

风险：

- 前端打开页面才刷新额度，状态短暂不一致。
- 多实例下 cache 不共享。
- quota 错误分类如果只存在内存里，无法审计。

终局改法：

- Worker 周期刷新 quota snapshot。
- DB 持久化 `quota_5h_remaining`、`quota_7d_remaining`、`quota_error_code`、`quota_checked_at`、`quota_state`。
- `deactivated_workspace` 等明确错误由 worker 统一转为禁用或需要人工处理状态。
- Admin UI 只读取快照，不在列表请求里做大量实时外部请求。

预期收益：

- Key 页面稳定。
- quota 状态可审计。
- 外部额度 API 抖动不会直接拖慢 Admin 页面。

风险等级：中等收益，中等风险。

验证方式：

- fake quota server 覆盖 200、401、402、429、5xx、timeout。
- 状态转换和前端展示一致。

### 3.9 前端页面需要继续模块化

位置：

- `frontend/src/features/keys/KeysPage.tsx`
- `frontend/src/features/imports/ImportsPage.tsx`

当前模式：

- 已经改成 COSS 风格，但页面文件仍接近 900 行。
- 列表、筛选、批量操作、导入弹窗、批次详情 modal、状态管理还混在页面中。

风险：

- UI 修复容易互相影响。
- 筛选状态、loading 状态、URL query 状态容易出现竞态。
- 后续新增列或批量操作成本高。

终局改法：

```text
frontend/src/features/keys/
  api.ts
  hooks/useTokenQuery.ts
  hooks/useTokenSelection.ts
  components/TokenFilters.tsx
  components/TokenTable.tsx
  components/QuotaMeter.tsx
  components/BulkToolbar.tsx
  components/RemarkDialog.tsx
  components/ImportTokenModal.tsx

frontend/src/features/imports/
  api.ts
  hooks/useImportJobs.ts
  components/ImportJobsTable.tsx
  components/ImportJobKeysModal.tsx
  components/ImportJobFilters.tsx
  components/DeleteImportJobDialog.tsx
```

预期收益：

- 每个交互点可单独测试。
- COSS 风格更一致。
- 修窄屏、表格、modal、筛选时互不影响。

风险等级：中等收益，低中风险。

验证方式：

- `npm run build:web`
- in-app Browser 检查桌面、窄屏、modal、筛选、导入、批量操作。

## 4. 终局架构

### 4.1 目标二进制

```text
cmd/oaix-gateway
  只负责热路径：
  - /v1/responses
  - /v1/chat/completions
  - /v1/images/generations
  - /v1/images/edits
  - minimal health/readiness

cmd/oaix-admin
  只负责管理面：
  - token list/detail/batch operations
  - import jobs
  - request logs
  - settings
  - quota snapshots
  - static frontend

cmd/oaix-worker
  只负责后台任务：
  - import job validation/publish
  - OAuth refresh
  - quota refresh
  - request log outbox drain
  - hourly stats aggregation
  - retention/cleanup
  - repair/reconcile jobs

cmd/oaix-migrate
  只负责 schema migration。

cmd/oaix-bench
  只负责压测和回归验证。
```

### 4.2 目标包结构

```text
internal/adminapi
internal/affinity
internal/config
internal/importer
internal/logs
internal/observability
internal/protocol/openai
internal/protocol/openai/responses
internal/protocol/openai/chat
internal/protocol/openai/images
internal/protocol/sse
internal/proxy
internal/quota
internal/runtime
internal/store
internal/tokens
internal/transport
internal/worker
```

### 4.3 请求热路径

```text
client request
-> auth
-> parse request intent
-> normalize payload with endpoint codec
-> prompt cache planning
-> token claim
-> upstream attempt
-> response bridge
-> classify outcome
-> token state commit
-> final log durable write
```

热路径规则：

- 不做 schema migration。
- 不做导入。
- 不做 quota 实时刷新。
- 不做管理统计聚合。
- 不阻塞等待非关键后台任务。
- 所有可降级任务必须有 metrics。

### 4.4 状态可靠性原则

- 进程内状态只能作为 cache。
- token disabled/cooling 必须写 DB。
- final request log 必须 durable。
- import job 状态必须可恢复。
- prompt affinity 可以 fail-open，但必须有指标。
- quota snapshot 应持久化，不应只在前端请求中临时存在。

## 5. 数据库终局设计

### 5.1 表职责

建议保留或补齐以下职责边界：

```text
tokens
token_secrets
token_runtime_state
token_state_events
token_refresh_history

token_quota_snapshots

request_logs
request_log_outbox
request_hourly_stats
token_hourly_stats
model_hourly_stats

prompt_affinity_lanes
prompt_affinity_token_index
response_owner_bindings

token_import_jobs
token_import_items

admin_audit_logs
settings
schema_migrations
```

### 5.2 SQL 性能原则

- Token list：服务端分页，必要时 cursor pagination。
- Token search：为 email/account_id/remark/source/error 做合适索引，必要时 `pg_trgm`。
- Plan/status counts：从聚合或轻量索引读，不在高频请求里做复杂扫描。
- Request logs：按时间分区或至少按时间索引。
- Request stats：读 hourly stats，不直接扫明细。
- Import publish：使用批量 upsert，不逐 item round-trip。
- Quota snapshot：worker 批量刷新并批量写入。

## 6. 可观测性与性能证据

任何“性能优化完成”必须满足以下证据要求之一：

- 有 before/after benchmark。
- 有线上或本地压测 p50/p95/p99 对比。
- 有 `pg_stat_statements` 或 DB query timing 证明。
- 有 Prometheus/metrics 指标证明。
- 有浏览器 performance/profile 证明。

禁止用“理论上更快”作为完成依据。

### 6.1 必须补齐的指标

Gateway：

- `oaix_gateway_requests_total`
- `oaix_gateway_request_duration_seconds`
- `oaix_gateway_ttft_seconds`
- `oaix_gateway_active_streams`
- `oaix_gateway_upstream_attempts_total`
- `oaix_gateway_upstream_errors_total`
- `oaix_gateway_client_canceled_total`

Token：

- `oaix_token_claim_duration_seconds`
- `oaix_token_claim_failures_total`
- `oaix_token_snapshot_refresh_duration_seconds`
- `oaix_token_snapshot_age_seconds`
- `oaix_token_ready_tokens`
- `oaix_token_active_streams`
- `oaix_token_state_commit_failures_total`

Affinity：

- `oaix_affinity_lookup_duration_seconds`
- `oaix_affinity_hit_total`
- `oaix_affinity_miss_total`
- `oaix_affinity_fail_open_total`
- `oaix_affinity_lane_create_total`

Logs：

- `oaix_request_log_queue_depth`
- `oaix_request_log_written_total`
- `oaix_request_log_dropped_total`
- `oaix_request_log_outbox_writes_total`
- `oaix_request_log_outbox_backlog`
- `oaix_request_log_batch_duration_seconds`

Worker：

- `oaix_worker_jobs_claimed_total`
- `oaix_worker_jobs_failed_total`
- `oaix_worker_job_duration_seconds`
- `oaix_import_items_published_total`
- `oaix_import_items_failed_total`
- `oaix_quota_refresh_duration_seconds`
- `oaix_quota_refresh_errors_total`

### 6.2 必须保留的压测场景

- 1k tokens claim benchmark。
- 10k tokens claim benchmark。
- 50k tokens snapshot refresh benchmark。
- 100 concurrent streams。
- 1k concurrent streams。
- 10k short non-stream requests。
- 上游 429/401/403/5xx 混合错误。
- 上游 SSE 半包、慢 body、中途断流。
- 客户端中断。
- DB 延迟和短暂不可用。
- Worker 高负载导入同时 gateway 转发。

## 7. 测试策略

### 7.1 协议兼容测试

每个 endpoint 都需要 fixture：

- `/v1/responses`
- `/v1/responses/compact`
- `/v1/chat/completions`
- `/v1/images/generations`
- `/v1/images/edits`

必须覆盖：

- `instructions` 缺失、空字符串、非空字符串。
- `store` 缺失、true、false。
- `reasoning_content` 在不同层级出现。
- `gpt-image-2` 请求体转换。
- multipart image edit。
- response.created keepalive。
- response.failed 自动重试。
- 上游 terminal error frame。
- non-stream 转 stream 的兼容行为。

### 7.2 状态机测试

必须覆盖：

- active -> cooling。
- active -> disabled。
- cooling -> active。
- disabled -> active by admin。
- deactivated_workspace -> disabled。
- quota_error -> visible but not falsely active。
- state commit failure。
- client cancel release claim。

### 7.3 导入测试

必须覆盖：

- 粘贴 `account_id,refresh_token`。
- 粘贴纯 `refresh_token`。
- 文件导入。
- OAuth callback 导入。
- 重复 refresh token。
- job cancel。
- job delete with tokens。
- worker crash/resume。
- partial success。
- OAuth refresh 429/401/5xx/timeout。

### 7.4 前端测试和手工验收

必须覆盖：

- Key 页面默认有效状态。
- 状态筛选、计划筛选、排序、搜索。
- 筛选切换立刻显示 loading。
- 不同状态下 key 都能显示。
- 额度胶囊对齐。
- 备注完整展示。
- 窄屏表格行高正常。
- 导入 modal tabs。
- 导入批次表格。
- 批次详情 modal 状态/计划筛选。
- 批次删除确认。
- 长 refresh token 输入不会撑破布局。

## 8. 详细 TODO List

### 8.1 立项与基线

- [ ] 确认本文件作为后续 oaix Go 版重构唯一实施方案。
- [ ] 冻结当前线上版本 commit。
- [ ] 记录当前 Fugue 部署版本、构建号和启动时间。
- [ ] 导出当前线上关键环境变量的 sanitized summary。
- [ ] 记录当前线上 token 总数、有效数、冷却数、禁用数。
- [ ] 记录当前线上 1 小时 request volume、错误率、缓存率。
- [ ] 记录当前线上 request log outbox backlog。
- [ ] 记录当前线上 DB pool 配置和连接数。
- [ ] 记录当前线上 gateway CPU/memory 基线。
- [ ] 建立重构分支。
- [ ] 建立 rollback checklist。

### 8.2 观测与性能证据

- [ ] 补齐 gateway request duration 指标。
- [ ] 补齐 TTFT 指标。
- [ ] 补齐 token claim duration 指标。
- [ ] 补齐 token snapshot refresh duration 指标。
- [ ] 补齐 prompt affinity hit/miss/fail-open 指标。
- [ ] 补齐 request log queue/outbox 指标。
- [ ] 补齐 worker job duration 指标。
- [ ] 补齐 quota refresh 指标。
- [ ] 增加 pprof 或等价 profiling 入口，并限制 admin 权限。
- [ ] 建立 `cmd/oaix-bench` 标准压测脚本。
- [ ] 建立 100 并发 stream benchmark。
- [ ] 建立 1k 并发 stream benchmark。
- [ ] 建立 10k token selector benchmark。
- [ ] 建立 50k token snapshot benchmark。
- [ ] 建立 DB 故障注入 benchmark。
- [ ] 建立上游错误注入 benchmark。
- [ ] 建立前端 Lighthouse 或 browser smoke 脚本。
- [ ] 写入 before/after 性能记录模板。

### 8.3 协议兼容层拆分

- [ ] 新建 `internal/protocol/openai` 根包。
- [ ] 新建 responses codec 包。
- [ ] 新建 chat codec 包。
- [ ] 新建 images codec 包。
- [ ] 新建 compact codec 包。
- [ ] 新建 SSE protocol helper 包。
- [ ] 从 `compat_payload.go` 抽出 `/v1/responses` normalize 逻辑。
- [ ] 从 `compat_payload.go` 抽出 `/v1/chat/completions` normalize 逻辑。
- [ ] 从 `compat_payload.go` 抽出 `/v1/images/generations` normalize 逻辑。
- [ ] 从 `compat_payload.go` 抽出 `/v1/images/edits` multipart normalize 逻辑。
- [ ] 抽出 `instructions` 默认注入逻辑。
- [ ] 抽出 `store=false` 强制逻辑。
- [ ] 抽出 `reasoning_content` 递归删除逻辑。
- [ ] 抽出 `response.created` keepalive 兼容逻辑。
- [ ] 抽出 `response.failed` 自动 retry 判断逻辑。
- [ ] 为 gpt-image-2 generation 增加 golden fixture。
- [ ] 为 gpt-image-2 edit 增加 golden fixture。
- [ ] 为 missing instructions 增加 golden fixture。
- [ ] 为 store true -> false 增加 golden fixture。
- [ ] 为 reasoning_content 删除增加 golden fixture。
- [ ] 为 response.created/failed SSE 增加 golden fixture。
- [ ] 确认 `compat_payload.go` 缩小到 orchestration 或删除。
- [ ] 跑通 `go test ./internal/protocol/openai/...`。

### 8.4 Proxy pipeline 拆分

- [ ] 定义 `RequestIntent` 独立类型和测试。
- [ ] 定义 `PreparedPayload` 独立类型和测试。
- [ ] 定义 `TokenClaim` 独立类型和 release contract。
- [ ] 定义 `UpstreamAttempt` 独立类型。
- [ ] 定义 `ProxyOutcome` 独立类型。
- [ ] 定义 `FailureClassifier` 接口。
- [ ] 定义 `RetryPolicy` 接口。
- [ ] 抽出 auth/normalize 阶段。
- [ ] 抽出 prompt cache planning 阶段。
- [ ] 抽出 token claim 阶段。
- [ ] 抽出 upstream request build 阶段。
- [ ] 抽出 stream bridge 阶段。
- [ ] 抽出 non-stream bridge 阶段。
- [ ] 抽出 outcome classification 阶段。
- [ ] 抽出 token state commit 阶段。
- [ ] 抽出 request final log 阶段。
- [ ] 为 client cancel 增加 release guarantee 测试。
- [ ] 为 panic/recover 增加 release guarantee 测试。
- [ ] 为 retry exhausted 增加 final log 测试。
- [ ] 为 upstream 429 cooldown 增加状态提交测试。
- [ ] 为 upstream 401/403 disabled 增加状态提交测试。
- [ ] 确认 `pipeline.go` 单文件行数降到可维护范围。

### 8.5 Gateway/Admin/Worker 边界

- [ ] 定义 gateway mode 只注册 `/v1/*` 和 minimal health。
- [ ] 定义 admin mode 只注册 `/admin/*` 和 frontend。
- [ ] 定义 worker mode 只运行后台任务。
- [ ] 将 `internal/httpapi` 拆成 public proxy API 和 admin API。
- [ ] 将 embedded worker 默认关闭，单容器部署显式开启。
- [ ] 把 worker 任务从 gateway runtime 依赖中抽离。
- [ ] 为 gateway 增加 worker disabled 情况下的启动测试。
- [ ] 为 admin 增加 proxy disabled 情况下的启动测试。
- [ ] 为 worker 增加 no HTTP server 情况下的启动测试。
- [ ] 更新 Dockerfile/entrypoint 支持 mode。
- [ ] 更新 Fugue 部署配置说明。
- [ ] 验证 worker 高负载时 gateway benchmark 不回退。

### 8.6 导入链路统一

- [ ] 定义唯一 import job 状态机。
- [ ] 定义唯一 import item 状态机。
- [ ] 粘贴导入改为创建 job。
- [ ] 文件导入改为创建 job。
- [ ] OAuth callback 导入改为创建 job。
- [ ] 删除 Admin 同步 publish 路径。
- [ ] Worker 实现 refresh token validation。
- [ ] Worker 实现 access token validation。
- [ ] Worker 实现 quota 初始化。
- [ ] Worker 实现批量 item update。
- [ ] Worker 实现批量 token publish。
- [ ] Worker 实现重复 refresh token 合并。
- [ ] Worker 实现 partial success。
- [ ] Worker 实现 crash/resume。
- [ ] Worker 实现 job delete with tokens。
- [ ] Admin API 返回 job id 和 progress。
- [ ] 前端导入 modal 轮询 job progress。
- [ ] 导入批次详情 modal 使用 job item/token 统一数据源。
- [ ] 覆盖 `account_id,refresh_token` 导入测试。
- [ ] 覆盖纯 `refresh_token` 导入测试。
- [ ] 覆盖 OAuth 导入测试。
- [ ] 覆盖批次删除测试。

### 8.7 Token selector 与 snapshot

- [ ] 明确当前 selector 策略优先级。
- [ ] 为 snapshot refresh 增加 duration 和 token count 指标。
- [ ] 为 selector claim 增加 duration 和 candidate count 指标。
- [ ] snapshot 增加按 status/plan/model/scope 的候选索引。
- [ ] snapshot 增加 active aggregate 快照。
- [ ] selector 使用局部候选集，避免全量 ready scan。
- [ ] fill-first selector 使用稳定 priority queue 或 ring。
- [ ] round-robin selector 使用按 scope/model 的 ring。
- [ ] quota-aware selector 只在 quota-ready 候选中选择。
- [ ] latency-aware selector 使用局部 top-k。
- [ ] prompt-affinity fallback 不全量排序。
- [ ] 支持 excluded token set O(1) 判断。
- [ ] 1k token claim benchmark 通过。
- [ ] 10k token claim benchmark 通过。
- [ ] 50k token snapshot benchmark 通过。
- [ ] 验证 selector 行为和当前线上策略一致。

### 8.8 Prompt cache affinity

- [ ] 梳理当前 prompt cache key 生成逻辑。
- [ ] 为 prompt cache key 增加 fixture。
- [ ] 为 response owner binding 增加测试。
- [ ] 为 lane primary/secondary selection 增加测试。
- [ ] 增加本地 LRU lane cache。
- [ ] 增加 affinity fail-open metrics。
- [ ] 增加 affinity lookup latency metrics。
- [ ] 增加 `prompt_affinity_token_index` 或等价反向索引。
- [ ] 改造 RemoveToken，避免扫描所有 lane。
- [ ] lane 创建使用 rendezvous top-k。
- [ ] disabled/cooling token 从 lane 中快速剔除。
- [ ] token 删除时清理 lane index。
- [ ] 多实例场景下验证 lane 共享。
- [ ] 缓存率 benchmark 对比改造前后。
- [ ] 线上观测 cache hit rate 无回退。

### 8.9 Request log 与统计

- [ ] 明确 start log 可降级策略。
- [ ] 明确 final log 零丢失策略。
- [ ] 为 final log 写入失败增加 outbox fallback 测试。
- [ ] 为 queue full 增加 metrics 和测试。
- [ ] 优化 batch upsert，评估 `COPY` staging 或 `unnest`。
- [ ] request log 表增加时间分区方案。
- [ ] worker 实现 request hourly stats 聚合。
- [ ] worker 实现 token hourly stats 聚合。
- [ ] worker 实现 model hourly stats 聚合。
- [ ] Admin 统计 API 优先读聚合表。
- [ ] request log retention 改为按分区或批量 limit 清理。
- [ ] DB unavailable 注入下 final log 不丢。
- [ ] outbox backlog 恢复到 0 的测试通过。
- [ ] 压测下 `request_log_dropped_total=0`。

### 8.10 Quota 与计划状态

- [ ] 从 Admin API 中抽出 quota service 到 `internal/quota`。
- [ ] 定义 quota snapshot DB model。
- [ ] Worker 周期刷新 quota snapshot。
- [ ] Worker 批量写 quota snapshot。
- [ ] Admin token list 默认读取 quota snapshot。
- [ ] 保留手动刷新 quota 按钮或 API。
- [ ] `deactivated_workspace` 统一转换为 disabled 或明确不可用状态。
- [ ] quota 402/401/429/5xx/timeout 分类明确。
- [ ] quota 错误不应显示为有效额度。
- [ ] plan/type 更新只由统一 quota/OAuth path 写入。
- [ ] 计划筛选 counts 与状态筛选关系明确。
- [ ] 为 plan count 增加独立测试。
- [ ] 为 quota error 展示增加前端测试。

### 8.11 Store 层批处理与索引

- [ ] 审查 `ListTokens` 查询计划。
- [ ] 审查 token counts 查询计划。
- [ ] 审查 plan counts 查询计划。
- [ ] 审查 request logs 查询计划。
- [ ] 审查 import jobs 查询计划。
- [ ] 为 token search 增加必要索引。
- [ ] 为 status/plan/order 增加组合索引。
- [ ] 为 request logs time/model/status 增加索引或分区。
- [ ] 为 import job/item status 增加索引。
- [ ] `UpsertTokenPayloads` 改为 staging/batch upsert。
- [ ] token secrets/history 写入批量化。
- [ ] token last_used_at 批量更新。
- [ ] request log outbox drain 批量化。
- [ ] 使用 `pg_stat_statements` 记录 before/after。

### 8.12 Admin API

- [ ] 将 admin token API 从 `server.go` 拆出。
- [ ] 将 import job API 从 `server.go` 拆出。
- [ ] 将 request log API 从 `server.go` 拆出。
- [ ] 将 settings API 从 `server.go` 拆出。
- [ ] 将 OAuth API 从 `server.go` 拆出。
- [ ] 定义统一 API error envelope。
- [ ] 所有列表 API 明确分页语义。
- [ ] 批量操作返回 job 或 operation id。
- [ ] 管理写操作写 audit log。
- [ ] 增加 admin API integration tests。
- [ ] `server.go` 缩小到路由装配和基础 middleware。

### 8.13 Frontend COSS 管理台

- [ ] 拆分 Key 页面 API client。
- [ ] 拆分 `useTokenQuery`。
- [ ] 拆分 `useTokenSelection`。
- [ ] 拆分 `TokenFilters`。
- [ ] 拆分 `TokenTable`。
- [ ] 拆分 `QuotaMeter`。
- [ ] 拆分 `BulkToolbar`。
- [ ] 拆分 `RemarkDialog`。
- [ ] 拆分 `ImportTokenModal`。
- [ ] 拆分导入页面 API client。
- [ ] 拆分 `useImportJobs`。
- [ ] 拆分 `ImportJobsTable`。
- [ ] 拆分 `ImportJobKeysModal`。
- [ ] 拆分 `ImportJobFilters`。
- [ ] 拆分 `DeleteImportJobDialog`。
- [ ] 所有输入框长字符串不撑破布局。
- [ ] 所有表格窄屏行高受控。
- [ ] 备注列完整展示并不破坏表格。
- [ ] 表格行最多显示两行内容的列保持一致。
- [ ] 状态筛选切换立即进入 loading。
- [ ] 计划筛选 counts 逻辑符合产品定义。
- [ ] 批次详情 modal 支持状态筛选。
- [ ] 批次详情 modal 支持计划筛选。
- [ ] 导入 modal 使用粘贴/文件/OAuth tabs。
- [ ] 删除无意义说明卡片。
- [ ] `npm run build:web` 通过。
- [ ] in-app Browser 桌面宽屏验收。
- [ ] in-app Browser 窄屏验收。

### 8.14 OAuth 导入

- [ ] 梳理 `sub2api` OAuth 授权流程。
- [ ] 明确 oaix OAuth callback URL 生成规则。
- [ ] 明确 public host 强制 HTTPS 规则。
- [ ] OAuth start API 创建 pending job 或 pending session。
- [ ] OAuth callback 将 refresh token 写入 import job。
- [ ] OAuth state 防 CSRF。
- [ ] OAuth callback 错误可展示给前端。
- [ ] OAuth 导入成功后跳转导入批次详情。
- [ ] OAuth 导入失败后保留 job 错误。
- [ ] OAuth refresh token 不在前端暴露。
- [ ] OAuth 相关 secret 不进入日志。
- [ ] OAuth 流程 integration test 通过。

### 8.15 部署与回滚

- [ ] 更新 Dockerfile 支持 gateway/admin/worker mode。
- [ ] 更新 Fugue 构建说明。
- [ ] 更新环境变量文档。
- [ ] 定义 migration 执行顺序。
- [ ] 定义 worker 单实例或多实例策略。
- [ ] 定义 gateway readiness degraded 条件。
- [ ] 定义 admin readiness 条件。
- [ ] 定义 rollback 触发阈值。
- [ ] 上线前备份关键表。
- [ ] 上线后监控 30 分钟关键指标。
- [ ] 确认缓存率无回退。
- [ ] 确认 error rate 无回退。
- [ ] 确认 request final log 不丢。
- [ ] 确认 worker backlog 不持续增长。

### 8.16 文档与维护

- [ ] 更新 README 中过期的 refresh worker 描述。
- [ ] 更新 architecture 文档。
- [ ] 更新 admin API 文档。
- [ ] 更新 import job 状态机文档。
- [ ] 更新 quota state 文档。
- [ ] 更新 prompt affinity 文档。
- [ ] 更新 benchmark 使用说明。
- [ ] 更新线上排障手册。
- [ ] 记录旧 Python 行为 fixture 来源。
- [ ] 标记旧 Python 运行时为 legacy/reference。

## 9. 完成定义

本次重构不能只以“代码写完”为完成。必须同时满足：

- Go 测试通过。
- 前端构建通过。
- 协议兼容 fixture 通过。
- 核心压测通过。
- 有 before/after 性能证据。
- 线上关键指标无回退。
- 缓存率无异常下降。
- request final log 无丢失。
- token active streams 无泄漏。
- worker backlog 可恢复。
- Admin 前端关键路径可用。
- 文档和 TODO 勾选状态同步更新。

## 10. 建议执行顺序

建议按以下顺序实施，避免一开始就动最大风险模块：

1. 观测与 benchmark 先行。
2. 协议兼容 fixture 先行。
3. 拆 protocol codec。
4. 拆 proxy pipeline。
5. 统一 import job。
6. 持久化 quota snapshot。
7. 拆 gateway/admin/worker 边界。
8. 优化 token selector 和 affinity。
9. 优化 request log 和统计。
10. 前端继续模块化。
11. 部署、压测、线上观察、逐项勾选。

每个阶段都必须保持线上业务可用，并且不得依赖猜测判断性能是否提升。
