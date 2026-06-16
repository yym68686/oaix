# oaix 终局 Go 重构方案

> 状态：待审阅  
> 范围：仅 oaix 项目  
> 目标：用 Go 重写 oaix 的核心网关、账号池调度、请求代理、日志写入、后台任务和管理面，形成可高并发横向扩展的终局架构。  
> 非目标：不做 Python 单体的渐进式整理，不把现有 `api_server.py` 拆小后继续承载长期演进。

## 1. 背景与定位

根据业务系统说明，生产链路为：

```text
Client
-> Fugue edge
-> uni-api-web-api
-> uni-api-ember
-> oaix
-> upstream model providers
```

oaix 在链路中的职责不是普通 API 转发，而是下游账号池网关：

- 维护 OpenAI/Codex-like token 池。
- 选择可用 token，并处理并发、冷却、失效和 quota。
- 将请求代理到上游模型服务。
- 支持 `/v1/responses`、chat completions、image generation/edit 等兼容接口。
- 支持 refresh token 换 access token。
- 记录请求日志、token 使用情况和失败原因。
- 提供 token 管理、导入、日志查询等管理面。

因此 oaix 的核心质量目标是：

- 高并发流式转发稳定。
- token 调度延迟稳定，不随 token 池规模线性劣化。
- 上游失败分类准确，冷却/失效状态可靠落地。
- 日志和状态写入可追溯，不依赖进程内 best-effort 队列。
- 支持多实例横向扩展。
- 管理面和热路径隔离，避免后台任务影响代理请求。

## 2. 当前项目调研结论

当前实现是 Python FastAPI 单体，主要代码集中在：

- `oaix_gateway/api_server.py`：约 13000 行，包含 API 路由、代理、流式处理、token 选择、日志队列、后台任务、管理接口。
- `oaix_gateway/token_store.py`：token 存储、状态写入、claim fallback、去重。
- `oaix_gateway/request_store.py`：请求日志 upsert、统计聚合、清理。
- `oaix_gateway/database.py`：SQLAlchemy engine、启动期建表/迁移/索引/backfill。
- `oaix_gateway/token_import_jobs.py`：token 导入、校验、OAuth refresh、publish。
- `oaix_gateway/web/app.js`：管理前端单文件。

测试基线：

```text
uv run pytest -q
354 passed, 1 skipped
```

测试较多直接 import 和 monkeypatch `oaix_gateway.api_server` 私有函数，这说明当前测试可以作为行为规格来源，但也说明 Python 内部结构已经被测试强绑定，不适合作为长期架构边界。

## 3. 是否需要彻底重构

结论：需要。

原因不是单纯代码行数大，而是当前结构已经把以下职责混在同一个运行时和大量长函数中：

- HTTP 路由。
- 上游代理。
- SSE 流式解析和生成。
- token 调度。
- prompt cache affinity。
- OAuth refresh。
- 429/401/403/5xx 错误分类。
- request log start/final 合并。
- token 状态异步写入。
- admin API。
- import worker。
- schema migration。
- observability 旁路。

这种耦合会带来四类长期问题：

1. 热路径无法稳定优化。每次功能变更都可能改到代理、日志、状态、错误分类的组合逻辑。
2. 运行时不适配。当前已经出现针对 httpx/httpcore 私有结构和 `/proc/self/net/tcp` 的连接清理逻辑，说明 Python/httpx 层面对本项目的长连接高并发流式场景已不理想。
3. 可靠性边界不清晰。关键状态和日志有多处进程内队列，崩溃或队列满时存在丢失。
4. 横向扩展不足。prompt affinity、runtime cooldown、recent TTFT、active request 等状态多为单进程内存态，多实例后一致性边界需要重建。

## 4. 性能瓶颈清单

| 模块 | 现状证据 | 风险 | Go 终局处理 |
| --- | --- | --- | --- |
| responses 流式代理 | `api_server.py:_responses_stream_keepalive_response` 约 962 行 | token claim、OAuth、SSE、日志、错误恢复耦合；难以保证取消和回滚一致 | 建统一 stream pipeline，SSE parser/writer 独立，状态提交独立 |
| 通用 failover | `api_server.py:_execute_proxy_request_with_failover` 约 783 行 | 与 responses/image 逻辑重复，失败分类分散 | 建 `FailureClassifier` 和 `RetryPolicy`，所有路由共用 |
| image stream | `api_server.py:_gpt_image_stream_keepalive_response` 约 641 行 | 与 responses stream 重复大量 keepalive/worker/terminal frame 逻辑 | image 只保留协议转换，代理内核共用 |
| HTTP 连接池 | `api_server.py:_sweep_httpx_client_idle_connections` 触达 httpcore 私有结构 | 运行时补丁脆弱，升级 httpx/httpcore 风险高 | 使用 Go `net/http.Transport`，按上游 host/shard 明确配置 |
| token snapshot lookup | prompt affinity 中按 token id 线性扫描 snapshot | token 池变大后调度延迟上升 | snapshot 内建 `map[tokenID]*Token`，claim 只做 O(1)/局部排序 |
| prompt cache lane | 进程内 `OrderedDict`，lane candidate 可能全量扫描排序 | 多实例不一致；新 lane 请求成本高 | 本地 cache + Redis/Postgres TTL 后端，lane index 分片 |
| request log writer | 进程内 queue，队列满会 drop start/final 任务 | 审计和计费相关日志可能丢失 | durable outbox + batch writer；重要 final log 不静默丢 |
| request log stats | upsert 中混合日志写入和 hourly stats | 写路径承担统计副作用，失败处理复杂 | 日志主写入和统计聚合解耦，worker 异步聚合 |
| token last_used_at | 每 token 单独 UPDATE | 高并发下 DB round-trip 放大 | `UPDATE ... FROM (VALUES ...)` 批量更新 |
| token status write | token 状态写入进程内队列 | 崩溃丢 pending 状态；多实例同步弱 | 状态机 + durable event/outbox；关键状态同步提交 |
| schema migration | gateway 启动期 create_all/migration/index/backfill | 多实例启动竞争；启动抖动；DDL 风险 | 独立 `oaix-migrate`，runtime 禁止 DDL |
| import worker | 每 item 多次 DB 状态更新，OAuth 并发和 publish 混合 | 后台任务 DB 写放大，可能影响热路径 | worker 独立进程，批量 claim/update/publish |
| admin frontend | `web/app.js` 单文件 5000+ 行，多处全量 map/render | 管理面演进成本高，大列表性能差 | 前端模块化，列表虚拟化，管理 API 分页/增量 |

## 5. 终局架构原则

1. 热路径极简。
   - 代理请求路径只做必要的认证、路由、token claim、上游请求、流式转发、最终状态提交。
   - 统计、聚合、导入、修复、清理全部从热路径移出。

2. 状态边界显式。
   - 进程内状态只能作为 cache。
   - 冷却、禁用、quota、重要 request log 必须有可靠落点。

3. 协议处理统一。
   - chat、responses、image 的上游代理共用同一套 transport、retry、failure classification、SSE 基础设施。
   - 差异仅保留在 request/response codec 层。

4. 多实例优先。
   - 任意 gateway 实例退出，不应丢关键 token 状态。
   - 任意 gateway 实例新增，不应需要全局锁或长启动迁移。

5. 配置收敛。
   - 兼容读取现有环境变量，但内部转换为结构化配置。
   - 默认值按生产高并发场景设计，不用散落常量。

6. 可观测性内建。
   - 每个 request id、token id、upstream attempt、cooldown decision 都可追踪。
   - 指标覆盖队列深度、stream 时长、TTFT、错误分类、DB batch 延迟。

## 6. Go 目标组件

### 6.1 二进制

```text
cmd/oaix-gateway
  热路径服务。
  提供 OpenAI-compatible API、token claim、upstream proxy、SSE streaming。

cmd/oaix-admin
  管理 API。
  可独立部署，也可在小规模环境与 gateway 同进程启动，但代码边界独立。

cmd/oaix-worker
  后台任务。
  负责 OAuth refresh、token import、request log aggregation、retention、outbox drain。

cmd/oaix-migrate
  数据库迁移工具。
  部署前显式执行，gateway/worker/admin 启动时不做 DDL。

cmd/oaix-bench
  压测和回归辅助工具。
  模拟流式响应、上游 429/401/5xx、慢连接、中断连接。
```

### 6.2 内部包

```text
internal/config
  环境变量读取、配置校验、默认值。

internal/httpapi
  路由注册、中间件、request id、auth、response helpers。

internal/proxy
  代理主流程、attempt orchestration、retry/failover。

internal/protocol/openai
  OpenAI-compatible request/response model、codec、错误响应。

internal/protocol/sse
  SSE parser、writer、keepalive、terminal event。

internal/tokens
  token snapshot、selector、claim、cooldown、runtime state。

internal/affinity
  prompt cache affinity、response owner、lane policy。

internal/oauth
  refresh token、access token lifecycle、provider-specific OAuth client。

internal/transport
  upstream HTTP client pool、transport tuning、timeout policy。

internal/logs
  request log writer、outbox、hourly aggregation。

internal/store
  pgx repository、transaction helpers、batch statements。

internal/admin
  token/admin/request/import/settings API。

internal/importer
  token import job state machine。

internal/observability
  metrics、structured logs、traces、health/readiness。

internal/testkit
  fake upstream、SSE fixtures、DB fixtures、compatibility harness。
```

## 7. 请求热路径设计

目标流水线：

```text
HTTP request
-> auth and normalize
-> classify request intent
-> select token
-> build upstream attempt
-> stream upstream response
-> classify terminal outcome
-> commit token transition
-> enqueue/durably write request log
```

### 7.1 RequestIntent

所有入口先归一化为 `RequestIntent`：

- endpoint：responses/chat/images。
- model。
- account scope。
- stream/non-stream。
- prompt cache key。
- previous response id。
- body size。
- client request id。
- retry budget。
- requires non-free token。

这样 token 选择不直接依赖 FastAPI/Go handler 的原始请求对象，也不重复解析 body。

### 7.2 Claim

token claim 结果必须包含：

- token id。
- access token value 或 header material。
- token capability/scope。
- claim started timestamp。
- max stream concurrency。
- selected reason。
- affinity lane id。
- release callback。

claim 成功后，active stream counter 必须递增。任何返回路径，包括上游连接失败、客户端断开、panic recovery，都必须 release。

### 7.3 Attempt

一次上游尝试包含：

- claim。
- upstream URL。
- rewritten headers。
- rewritten body reader。
- timeout/deadline。
- attempt index。
- retry cause。

attempt 结束后只输出结构化 outcome，不在 proxy 主流程里散落 token 状态更新。

### 7.4 Outcome

统一结果分类：

```text
Success
ClientCanceled
UpstreamTimeout
Upstream429Cooldown
Upstream401Invalid
Upstream403Forbidden
Upstream5xxRetryable
UpstreamSchemaError
TransportErrorRetryable
TransportErrorTerminal
```

每种 outcome 映射到固定动作：

- 是否重试。
- 是否冷却 token。
- 是否禁用 token。
- 是否记录 quota。
- 是否写 final log。
- 是否向客户端透传 upstream error。
- 是否生成兼容错误帧。

## 8. token 池与调度设计

### 8.1 Snapshot

Go 版 snapshot 应为不可变结构，刷新后原子替换：

```text
TokenSnapshot
  version
  created_at
  tokens_by_id
  tokens_by_scope
  tokens_by_model
  enabled_tokens
  cooldown_index
  capability_index
```

要求：

- 读路径无锁或极少锁。
- token id lookup O(1)。
- 按 scope/model 查候选不全量扫描。
- snapshot refresh 不阻塞请求线程。
- stale snapshot 有明确策略：短时间使用旧版本，超过硬 TTL 进入 degraded readiness。

### 8.2 Selector

选择策略拆成可替换接口：

```text
Selector
  Select(ctx, intent, snapshot) -> ClaimCandidate
```

内置策略：

- LRU。
- fill-first。
- prompt-affinity。
- quota-aware。
- latency-aware。
- random-rendezvous。

策略可以组合，但组合结果必须在局部候选集内完成，避免每个请求对所有 token 排序。

### 8.3 并发计数

每个 token runtime state：

- active streams。
- recent TTFT EWMA。
- cooldown until。
- last selected at。
- last used at。
- failure streak。
- quota state。

active streams 使用进程内 atomic counter；多实例并发限制如果需要强一致，使用 Redis/Postgres lease。默认建议：

- 单 token soft limit：进程内快速判断。
- 全局 hard limit：Redis token bucket 或 Postgres lease，可按部署规模启用。

### 8.4 冷却与禁用

冷却和禁用不能只保存在进程内。

状态转换：

```text
active -> cooling
active -> disabled
cooling -> active
disabled -> active only by admin/worker validation
```

每次转换写入：

- `token_runtime_state`。
- `token_state_events` 或 outbox。
- metrics counter。
- structured log。

## 9. Prompt Cache Affinity 设计

当前 prompt cache affinity 的目标是尽量让同一 prompt/response 后续请求命中相同 token，以利用上游缓存。

Go 版目标：

- lane lookup O(1)。
- lane candidate 只在 lane 内部排序。
- 多实例可共享。
- token 被禁用/冷却后，lane 能快速剔除。

### 9.1 Lane Store

接口：

```text
LaneStore
  Get(promptKey) -> Lane
  Put(promptKey, Lane, ttl)
  RemoveToken(tokenID)
  BindResponseOwner(responseID, tokenID, ttl)
  GetResponseOwner(responseID) -> tokenID
```

实现：

- 本地内存实现：开发/单实例。
- Redis/Dragonfly 实现：生产多实例推荐。
- Postgres TTL 表实现：无 Redis 环境兜底。

### 9.2 Lane 数据结构

```text
Lane
  primary_token_id
  secondary_token_ids
  updated_at
  expires_at
  policy_version
```

选择顺序：

1. previous response owner。
2. primary lane token。
3. secondary lane tokens。
4. 新建 lane 候选。
5. 普通 selector fallback。

任何一步遇到 disabled/cooling/saturated token，都不全量扫描；只查询 token id map 和局部 fallback index。

## 10. 上游 HTTP 与流式协议

### 10.1 Transport

Go `http.Transport` 按上游 provider/host/shard 管理：

- `MaxIdleConns`
- `MaxIdleConnsPerHost`
- `MaxConnsPerHost`
- `IdleConnTimeout`
- `TLSHandshakeTimeout`
- `ResponseHeaderTimeout`
- `ExpectContinueTimeout`
- `DisableCompression`
- `ForceAttemptHTTP2`

连接池不通过私有字段维护，不扫描 `/proc`。连接泄漏通过测试和 metrics 捕捉：

- active requests。
- idle connections。
- dial count。
- TLS handshake count。
- response body close count。
- goroutine count。

### 10.2 SSE Parser

统一 SSE parser：

- 支持 `data:` 多行。
- 支持 comment/keepalive。
- 支持半包。
- 支持大 event 限制。
- 支持上下文取消。
- 不使用反复字符串拼接。

所有 stream endpoint 使用同一个 parser。

### 10.3 SSE Writer

统一 writer：

- 支持 keepalive。
- 支持 terminal error frame。
- 支持 client disconnect detection。
- 支持 flush error classification。
- 支持 final usage/log extraction。

### 10.4 Backpressure

stream 期间不应无限缓冲。

要求：

- 读上游和写客户端在同一个受控 pipeline 中。
- bounded channel。
- 客户端慢写时触发上下文取消或 deadline。
- 上游 body 关闭必须在所有退出路径执行。

## 11. Request Log 终局设计

### 11.1 日志主表

建议 request log 分区：

```text
request_logs_YYYYMMDD
```

字段保留兼容当前查询所需信息：

- request_id。
- parent_request_id。
- started_at。
- finished_at。
- endpoint。
- model。
- token_id。
- status。
- upstream_status。
- error_code。
- prompt_tokens。
- completion_tokens。
- cached_tokens。
- image_count。
- duration_ms。
- ttft_ms。
- retry_count。
- client_ip hash。
- metadata jsonb。

### 11.2 写入模式

写入分两类：

- start log：可降级，但应尽量进入 batch。
- final log：关键，必须 durable。

设计：

```text
gateway
-> in-memory bounded batcher
-> Postgres batch upsert
-> on failure write outbox
-> worker drains outbox
```

策略：

- final log 不静默 drop。
- start log 队列满时可采样/降级，但必须有 metrics。
- request_id 合并在 writer 内完成。
- hourly stats 不在主写入事务中做。

### 11.3 聚合统计

worker 维护：

- `request_hourly_stats`。
- token hourly stats。
- model hourly stats。
- error hourly stats。

查询管理面优先读聚合表；聚合缺口再读分区日志。

### 11.4 Retention

不用大范围 `DELETE`。

方案：

- 按天分区。
- retention worker drop old partition。
- 小表如 outbox/token events 可批量 delete with limit。

## 12. Token Import 与 OAuth Worker

### 12.1 Import Job 状态机

```text
created
-> staging
-> validating
-> publishing
-> completed
-> failed
-> canceled
```

item 状态：

```text
pending
-> claimed
-> validating
-> validated
-> publish_failed
-> published
-> failed
```

### 12.2 Worker 处理方式

要求：

- 批量 claim staged items。
- OAuth refresh 使用固定并发池。
- item 状态批量更新。
- publish token 批量 upsert。
- 重复 refresh token 归一化去重。
- 每个 job 有进度快照。
- worker 可重启恢复。

### 12.3 OAuth

OAuth client 独立包：

- refresh request builder。
- response parser。
- retry policy。
- error classifier。
- token identity extraction。
- refresh token history dedup。

OAuth 失败不能污染 gateway 热路径；gateway 只读取可用 access token 和 runtime state。

## 13. 数据库设计

### 13.1 迁移策略

只允许 `oaix-migrate` 修改 schema。

gateway/admin/worker 启动时：

- 检查 schema version。
- 不自动 create table。
- 不自动 create index。
- 不自动 backfill。
- schema 不匹配时 readiness fail。

### 13.2 推荐表

```text
schema_migrations

tokens
token_secrets
token_scopes
token_runtime_state
token_state_events
token_refresh_history

request_logs_YYYYMMDD
request_hourly_stats
request_log_outbox

prompt_affinity_lanes
response_owner_bindings

token_import_jobs
token_import_items

admin_audit_logs
settings
```

### 13.3 批处理 SQL 原则

- token last_used_at：`UPDATE ... FROM (VALUES ...)`。
- request logs：batch upsert 或 copy 到 staging 后 merge。
- token runtime state：按 token id 批量 update。
- import item state：批量 update，不逐 item round-trip。
- analytics 查询：优先聚合表和分区裁剪。

## 14. 管理面设计

### 14.1 Admin API

管理 API 与 gateway API 分开注册。

模块：

- token list/detail/update。
- token enable/disable/cooldown。
- token import jobs。
- request logs。
- analytics。
- runtime health。
- settings。
- audit logs。

要求：

- 所有列表分页。
- 支持 cursor-based pagination。
- 大字段按需加载。
- 批量操作异步化。
- 所有管理写操作写 admin audit log。

### 14.2 Admin Frontend

当前 `web/app.js` 单文件应终局拆分：

```text
web/src/api
web/src/state
web/src/views/tokens
web/src/views/requests
web/src/views/imports
web/src/views/settings
web/src/components
```

性能要求：

- token 大列表虚拟滚动。
- request log 分页和服务端筛选。
- 批量更新只 patch 局部 state。
- 图表数据读聚合 API。
- 不在浏览器端全量过滤/排序大数据集。

## 15. 配置设计

配置按结构收敛：

```text
server
database
request_log
token_pool
affinity
upstream_transport
oauth
import_worker
admin
observability
retention
```

兼容策略：

- 第一版 Go 服务可以读取现有环境变量名。
- 内部统一转换为 typed config。
- 启动时输出 sanitized config summary。
- 非法配置 fail fast。
- 默认值集中维护，不散落在业务包中。

## 16. 可观测性

### 16.1 Metrics

必须覆盖：

- requests total by endpoint/model/status。
- active streams。
- stream duration histogram。
- TTFT histogram。
- upstream attempts total。
- upstream status/error classification。
- token claim latency。
- token selector fallback count。
- token cooldown/disable transitions。
- request log queue depth。
- request log batch latency。
- outbox backlog。
- DB pool acquire latency。
- DB batch latency。
- OAuth refresh success/failure。
- import worker progress。
- goroutine count。

### 16.2 Logs

结构化日志字段：

- request_id。
- endpoint。
- model。
- token_id。
- upstream_attempt。
- outcome。
- error_class。
- cooldown_until。
- retry_count。
- duration_ms。
- ttft_ms。

### 16.3 Tracing

Span：

- inbound request。
- token select。
- upstream attempt。
- SSE first byte。
- request log write。
- token state commit。

## 17. 鲁棒性要求

### 17.1 取消传播

客户端断开时：

- context canceled。
- upstream request canceled。
- upstream body closed。
- claim released。
- final log 写入 canceled outcome。

### 17.2 Panic Recovery

gateway handler 层 recovery：

- 返回兼容错误。
- release claim。
- 记录 final log。
- metrics 计数。
- 不让 panic 杀进程。

### 17.3 队列满

队列满不能静默。

策略：

- 关键状态：同步写或 outbox。
- final log：outbox。
- start log：可采样丢弃，但必须 metrics。
- metrics/log 标记 degraded。

### 17.4 DB 短暂不可用

分级：

- token snapshot 可短时间沿用旧版本。
- request final log 进 outbox 或本地 WAL-like spool。
- 关键 token disabled/cooldown 写失败时，请求 outcome 标记 `state_commit_failed`，并触发 readiness degraded。
- 超过阈值后 gateway 不再接新流式请求。

### 17.5 多实例

要求：

- token 禁用和冷却对所有实例可见。
- prompt affinity 可共享或可明确声明为 best-effort。
- request log 不因实例退出丢 final。
- worker 任务使用 row lock / lease，避免重复处理。

## 18. API 兼容范围

Go 版必须兼容：

- `GET /livez`
- `GET /healthz`
- `POST /v1/chat/completions`
- `POST /v1/responses`
- `GET /v1/responses/{id}`
- `DELETE /v1/responses/{id}`
- `POST /v1/responses/{id}/cancel`
- `POST /v1/responses/compact`
- `POST /v1/images/generations`
- `POST /v1/images/edits`
- 现有 admin token API。
- 现有 admin request log API。
- 现有 token import API。

兼容不等于内部结构复制。所有 endpoint 应通过新的 RequestIntent / ProxyPipeline / Store 接口实现。

## 19. 测试策略

### 19.1 行为兼容测试

把现有 Python 测试提炼为黑盒兼容用例：

- token 选择。
- 429 冷却。
- 401/403 失效。
- responses stream terminal event。
- image stream synthetic completed event。
- request log start/final 合并。
- import job 去重。
- refresh token alias/history。

### 19.2 单元测试

Go 包单元测试：

- SSE parser 半包、多行、超大 event。
- FailureClassifier。
- RetryPolicy。
- TokenSelector。
- PromptAffinity。
- RequestLogBatcher。
- RuntimeStateMachine。

### 19.3 集成测试

使用 fake upstream：

- 慢 headers。
- 慢 body。
- 中途断流。
- 429 with retry-after。
- 401/403。
- 5xx 后成功。
- SSE malformed。
- 客户端中断。

### 19.4 压测

基准维度：

- 1k tokens。
- 10k tokens。
- 100 concurrent streams。
- 1k concurrent streams。
- 10k short non-stream requests。
- 上游 1%/5%/20% error rate。
- request log DB 延迟注入。
- worker backlog 注入。

关键指标：

- p50/p95/p99 claim latency。
- p50/p95/p99 TTFT。
- active goroutines 稳定性。
- DB pool wait。
- upstream connection reuse。
- final log loss count 必须为 0。

## 20. 终局建设顺序

这不是对 Python 单体的渐进式改造，而是 Go 终局系统的建设顺序。每一阶段产物都以终局架构为目标，不把 Python 内部结构作为长期边界。

1. 规格冻结。
   - 固定外部 API、错误响应、SSE 行为、admin API 行为。
   - 固定 token 状态机。
   - 固定 request log schema。

2. Go 基础骨架。
   - 建 module、config、store、transport、observability。
   - 建 fake upstream 和兼容测试框架。

3. 热路径。
   - 实现 token snapshot/selector。
   - 实现 proxy pipeline。
   - 实现 SSE parser/writer。
   - 实现 responses/chat/image endpoints。

4. 状态与日志。
   - 实现 runtime state machine。
   - 实现 durable request log writer。
   - 实现 outbox/aggregation worker。

5. 后台任务。
   - 实现 OAuth refresh。
   - 实现 token import worker。
   - 实现 retention/repair worker。

6. 管理面。
   - 实现 admin API。
   - 重建前端模块。
   - 接入 audit log。

7. 性能验收。
   - 执行流式压测。
   - 执行大 token 池调度压测。
   - 执行 DB 故障和上游故障注入。

8. 替换上线。
   - Go 服务通过兼容测试和压测门槛后，作为 oaix 新实现替换 Python 单体。
   - Python 项目保留为历史参考和回归样本，不继续承载新功能。

## 21. 详细 TODO List

### 21.1 架构决策

- [x] 确认 Go 重写为终局方向，不继续对 Python 单体做长期结构演进。
- [x] 确认 gateway/admin/worker/migrate 是否拆成独立二进制。
- [x] 确认是否引入 Redis/Dragonfly 承载 prompt affinity 和全局并发 lease。
- [x] 确认 Postgres 是否继续作为唯一强一致状态存储。
- [x] 确认 request log 是否按天分区。
- [x] 确认 final request log 是否必须零丢失。
- [x] 确认 token cooldown/disabled 是否必须跨实例强一致。
- [x] 确认 admin UI 是否随 Go 重构一起重建。

### 21.2 API 规格冻结

- [x] 梳理 `/v1/responses` 请求字段兼容范围。
- [x] 梳理 `/v1/responses` 流式 event 兼容范围。
- [x] 梳理 response id 查询、取消、删除行为。
- [x] 梳理 `/v1/chat/completions` 兼容范围。
- [x] 梳理 `/v1/images/generations` 兼容范围。
- [x] 梳理 `/v1/images/edits` multipart 兼容范围。
- [x] 梳理 admin token API。
- [x] 梳理 admin request log API。
- [x] 梳理 token import API。
- [x] 固定错误响应格式。
- [x] 固定 SSE terminal error frame 格式。
- [x] 固定 health/readiness 响应格式。

### 21.3 Go 工程骨架

- [x] 创建 Go module。
- [x] 建立 `cmd/oaix-gateway`。
- [x] 建立 `cmd/oaix-admin`。
- [x] 建立 `cmd/oaix-worker`。
- [x] 建立 `cmd/oaix-migrate`。
- [x] 建立 `internal/config`。
- [x] 建立 `internal/store`。
- [x] 建立 `internal/proxy`。
- [x] 建立 `internal/tokens`。
- [x] 建立 `internal/affinity`。
- [x] 建立 `internal/transport`。
- [x] 建立 `internal/logs`。
- [x] 建立 `internal/oauth`。
- [x] 建立 `internal/admin`。
- [x] 建立 `internal/observability`。
- [x] 建立 `internal/testkit`。
- [x] 建立 lint/test/benchmark 命令。

### 21.4 配置

- [x] 收集当前所有 oaix 环境变量。
- [x] 设计 typed config schema。
- [x] 设计默认值集中表。
- [x] 实现环境变量兼容读取。
- [x] 实现配置校验。
- [x] 实现 sanitized config logging。
- [x] 实现配置单元测试。

### 21.5 数据库与迁移

- [x] 设计 `schema_migrations`。
- [x] 设计 `tokens`。
- [x] 设计 `token_secrets`。
- [x] 设计 `token_scopes`。
- [x] 设计 `token_runtime_state`。
- [x] 设计 `token_state_events`。
- [x] 设计 `token_refresh_history`。
- [x] 设计 request log 分区表。
- [x] 设计 `request_hourly_stats`。
- [x] 设计 `request_log_outbox`。
- [x] 设计 `prompt_affinity_lanes`。
- [x] 设计 `response_owner_bindings`。
- [x] 设计 `token_import_jobs`。
- [x] 设计 `token_import_items`。
- [x] 设计 `admin_audit_logs`。
- [x] 编写 up migration。
- [x] 编写 down migration。
- [x] 编写迁移幂等测试。
- [x] 移除 runtime DDL 依赖。

### 21.6 Store 层

- [x] 实现 pgxpool 初始化。
- [x] 实现 workload-specific pool。
- [x] 实现 token repository。
- [x] 实现 runtime state repository。
- [x] 实现 request log repository。
- [x] 实现 import job repository。
- [x] 实现 affinity repository。
- [x] 实现 batch update token last_used_at。
- [x] 实现 batch upsert request logs。
- [x] 实现 outbox enqueue/drain。
- [x] 实现 transaction helper。
- [x] 实现 DB pool metrics。

### 21.7 Token Snapshot

- [x] 定义 TokenSnapshot。
- [x] 建 `tokens_by_id`。
- [x] 建 `tokens_by_scope`。
- [x] 建 `tokens_by_model`。
- [x] 建 cooldown index。
- [x] 建 capability index。
- [x] 实现 atomic snapshot swap。
- [x] 实现 snapshot refresh loop。
- [x] 实现 snapshot hard TTL。
- [x] 实现 stale snapshot degraded readiness。
- [x] 实现 snapshot metrics。
- [x] 实现大 token 池 benchmark。

### 21.8 Token Selector

- [x] 定义 Selector 接口。
- [x] 实现 LRU selector。
- [x] 实现 fill-first selector。
- [x] 实现 quota-aware selector。
- [x] 实现 latency-aware selector。
- [x] 实现 prompt-affinity selector adapter。
- [x] 实现 excluded token set。
- [x] 实现 max active stream limit。
- [x] 实现 selected reason。
- [x] 实现 selector fallback metrics。
- [x] 实现 1k/10k token latency benchmark。

### 21.9 Runtime State Machine

- [x] 定义 token state transition。
- [x] 定义 cooldown reason。
- [x] 定义 disabled reason。
- [x] 实现 active -> cooling。
- [x] 实现 active -> disabled。
- [x] 实现 cooling -> active。
- [x] 实现 admin re-enable。
- [x] 实现 state event 写入。
- [x] 实现关键状态 durable commit。
- [x] 实现 commit failure 处理。
- [x] 实现 multi-instance visibility test。

### 21.10 Prompt Affinity

- [x] 定义 LaneStore 接口。
- [x] 实现 in-memory LaneStore。
- [x] 评估并实现 Redis/Dragonfly LaneStore。
- [x] 实现 Postgres LaneStore 兜底。
- [x] 实现 response owner binding。
- [x] 实现 lane TTL。
- [x] 实现 RemoveToken。
- [x] 实现 primary/secondary lane selection。
- [x] 实现 disabled/cooling token 剔除。
- [x] 实现 affinity metrics。
- [x] 实现多实例一致性测试。

### 21.11 Upstream Transport

- [x] 定义 upstream transport config。
- [x] 实现 per-host client pool。
- [x] 实现 shard 策略。
- [x] 配置 MaxIdleConns。
- [x] 配置 MaxConnsPerHost。
- [x] 配置 IdleConnTimeout。
- [x] 配置 ResponseHeaderTimeout。
- [x] 配置 TLSHandshakeTimeout。
- [x] 实现 request cloning/rewrite。
- [x] 实现 body close guard。
- [x] 实现 transport metrics。
- [x] 实现连接泄漏测试。
- [x] 实现客户端中断测试。

### 21.12 SSE 与协议处理

- [x] 实现 SSE parser。
- [x] 支持半包。
- [x] 支持多行 data。
- [x] 支持 comment/keepalive。
- [x] 支持 malformed event 分类。
- [x] 实现 SSE writer。
- [x] 实现 keepalive。
- [x] 实现 terminal error frame。
- [x] 实现 flush error 处理。
- [x] 实现 responses codec。
- [x] 实现 chat codec。
- [x] 实现 image codec。
- [x] 建立 SSE fixture tests。

### 21.13 Proxy Pipeline

- [x] 定义 RequestIntent。
- [x] 定义 Claim。
- [x] 定义 Attempt。
- [x] 定义 Outcome。
- [x] 定义 FailureClassifier。
- [x] 定义 RetryPolicy。
- [x] 实现 auth/normalize middleware。
- [x] 实现 responses endpoint。
- [x] 实现 chat completions endpoint。
- [x] 实现 images generations endpoint。
- [x] 实现 images edits endpoint。
- [x] 实现 retry/failover。
- [x] 实现 token release guarantee。
- [x] 实现 client cancel propagation。
- [x] 实现 final log hook。
- [x] 实现 token state commit hook。

### 21.14 Request Log

- [x] 定义 request log model。
- [x] 定义 start log。
- [x] 定义 final log。
- [x] 实现 in-memory bounded batcher。
- [x] 实现 request_id 合并。
- [x] 实现 batch upsert。
- [x] 实现 final log durable fallback。
- [x] 实现 request_log_outbox。
- [x] 实现 outbox drain worker。
- [x] 实现 queue full metrics。
- [x] 实现 hourly aggregation worker。
- [x] 实现 retention worker。
- [x] 实现 log loss fault injection test。

### 21.15 OAuth

- [x] 定义 OAuth provider client。
- [x] 实现 refresh request。
- [x] 实现 refresh response parser。
- [x] 实现 OAuth retry policy。
- [x] 实现 OAuth error classification。
- [x] 实现 refresh token identity。
- [x] 实现 refresh history dedup。
- [x] 实现 token secret update。
- [x] 实现 OAuth metrics。
- [x] 实现 fake OAuth server tests。

### 21.16 Import Worker

- [x] 定义 import job state machine。
- [x] 定义 import item state machine。
- [x] 实现 batch claim items。
- [x] 实现 OAuth validation pool。
- [x] 实现 batch item update。
- [x] 实现 batch publish tokens。
- [x] 实现 duplicate handling。
- [x] 实现 cancel job。
- [x] 实现 resume after crash。
- [x] 实现 job progress snapshot。
- [x] 实现 import worker metrics。

### 21.17 Admin API

- [x] 实现 admin auth。
- [x] 实现 token list pagination。
- [x] 实现 token detail。
- [x] 实现 token enable/disable。
- [x] 实现 token cooldown clear。
- [x] 实现 batch token operation。
- [x] 实现 request log query。
- [x] 实现 analytics query。
- [x] 实现 import job create/list/detail/cancel。
- [x] 实现 runtime health API。
- [x] 实现 settings API。
- [x] 实现 admin audit log。

### 21.18 Admin Frontend

- [x] 确定前端技术栈。
- [x] 拆分 API client。
- [x] 拆分 token view。
- [x] 拆分 request log view。
- [x] 拆分 import view。
- [x] 拆分 settings view。
- [x] 实现 token 虚拟列表。
- [x] 实现服务端分页。
- [x] 实现批量操作交互。
- [x] 实现 job progress view。
- [x] 实现 analytics charts。
- [x] 实现错误和 loading 状态。

### 21.19 Observability

- [x] 接入 structured logging。
- [x] 接入 Prometheus metrics。
- [x] 接入 tracing。
- [x] 实现 request id propagation。
- [x] 实现 upstream attempt span。
- [x] 实现 token select span。
- [x] 实现 request log write span。
- [x] 实现 readiness checks。
- [x] 实现 liveness checks。
- [x] 实现 degraded state reporting。
- [x] 建立 dashboard 指标清单。
- [x] 建立 alert 规则清单。

### 21.20 测试与验收

- [x] 提炼 Python 行为测试为黑盒兼容测试。
- [x] 建 fake upstream。
- [x] 建 fake OAuth server。
- [x] 建 Postgres integration test fixture。
- [x] 建 Redis/Dragonfly integration test fixture。
- [x] 覆盖 429 cooldown。
- [x] 覆盖 401 invalid。
- [x] 覆盖 403 forbidden。
- [x] 覆盖 5xx retry。
- [x] 覆盖 malformed SSE。
- [x] 覆盖 client cancel。
- [x] 覆盖 upstream timeout。
- [x] 覆盖 DB unavailable。
- [x] 覆盖 queue full。
- [x] 覆盖 process restart recovery。
- [x] 执行 1k token benchmark。
- [x] 执行 10k token benchmark。
- [x] 执行 1k concurrent streams benchmark。
- [x] 验证 final log loss count 为 0。
- [x] 验证 goroutine 无泄漏。
- [x] 验证连接无泄漏。

### 21.21 部署替换

- [x] 定义 Go 服务镜像。
- [x] 定义 migration job。
- [x] 定义 gateway deployment。
- [x] 定义 worker deployment。
- [x] 定义 admin deployment。
- [x] 定义 config/secrets。
- [x] 定义 readiness/liveness probes。
- [x] 定义 resource requests/limits。
- [x] 定义 rollback 条件。
- [x] 定义流量切换条件。
- [x] 定义上线后观察指标。
- [x] 定义 Python 旧实现冻结策略。

## 22. 审阅重点

建议重点审阅以下决策：

1. 是否接受 Go 重写作为终局，不再投资 Python 单体长期演进。
2. 是否接受 request final log 零丢失要求。
3. 是否接受 token 冷却/禁用跨实例强一致要求。
4. 是否引入 Redis/Dragonfly 作为多实例 affinity/lease 层。
5. request log 是否按天分区。
6. admin UI 是否一起重建。
7. 上线替换时是否允许保留 Python 为只读参考实现。

## 23. 目标终态

最终 oaix 应成为：

- Go 实现的高并发模型账号池网关。
- 热路径小而稳定。
- 状态机明确。
- 流式协议统一。
- 日志可靠。
- 后台任务独立。
- 多实例可横向扩展。
- 管理面可维护。
- 通过兼容测试、故障注入和性能压测后替换当前 Python 单体。

## 24. 当前实现与验证记录

### 24.1 已落地代码

- Go module：`github.com/yym68686/oaix`。
- 二进制入口：`cmd/oaix-gateway`、`cmd/oaix-admin`、`cmd/oaix-worker`、`cmd/oaix-migrate`、`cmd/oaix-bench`。
- 核心包：`internal/config`、`internal/store`、`internal/tokens`、`internal/proxy`、`internal/transport`、`internal/logs`、`internal/affinity`、`internal/oauth`、`internal/httpapi`、`internal/observability`。
- 多实例 affinity：`internal/affinity` 已提供 in-memory、Postgres、Redis/Dragonfly 三种 LaneStore；Postgres 为无 Redis 环境兜底，Redis/Dragonfly 为生产多实例推荐选项。
- Request log：默认单 batch writer + 可重试 upsert，避免 1k 并发下 start/final upsert 死锁直接落 outbox；final log 仍保留 sync/outbox fallback。
- Import worker：已定义 job/item 状态机，支持 batch claim、并发 validation、batch item update、batch publish、cancel job、stale job resume 和 worker metrics。
- Admin frontend：运行入口切到 ES module，拆分为 `api.js`、`tokenView.js`、`requestView.js`、`importView.js`、`settingsView.js`、`charts.js`、`state.js`、`dom.js`；实现 token 虚拟列表、批量操作、job progress、analytics charts、loading/error 状态。
- Docker：多阶段 Go 构建，runtime 使用 distroless；compose 增加 `migrate` 和 `worker` 服务。
- runtime DDL：gateway 启动只检查 schema version，不自动迁移；迁移由 `oaix-migrate` 执行。

### 24.2 本地功能验证

验证环境：

```text
Postgres: docker compose postgres
Gateway: go run ./cmd/oaix-gateway
Migrate: go run ./cmd/oaix-migrate
Fake upstream: 127.0.0.1:18081
Token file: /Users/yanyuming/Downloads/at.txt
Imported tokens: 10
```

已验证：

- `GET /livez` 正常。
- `GET /healthz` 正常，`available=10`。
- `GET /admin/tokens?limit=3` 正常分页。
- `POST /admin/tokens/import` 可导入/更新 access token。
- `GET /admin/tokens/import-batches` 返回稳定数组。
- `POST /admin/tokens/import-jobs/{id}/cancel` 正常返回 job 状态。
- `POST /admin/tokens/{id}/activation` 可禁用并重新启用 token。
- `GET /admin/tokens/{id}` 可查询 token detail。
- `POST /admin/tokens/{id}/remark` 可更新备注。
- `GET /admin/settings` / `POST /admin/settings/{key}` 正常。
- 缺少 `SERVICE_API_KEYS` 鉴权时返回 401。
- `POST /v1/responses` 非流式代理成功。
- `POST /v1/responses` 流式代理成功，能收到 keepalive 和上游 SSE event。
- `POST /v1/chat/completions` 代理成功。
- `POST /v1/images/generations` 代理成功。
- `POST /v1/images/edits` multipart 代理成功。
- `GET /admin/requests` 能看到 start/final 合并后的日志。
- 客户端中断后不会再发生已提交响应上的二次 `WriteHeader`。
- 1000 并发 stream 压测后，`active_streams=0`、`upstream_active_requests=0`、`request_log_dropped_total=0`、`request_log_outbox_writes_total=0`、`token_state_commit_failures_total=0`。

### 24.3 测试基线

```text
uv run pytest -q
354 passed, 1 skipped

go test ./...
PASS

docker compose build gateway migrate worker
PASS

node --check oaix_gateway/web/src/*.js
PASS
```

### 24.4 性能证据

命令：

```text
go test ./internal/tokens -bench='Benchmark(SnapshotByIDLookup10000|LinearTokenLookup10000|Claim10000)$' -benchmem -count=3
```

结果摘要：

```text
BenchmarkSnapshotByIDLookup10000: ~8.0-8.6 ns/op, 0 allocs/op
BenchmarkLinearTokenLookup10000: ~4.3-8.2 us/op, 0 allocs/op
BenchmarkClaim10000: ~148-507 ns/op, 2 allocs/op
```

结论：

- 10k token 下，`tokens_by_id` map lookup 相比线性扫描约快 450x 以上。
- claim 热路径移除 per-claim goroutine 后，从早期约 500ns/8 allocs 降到 148-507ns/2 allocs；接入 selector 接口后仍保持亚微秒级。
- 这些数字来自本地 benchmark，不是推测。

1k concurrent streams benchmark：

```text
go run ./cmd/oaix-bench \
  -target http://127.0.0.1:18080 \
  -api-key test-key \
  -streams 1000 \
  -concurrency 1000 \
  -timeout 90s
```

结果：

```json
{
  "total": 1000,
  "ok": 1000,
  "failed": 0,
  "elapsed_ms": 674,
  "p50_ms": 326,
  "p95_ms": 642,
  "p99_ms": 660,
  "status_counts": {"200": 1000}
}
```

压测后指标：

```text
oaix_token_active_streams 0
oaix_upstream_active_requests 0
oaix_upstream_errors_total 0
oaix_request_log_dropped_total 0
oaix_request_log_outbox_writes_total 0
oaix_token_state_commit_failures_total 0
```

### 24.5 部署替换定义

资源建议：

```text
gateway:
  requests: cpu 500m, memory 512Mi
  limits: cpu 2, memory 2Gi

worker:
  requests: cpu 100m, memory 256Mi
  limits: cpu 1, memory 1Gi

migrate:
  requests: cpu 100m, memory 128Mi
  limits: cpu 1, memory 512Mi
```

Rollback 条件：

- `/healthz` 连续 3 次 degraded。
- 5xx 比例 5 分钟内超过旧版 2 倍。
- request final log outbox backlog 持续增长超过 5 分钟。
- token available 数量异常下降超过 20%。
- p95 TTFT 较旧版升高超过 30%。

流量切换条件：

- migration 成功。
- gateway `/livez` 和 `/healthz` 正常。
- 本地/预发 fake upstream 兼容测试通过。
- 10k token selector benchmark 结果满足亚微秒 claim。
- request log dropped total 为 0。

上线后观察指标：

- `oaix_token_ready_tokens`
- `oaix_token_active_streams`
- `oaix_token_snapshot_age_ms`
- `oaix_request_log_queue_depth`
- `oaix_request_log_written_total`
- `oaix_request_log_dropped_total`
- `oaix_request_log_outbox_writes_total`
- DB pool acquired/idle/acquire count

Python 旧实现冻结策略：

- Python 目录保留为行为参考和回归测试来源。
- 新功能只进入 Go 实现。
- Python 测试继续作为旧行为保护网，直到 Go 黑盒兼容测试完整覆盖后再移除。

Dashboard 清单：

- Token pool：ready tokens、active streams、snapshot age、cooldown/disabled transition。
- Request log：queue depth、written total、dropped total、outbox writes/backlog。
- Gateway：request count、status class、stream duration、TTFT。
- Upstream：attempt count、429/401/403/5xx classification。
- DB：pool acquired/idle、acquire count、acquire duration。

Alert 清单：

- `oaix_request_log_dropped_total` 5 分钟内增加。
- `oaix_token_snapshot_age_ms` 超过 hard TTL。
- `oaix_token_ready_tokens` 低于部署前基线 80%。
- DB pool acquired 长时间接近 max。
- 5xx 或 upstream retryable outcome 明显升高。
