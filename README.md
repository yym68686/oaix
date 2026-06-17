# oaix-gateway

从 `oai-x` 单独拆出的精简项目，只保留这些能力：

- key 池持久化与状态维护
- 请求前选择可用 key
- refresh token 换 access token
- 429 配额冷却
- 401/403 失效处理
- `/v1/responses` 端点网关代理
- OpenAI 兼容的 `/v1/images/generations`、`/v1/images/edits` 图片接口
- 基于 RT 历史链的 key 去重

明确不包含原项目的这些部分：

- 注册 OpenAI/Codex 账号
- 邮箱供应商接入
- Sentinel/注册补号流程
- 自动补号脚本和相关后台任务

## 目录

- `cmd/oaix-gateway`: Go 网关主进程，承载 OpenAI-compatible API、管理 API、token snapshot 调度、流式代理。
- `cmd/oaix-migrate`: Go 数据库迁移工具。源码本地运行时 gateway 默认只检查 schema；容器镜像为适配 Fugue 单容器部署，默认 `OAIX_AUTO_MIGRATE_ON_STARTUP=true`，会在 gateway 启动前执行同一套幂等迁移。
- `cmd/oaix-worker`: Go 后台 worker，当前负责 request log outbox drain。
- `internal/*`: Go 终局实现的配置、存储、代理、token 池、日志、transport、observability 等模块。
- `oaix_gateway/*`: Python 旧实现，保留为行为参考和回归测试来源。
- `import_tokens.py`: Python 旧实现的本地导入辅助脚本。

## 环境变量

- `DATABASE_URL`: PostgreSQL DSN，默认 `postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/oaix_gateway`
- `SERVICE_API_KEYS`: 服务端鉴权 key，多个用逗号分隔；未设置时不鉴权
- `CORS_ALLOW_ORIGINS`: 浏览器跨域允许的 Origin 列表，逗号分隔；默认 `*`
- `CORS_ALLOW_ORIGIN_REGEX`: 浏览器跨域允许的 Origin 正则；默认不设置
- `CORS_ALLOW_CREDENTIALS`: 是否允许浏览器跨域携带 credentials；默认 `false`
- `CODEX_BASE_URL`: 上游 Codex responses 地址。默认 `https://chatgpt.com/backend-api/codex/responses`
- `MAX_REQUEST_ACCOUNT_RETRIES`: 单次请求最多切换多少个 key，默认 `100`
- `PROXY_MAX_ACTIVE_RESPONSES`: 网关同时处理的 `/v1/responses*` 请求上限，默认 `64`
- `PROXY_QUEUE_TIMEOUT_SECONDS`: 达到并发上限后排队等待秒数，默认 `1`
- `TOKEN_POOL_SNAPSHOT_MAX_AGE_SECONDS`: 内存 key 池快照最大复用时间，默认 `10`
- `PROMPT_CACHE_AFFINITY_ENABLED`: 是否启用 prompt cache 亲和调度，默认 `true`
- `PROMPT_CACHE_AUTO_KEY_ENABLED`: 请求未带 `prompt_cache_key` 时是否自动从稳定上下文派生，默认 `true`
- `PROMPT_CACHE_MAX_LANES_PER_KEY`: 每个 cache key 最多绑定多少个 key lane，默认 `3`
- `PROMPT_CACHE_PRIMARY_WAIT_MS`: primary lane 满载时最多短等毫秒数，默认 `500`
- `PROMPT_CACHE_LANE_WAIT_MS`: 所有已绑定 lane 满载时的二次短等毫秒数，默认 `100`
- `PROMPT_CACHE_GLOBAL_FALLBACK_ENABLED`: lanes 都不可用时是否允许走全局低负载 key，默认 `true`
- `PROMPT_CACHE_SESSION_ID_MODE`: prompt cache 请求的上游 `Session_id` 选择策略，默认使用由 cache key 派生的稳定值；设为 `header` 可恢复优先透传客户端 header
- `PROMPT_CACHE_PREVIOUS_OWNER_WAIT_MS`: `previous_response_id` owner key 满载时最多短等毫秒数，默认 `800`
- `PROMPT_CACHE_PREVIOUS_REPLAY_FALLBACK_ENABLED`: owner 忙时是否允许 `previous_response_id` 请求 fallback 到 prompt cache lanes，仅在调用方已保证 stateless replay 时开启，默认 `false`
- `PROMPT_CACHE_REBIND_PRIMARY`: spillover 是否覆盖 primary lane，默认 `false`
- `PROMPT_CACHE_LANE_TTL_SECONDS`: cache key lane 状态空闲保留秒数，默认 `3600`
- `IMAGE_REQUEST_MAX_ACCOUNT_RETRIES`: 图片接口单次请求最多切换多少个 key，默认 `8`
- `IMAGE_INPUT_MAX_PER_REQUEST`: 单次图片请求最多允许多少张输入图片，默认 `249`；达到上游 `input-images per min` 桶大小前直接拒绝超大请求
- `IMAGE_UPLOAD_MAX_BYTES`: multipart 图片上传单文件最大字节数，默认 `26214400`（25 MiB）
- `UPSTREAM_NON_STREAM_MAX_RESPONSE_BYTES`: 非流式上游响应聚合最大字节数，默认 `67108864`（64 MiB）
- `STREAM_CAPTURE_MAX_BYTES`: 流式响应中用于错误/统计解析的最大捕获字节数，默认 `8388608`（8 MiB）；超过后继续透传但停止内存捕获
- `IMAGE_RATE_LIMIT_DEFAULT_COOLDOWN_SECONDS`: `gpt-image-2` 撞到上游 `input-images` 短速率限制且未返回明确重试时间时的 scoped 冷却秒数，默认 `5`
- `IMAGE_RATE_LIMIT_MIN_COOLDOWN_SECONDS`: 解析到 `Please try again in ...` 时的最小 scoped 冷却秒数，默认 `1`
- `DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS`: 429 且没有明确重置时间时的默认冷却秒数，默认 `300`
- `REQUEST_LOG_WRITE_CONCURRENCY`: 请求日志异步写入并发数，默认 `2`
- `REQUEST_LOG_WRITE_BATCH_SIZE`: 请求日志异步写入批大小，默认 `50`
- `REQUEST_LOG_WRITE_QUEUE_MAX_SIZE`: 请求日志写入队列最大长度，默认 `2000`；满队列时会丢弃日志而不阻塞请求
- `REQUEST_LOG_RETENTION_DAYS`: 请求日志保留天数，默认 `30`；设为 `0` 关闭清理
- `REQUEST_LOG_CLEANUP_INTERVAL_SECONDS`: 请求日志清理后台任务间隔秒数，默认 `3600`
- `ADMIN_REQUESTS_CACHE_TTL_SECONDS`: `/admin/requests` 汇总与图表缓存 TTL，默认 `5`
- `ADMIN_TOKEN_COUNTS_CACHE_TTL_SECONDS`: `/admin/tokens` 统计计数缓存 TTL，默认 `2`
- `IMPORT_JOB_MAX_CONCURRENCY`: 后台导入 key 的 OAuth 验证最大并发数，默认 `16`
- `IMPORT_STAGING_INSERT_BATCH_SIZE`: 导入 staging 明细批量写入大小，默认 `1000`
- `IMPORT_PUBLISH_BATCH_SIZE`: 验证成功后发布到正式 key 池的批大小，默认 `20`
- `IMPORT_PUBLISH_FLUSH_INTERVAL_SECONDS`: 多批发布之间的间隔秒数，默认 `0.5`
- `IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC`: 后台导入是否为活跃 `/v1/responses*` 流量让路，默认 `false`；仅建议作为过载保护开关临时开启
- `TOKEN_IMPORT_PAYLOAD_SECRET`: staging 区 `encrypted_payload` 的加密密钥；未设置时会从 `DATABASE_URL` 派生
- `ADMIN_DATABASE_POOL_SIZE` / `ADMIN_DATABASE_MAX_OVERFLOW` / `ADMIN_DATABASE_POOL_TIMEOUT_SECONDS`: 管理端 DB 池参数
- `IMPORT_DATABASE_POOL_SIZE` / `IMPORT_DATABASE_MAX_OVERFLOW` / `IMPORT_DATABASE_POOL_TIMEOUT_SECONDS`: 导入 worker DB 池参数
- `REQUEST_LOG_DATABASE_POOL_SIZE` / `REQUEST_LOG_DATABASE_MAX_OVERFLOW` / `REQUEST_LOG_DATABASE_POOL_TIMEOUT_SECONDS`: 请求日志写入 DB 池参数
- `FUGUE_OBSERVABILITY_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`: 可选 Fugue telemetry-agent HTTP 端点，例如 `http://fugue-fugue-telemetry-agent:7834`；未设置时不启用，业务行为与当前一致
- `FUGUE_OBSERVABILITY_SERVICE_NAME`: 可选观测服务名，默认 `oaix`
- `FUGUE_OBSERVABILITY_SERVICE_VERSION`: 可选观测版本标记
- `FUGUE_OBSERVABILITY_QUEUE_MAX_SIZE`: Fugue 观测旁路发送队列大小，默认 `1000`；满队列时丢弃诊断事件，不阻塞请求
- `FUGUE_OBSERVABILITY_EXPORT_TIMEOUT_SECONDS`: Fugue 观测 HTTP 发送超时，默认 `2`
- `FUGUE_OBSERVABILITY_SAMPLE_RATE`: 成功请求诊断采样率，默认 `1`；错误请求始终保留诊断事件
- `FUGUE_OBSERVABILITY_TENANT_ID` / `FUGUE_OBSERVABILITY_PROJECT_ID` / `FUGUE_OBSERVABILITY_APP_ID` / `FUGUE_OBSERVABILITY_RUNTIME_ID`: 可选 Fugue 观测身份字段；部署在 Fugue 内部时通常由平台侧 telemetry-agent 注入
- `FUGUE_OBSERVABILITY_REQUEST_SUMMARY_ENABLED` / `FUGUE_OBSERVABILITY_STAGE_SPANS_ENABLED` / `FUGUE_OBSERVABILITY_METRICS_ENABLED`: 分别控制 request summary、阶段 span、低基数 metric 诊断副本，默认均开启
- `IMPORT_RESPONSE_IDLE_GRACE_SECONDS`: 导入 key 遇到活跃 `/v1/responses*` 流量时，等流量清空后再额外静默多久继续补号，默认 `0.25`
- `IMPORT_WAIT_TIMEOUT_SECONDS`: 导入 key 在为活跃 `/v1/responses*` 流量让路时，最多等待多久；超时后不再整批返回 `503`，而是记一次超时并继续低优先级导入，默认 `30`
- `COMPACT_SERVER_ERROR_COOLDOWN_SECONDS`: `/v1/responses/compact` 遇到上游 5xx / 传输错误时的冷却秒数，默认 `60`；设为 `0` 可关闭
- `HOST`: 内置启动命令监听地址，默认 `0.0.0.0`
- `PORT`: 内置启动命令端口，默认 `8000`

## 安装

```bash
go mod download
```

## 启动

```bash
export DATABASE_URL='postgresql://oaix:oaix_password@127.0.0.1:5432/oaix_gateway'
go run ./cmd/oaix-migrate
go run ./cmd/oaix-gateway
```

导入本地 access token 文件并启动：

```bash
OAIX_ACCESS_TOKEN_FILE='/Users/yanyuming/Downloads/at.txt' \
SERVICE_API_KEYS='change-me' \
go run ./cmd/oaix-gateway
```

启动后直接打开：

```bash
http://127.0.0.1:8000/
```

根路径现在带一个前端控制台，可展示：

- 当前有效 key 数量
- 正在冷却的 key 数量
- 已禁用的 key 数量
- 请求总数、成功/失败数、平均首字时间
- 最近请求日志，包括请求端点、状态码、首字时间、尝试次数和错误摘要
- 可按搜索、状态、计划和排序全局查询 key 状态列表
- 可独立查看导入批次，并从批次进入关联 key
- 导入 key 的表单和 JSON 文件导入入口

## Docker Compose

项目根目录已经带了 `docker-compose.yml` 和 `Dockerfile`，可以直接启动 PostgreSQL 和网关：

```bash
docker compose up -d --build
```

默认行为：

- `postgres`: PostgreSQL 16
- `migrate`: 显式执行 Go schema migration
- `gateway`: Go Web 控制台 + `/v1/responses`、`/v1/chat/completions`、`/v1/images/*` 网关；容器内默认启动前执行幂等迁移，避免没有独立 migration job 的部署平台启动失败
- `worker`: Go request log outbox worker

常用环境变量：

```bash
export GATEWAY_PORT='8000'
export POSTGRES_DB='oaix_gateway'
export POSTGRES_USER='oaix'
export POSTGRES_PASSWORD='oaix_password'
export SERVICE_API_KEYS='change-me'
export CORS_ALLOW_ORIGINS='https://your-app.example'
export CODEX_BASE_URL=''
export PROMPT_CACHE_AFFINITY_ENABLED='true'
export PROMPT_CACHE_AUTO_KEY_ENABLED='true'
export PROMPT_CACHE_MAX_LANES_PER_KEY='3'
export PROMPT_CACHE_PRIMARY_WAIT_MS='500'
export PROMPT_CACHE_LANE_WAIT_MS='100'
export PROMPT_CACHE_GLOBAL_FALLBACK_ENABLED='true'
export PROMPT_CACHE_PREVIOUS_OWNER_WAIT_MS='800'
export PROMPT_CACHE_PREVIOUS_REPLAY_FALLBACK_ENABLED='false'
export PROMPT_CACHE_REBIND_PRIMARY='false'
export PROMPT_CACHE_LANE_TTL_SECONDS='3600'
export IMPORT_JOB_MAX_CONCURRENCY='16'
export IMPORT_STAGING_INSERT_BATCH_SIZE='1000'
export IMPORT_PUBLISH_BATCH_SIZE='20'
export IMPORT_PUBLISH_FLUSH_INTERVAL_SECONDS='0.5'
export IMPORT_JOB_RESPECT_RESPONSE_TRAFFIC='false'
export IMPORT_RESPONSE_IDLE_GRACE_SECONDS='0.25'
export IMPORT_WAIT_TIMEOUT_SECONDS='30'
export REQUEST_LOG_WRITE_CONCURRENCY='1'
export OAIX_AUTO_MIGRATE_ON_STARTUP='true'
export REQUEST_LOG_WRITE_BATCH_SIZE='250'
export REQUEST_LOG_WRITE_QUEUE_MAX_SIZE='20000'
export REQUEST_LOG_RETENTION_DAYS='30'
export REQUEST_LOG_CLEANUP_INTERVAL_SECONDS='3600'
export ADMIN_REQUESTS_CACHE_TTL_SECONDS='5'
export ADMIN_TOKEN_COUNTS_CACHE_TTL_SECONDS='2'
export IMAGE_UPLOAD_MAX_BYTES='26214400'
export UPSTREAM_NON_STREAM_MAX_RESPONSE_BYTES='67108864'
export STREAM_CAPTURE_MAX_BYTES='8388608'
export COMPACT_SERVER_ERROR_COOLDOWN_SECONDS='60'
```

启动后打开：

```bash
http://127.0.0.1:8000/
```

如果设置了 `SERVICE_API_KEYS`，前端右侧导入面板里填同一个 key 即可查看明细和导入。

## 导入 key

导入单个 token JSON：

```bash
curl -X POST http://127.0.0.1:8000/admin/tokens/import \
  -H "Authorization: Bearer $SERVICE_KEY" \
  -H "Content-Type: application/json" \
  --data @token_example.json
```

导入多个 token JSON 文件：

```bash
python import_tokens.py token_*.json
```

前端控制台里除了 JSON，也支持直接粘贴逐行格式。只有 refresh token 时可以每行一个：

```text
refresh_token_1
refresh_token_2
refresh_token_3
```

如果已经有账号 ID，也可以继续使用兼容格式：

```text
account_id_1,refresh_token_1
account_id_2,refresh_token_2
account_id_3,refresh_token_3
```

如果只有 JWT 格式的 access token，也可以直接每行粘贴一个：

```text
access_token_1
access_token_2
access_token_3
```

Go 版当前支持 access token 直接导入：导入接口会解码 JWT payload 中可用的账号、邮箱、套餐信息，并 upsert 到正式 `codex_tokens` 池，然后刷新内存 token snapshot。refresh token OAuth 验证接口已经预留，但 refresh worker 尚未启用；需要 refresh token 自动换 access token 的场景仍在 TODO 中。

导入去重规则：

- 不再按 `account_id` 或 `email` 合并
- 同一个工作空间下允许存在多个相同 `account_id`、但不同 `refresh_token` 的账号
- 系统会为每条 key 记录保存 RT 历史链；如果后续轮转出了 `rt2`、`rt3`，再次导入这些 RT 会命中同一条记录并判为重复
- 如果重复的是当前仍在使用的最新 RT，导入会视为“更新现有记录”
- 如果重复的是历史旧 RT，导入会直接跳过，不会把当前最新 RT 回滚成旧值
- 前端导入时可选择把本批新增/更新的 key 放到请求队列开头或最后，默认放到开头
- 批量导入 access token 时，Go 版使用短事务 upsert 正式池，并在导入后刷新内存 token 池快照
- 正常 `/v1/responses*` 请求从内存 token 池快照选择 key；DB 仅作为启动/快照重建来源，以及请求日志和 token 状态的异步持久化目标

## 接口

- `GET /healthz`: 查看可用 key 数量与状态
- `GET /admin/tokens`: 查看 key 列表与统计，支持 `limit` / `offset` 分页，以及 `q` / `status` / `plan_type` / `sort` / `import_batch_id` 查询
- `GET /admin/tokens/{id}`: 查看单个 key
- `POST /admin/tokens/{id}/activation`: 启用/禁用 key
- `POST /admin/tokens/{id}/remark`: 更新 key 备注
- `GET /admin/requests`: 查看请求次数汇总与最近请求日志
- `POST /admin/tokens/import`: 导入单个 key、key 数组，或 `{"tokens": [...], "import_queue_position": "front|back"}` 批量导入
- `GET /admin/tokens/import-batches`: 查看导入批次
- `GET /admin/tokens/import-jobs/{id}`: 查看导入 job
- `POST /admin/tokens/import-jobs/{id}/cancel`: 取消导入 job
- `GET /admin/settings` / `POST /admin/settings/{key}`: 查看/更新设置
- `POST /v1/responses`: 代理到上游 Codex responses
- `POST /v1/chat/completions`: 代理 chat completions
- `POST /v1/responses/compact`: 代理到上游 Codex responses compact
- `POST /v1/images/generations`: OpenAI 兼容图片生成，内部转到 Codex responses 的 `image_generation` tool
- `POST /v1/images/edits`: OpenAI 兼容图片编辑，支持 JSON 和 multipart form-data，内部转到 Codex responses 的 `image_generation` tool
- `GET /`: Web 控制台

## 网关行为

- 只会选择 `is_active=true` 且 `cooldown_until` 不在未来的 key
- `/v1/responses/compact` 会透传到上游 `/responses/compact`；上游不支持 `store` 参数，网关会先移除该字段，非流式调用时也不会自动补 `stream`
- 下游如果把 `/v1/responses` 当非流式调用，网关会自动把上游改成 `stream=true`，先在网关内收完整个 SSE，再拼成一个普通 JSON 响应返回
- `/v1/images/generations` 和 `/v1/images/edits` 默认把 `model` 当作图片工具模型处理；未指定时默认 `gpt-image-2`，内部主模型固定走 `gpt-5.5`
- 图片接口内部统一走上游 `/responses` 的 `image_generation` tool；非流式会在网关内收完整个 SSE 后再拼成 OpenAI Images API 形状，流式会把上游 responses 事件改写成 `image_generation.*` / `image_edit.*`
- 图片编辑或 `gpt-image-2` responses 请求的输入图片数超过 `IMAGE_INPUT_MAX_PER_REQUEST` 时，网关直接返回 `400`
- `gpt-image-2` 上游返回 `429 rate_limit_exceeded` 且命中 `input-images` 速率桶时，只会对当前 key 的图片桶做短冷却并自动重试下一个 key，不影响该 key 处理其他模型
- `/v1/responses/compact` 在上游 5xx 或传输错误时，会默认把当前 key 冷却 `60` 秒后切换下一把 key；可用 `COMPACT_SERVER_ERROR_COOLDOWN_SECONDS=0` 关闭
- 流式 `/v1/responses*` 会先预读开头的 SSE 状态事件；如果前缀已经是 `response.failed` / `type=error` / 不完整流 / 预读阶段网络错误，就不会先把坏流交给客户端，而是留在网关里切下一把 key
- `/admin/tokens/import` 只写 staging，默认最多 16 并发做 OAuth 验证；验证成功后按小批短事务发布到正式 key 池，并刷新内存 token 池快照
- 未验证或验证失败的 refresh token 不会进入正式池；正常请求最多只会感知到“新 key 稍后可用”，不会替导入任务试错
- 如果上游返回 `429` 且 `error.type=usage_limit_reached`，会按 `resets_in_seconds` 或 `resets_at` 冷却当前 key，然后自动重试下一个 key
- 如果上游返回 `402 {"detail":{"code":"deactivated_workspace"}}` 或 `401` 且 `error.code=account_deactivated`，会永久停用该 key
- 如果 refresh token 明确已经失效，例如 `refresh_token_reused` / `invalid_grant` / “Please try signing in again”，网关会清空 access token 并永久停用该 key
- 如果 key 是 access-token-only，网关不会刷新；JWT 过期或上游返回 `401/403` 时会清空 access token 并永久停用该 key
- 如果上游返回普通 `401/403`，会清空当前 access token，并尝试刷新/切换下一个 key
