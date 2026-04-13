# oaix-gateway

从 `oai-x` 单独拆出的精简项目，只保留这些能力：

- key 池持久化与状态维护
- 请求前选择可用 key
- refresh token 换 access token
- 429 配额冷却
- 401/403 失效处理
- `/v1/responses` 端点网关代理
- 基于 RT 历史链的 key 去重

明确不包含原项目的这些部分：

- 注册 OpenAI/Codex 账号
- 邮箱供应商接入
- Sentinel/注册补号流程
- 自动补号脚本和相关后台任务

## 目录

- `oaix_gateway/database.py`: PostgreSQL 模型与连接初始化
- `oaix_gateway/token_store.py`: key 导入、选择、冷却、失败/成功状态更新
- `oaix_gateway/oauth.py`: access token 刷新、缓存、并发锁
- `oaix_gateway/api_server.py`: FastAPI 网关与管理接口
- `import_tokens.py`: 从本地 `token_*.json` 批量导入

## 环境变量

- `DATABASE_URL`: PostgreSQL DSN，默认 `postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/oaix_gateway`
- `SERVICE_API_KEYS`: 服务端鉴权 key，多个用逗号分隔；未设置时不鉴权
- `CODEX_BASE_URL`: 上游 Codex responses 地址。默认 `https://chatgpt.com/backend-api/codex/responses`
- `MAX_REQUEST_ACCOUNT_RETRIES`: 单次请求最多切换多少个 key，默认 `100`
- `DEFAULT_USAGE_LIMIT_COOLDOWN_SECONDS`: 429 且没有明确重置时间时的默认冷却秒数，默认 `300`
- `IMPORT_RESPONSE_IDLE_GRACE_SECONDS`: 导入 key 遇到活跃 `/v1/responses*` 流量时，等流量清空后再额外静默多久继续补号，默认 `0.25`
- `COMPACT_SERVER_ERROR_COOLDOWN_SECONDS`: `/v1/responses/compact` 遇到上游 5xx / 传输错误时的冷却秒数，默认 `60`；设为 `0` 可关闭
- `HOST`: 内置启动命令监听地址，默认 `0.0.0.0`
- `PORT`: 内置启动命令端口，默认 `8000`

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 启动

```bash
uvicorn oaix_gateway.api_server:app --host 0.0.0.0 --port 8000
```

或：

```bash
python -m oaix_gateway
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
- 最近 key 状态列表
- 导入 key 的表单和 JSON 文件导入入口

## Docker Compose

项目根目录已经带了 `docker-compose.yml` 和 `Dockerfile`，可以直接启动 PostgreSQL 和网关：

```bash
docker compose up -d --build
```

默认行为：

- `postgres`: PostgreSQL 16
- `gateway`: Web 控制台 + `/v1/responses` 网关

常用环境变量：

```bash
export GATEWAY_PORT='8000'
export POSTGRES_DB='oaix_gateway'
export POSTGRES_USER='oaix'
export POSTGRES_PASSWORD='oaix_password'
export SERVICE_API_KEYS='change-me'
export CODEX_BASE_URL=''
export IMPORT_RESPONSE_IDLE_GRACE_SECONDS='0.25'
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

前端控制台里除了 JSON，也支持直接粘贴这种逐行格式：

```text
account_id_1,refresh_token_1
account_id_2,refresh_token_2
account_id_3,refresh_token_3
```

也就是每行一个 `account_id,refresh_token`，粘贴后会自动解析成导入批次。

导入去重规则：

- 不再按 `account_id` 或 `email` 合并
- 同一个工作空间下允许存在多个相同 `account_id`、但不同 `refresh_token` 的账号
- 系统会为每条 key 记录保存 RT 历史链；如果后续轮转出了 `rt2`、`rt3`，再次导入这些 RT 会命中同一条记录并判为重复
- 如果重复的是当前仍在使用的最新 RT，导入会视为“更新现有记录”
- 如果重复的是历史旧 RT，导入会直接跳过，不会把当前最新 RT 回滚成旧值
- 批量导入时，如果同时有活跃的 `/v1/responses*` 请求，导入会在每条记录前主动让路，尽量把代理请求放在前面

## 接口

- `GET /healthz`: 查看可用 key 数量与状态
- `GET /admin/tokens`: 查看 key 列表与统计
- `GET /admin/requests`: 查看请求次数汇总与最近请求日志
- `POST /admin/tokens/import`: 导入单个 key、key 数组，或 `{"tokens": [...]}` 批量导入
- `POST /v1/responses`: 代理到上游 Codex responses
- `POST /v1/responses/compact`: 代理到上游 Codex responses compact
- `GET /`: Web 控制台

## 网关行为

- 只会选择 `is_active=true` 且 `cooldown_until` 不在未来的 key
- `/v1/responses/compact` 会透传到上游 `/responses/compact`，并按 `uni-api` 的相关逻辑去掉 `store`；非流式调用时也不会自动补 `stream`
- 下游如果把 `/v1/responses` 当非流式调用，网关会自动把上游改成 `stream=true`，先在网关内收完整个 SSE，再拼成一个普通 JSON 响应返回
- `/v1/responses/compact` 在上游 5xx 或传输错误时，会默认把当前 key 冷却 `60` 秒后切换下一把 key；可用 `COMPACT_SERVER_ERROR_COOLDOWN_SECONDS=0` 关闭
- 流式 `/v1/responses*` 会先预读开头的 SSE 状态事件；如果前缀已经是 `response.failed` / `type=error` / 不完整流 / 预读阶段网络错误，就不会先把坏流交给客户端，而是留在网关里切下一把 key
- `/admin/tokens/import` 是低优先级导入；检测到活跃 `/v1/responses*` 流量时，会逐条暂停导入，让代理请求先占用数据库和事件循环
- 如果上游返回 `429` 且 `error.type=usage_limit_reached`，会按 `resets_in_seconds` 或 `resets_at` 冷却当前 key，然后自动重试下一个 key
- 如果上游返回 `402 {"detail":{"code":"deactivated_workspace"}}` 或 `401` 且 `error.code=account_deactivated`，会永久停用该 key
- 如果上游返回普通 `401/403`，会清空当前 access token，并尝试刷新/切换下一个 key
