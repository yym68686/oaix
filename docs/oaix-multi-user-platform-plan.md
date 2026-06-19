# oaix 多用户平台改造方案与 TODO

> 调研时间：2026-06-19
> 范围：当前 `oaix` Go 版后端与 COSS 风格前端。
> 目标：把现有单租户 key 池控制台改造为多用户平台。平台级 `Service API Key` 可以操作任何资源；普通用户只能创建和使用自己的 API Key、导入和管理自己的 ChatGPT/Codex 账号；管理员可以查看所有用户、所有账号池和全局使用情况。

## 当前结论

当前 oaix 仍是单租户控制台：

- `SERVICE_API_KEYS` 和 `admin_api_keys` 只表达“管理接口是否可信”，没有用户身份。
- `codex_tokens`、`token_import_jobs`、`token_import_items`、`gateway_request_logs`、`prompt_affinity_lanes`、`response_owner_bindings` 都没有 owner 维度。
- `/v1/*` 代理请求从全局 token pool 选 key，无法按调用用户隔离号池。
- 前端只有一个 Service API Key 输入框，所有页面默认是在同一全局管理视角下工作。
- 管理员“查看所有用户状态、全局号池总体情况”的数据模型和 API 还不存在。

因此多用户改造不能只在前端加页面，必须从认证上下文、数据库 owner、token pool、请求日志、导入任务、prompt cache namespace 全链路改造。

## 终局能力

### 角色模型

| 角色 | 来源 | 权限 |
| --- | --- | --- |
| Platform Service | `SERVICE_API_KEYS` 环境变量或 DB 中 `api_keys.kind = service` | 超级权限；可操作任何用户、任何账号、任何设置；可通过 `X-OAIX-Act-As-User` 代入某个用户执行。 |
| Admin | DB 用户 `role = admin` 的 API Key | 可查看和管理所有用户；可查看全局账号池、全局请求日志、全局统计；默认不直接暴露敏感 token 明文。 |
| User | DB 用户 `role = user` 的 API Key | 只能查看/管理自己的 API Key、自己的 ChatGPT/Codex 账号、自己的导入任务、自己的请求日志和统计。 |
| Readonly Admin | 可选 DB 用户 `role = readonly_admin` | 可看全局状态和用户状态，不可导入、删除、禁用、轮换 token。 |

### 关键术语

- **Platform Service API Key**：平台级超级 key，替代现在单租户 Service API Key 的全部能力，并保留兼容。
- **User API Key**：用户用于调用 oaix `/v1/*` 和管理自己资源的 key。
- **Account Token / ChatGPT 账号**：`codex_tokens` 中存的 refresh token / access token / account_id 等上游账号凭据。
- **Owner User**：资源所属用户。所有账号、导入批次、请求日志、prompt cache lane 都必须有 owner。
- **Act-As User**：只有 Platform Service 或 Admin 可以用 header 指定代入用户上下文，便于运维和客服排查。

## 后端总体设计

### 认证上下文

新增统一认证模型 `AuthContext`，所有 handler、proxy、store 查询都从这里拿权限和 owner。

```go
type AuthContext struct {
    PrincipalType string // service | user
    UserID        *int64
    APIKeyID      *int64
    Role          string // service | admin | readonly_admin | user
    Scopes        []string
    ActAsUserID   *int64
    IsService     bool
    IsAdmin       bool
    ReadOnly      bool
}
```

规则：

- `SERVICE_API_KEYS` 命中时生成 `Role=service`，`IsService=true`。
- DB API Key 命中时加载 `api_keys.user_id -> platform_users.role/status`。
- 用户状态为 `disabled`、`suspended`、`deleted` 时拒绝所有 mutating API 和 `/v1/*` 代理调用。
- User 请求任何列表或详情时自动追加 `owner_user_id = AuthContext.UserID`。
- Admin 请求默认可跨用户读取；mutating API 必须显式带 `user_id` 或路径中包含 owner，避免误操作全局资源。
- Service 请求可绕过 owner 限制；如带 `X-OAIX-Act-As-User` 则在代理和用户 API 中按该用户选择号池。

### 数据模型

#### 新表：platform_users

```sql
create table platform_users (
  id bigserial primary key,
  email varchar(320) unique,
  display_name varchar(128),
  role varchar(32) not null default 'user',
  status varchar(32) not null default 'active',
  plan varchar(32),
  notes text,
  created_by_user_id bigint references platform_users(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_seen_at timestamptz,
  disabled_at timestamptz
);
create index ix_platform_users_status_role on platform_users(status, role);
```

#### 新表：api_keys

`admin_api_keys` 当前只适合管理端，应替换为统一 `api_keys`。

```sql
create table api_keys (
  id bigserial primary key,
  user_id bigint references platform_users(id) on delete cascade,
  kind varchar(32) not null default 'user', -- service | user
  name varchar(128) not null,
  role varchar(32) not null default 'user',
  key_prefix varchar(24) not null,
  key_hash varchar(128) not null unique,
  scopes jsonb,
  last_used_at timestamptz,
  expires_at timestamptz,
  revoked_at timestamptz,
  created_by_user_id bigint references platform_users(id),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index ix_api_keys_user_active on api_keys(user_id, revoked_at, expires_at);
create index ix_api_keys_kind_role on api_keys(kind, role);
```

#### 现有表新增 owner 字段

必须新增并回填：

- `codex_tokens.owner_user_id bigint not null references platform_users(id)`
- `token_secrets.owner_user_id bigint not null`
- `token_scopes.owner_user_id bigint not null`
- `token_refresh_history.owner_user_id bigint not null`
- `codex_token_scoped_cooldowns.owner_user_id bigint not null`
- `token_runtime_state.owner_user_id bigint not null`
- `token_state_events.owner_user_id bigint not null`
- `token_import_jobs.owner_user_id bigint not null`
- `token_import_items.owner_user_id bigint not null`
- `gateway_request_logs.owner_user_id bigint`
- `gateway_request_logs.api_key_id bigint`
- `gateway_request_logs.token_owner_user_id bigint`
- `gateway_request_token_costs.owner_user_id bigint not null`
- `token_quota_snapshots.owner_user_id bigint not null`
- `openai_oauth_sessions.owner_user_id bigint not null`
- `admin_audit_logs.actor_user_id bigint`
- `admin_audit_logs.target_user_id bigint`
- `prompt_affinity_lanes.owner_user_id bigint not null`
- `response_owner_bindings.owner_user_id bigint not null`

建议索引：

```sql
create index ix_codex_tokens_owner_active_ready
  on codex_tokens(owner_user_id, is_active, cooldown_until, last_used_at, id)
  where merged_into_token_id is null;

create index ix_codex_tokens_owner_plan on codex_tokens(owner_user_id, plan_type);
create index ix_token_import_jobs_owner_submitted on token_import_jobs(owner_user_id, submitted_at desc);
create index ix_token_import_items_owner_job_status on token_import_items(owner_user_id, job_id, status);
create index ix_gateway_request_logs_owner_started on gateway_request_logs(owner_user_id, started_at desc);
create index ix_gateway_request_logs_owner_model_started on gateway_request_logs(owner_user_id, model_name, started_at desc);
create index ix_gateway_request_logs_owner_token_started on gateway_request_logs(owner_user_id, token_id, started_at desc);
create unique index ux_prompt_affinity_lanes_owner_hash on prompt_affinity_lanes(owner_user_id, prompt_key_hash);
create unique index ux_response_owner_bindings_owner_hash on response_owner_bindings(owner_user_id, response_id_hash);
```

#### 兼容迁移

创建一个 bootstrap platform 用户，把现有所有资源归属到该用户：

- `email = 'platform@oaix.local'`
- `role = 'admin'`
- `status = 'active'`
- 所有旧 `codex_tokens`、导入批次、日志、prompt cache 绑定先回填到这个用户。

迁移原则：

- 所有新增列先允许 null，分批回填，验证后再设 `not null`。
- 请求日志大表的 owner 回填必须批处理，不能在启动迁移中做大事务。
- 大索引用后台脚本或独立 migration job 创建，禁止塞进 gateway 启动路径。

### Token Pool 改造

当前 token pool 是全局快照。多用户后应变为按 owner 分片：

```go
type TokenPoolManager interface {
    Snapshot(ownerUserID int64) TokenSnapshot
    Acquire(ctx context.Context, ownerUserID int64, req AcquireRequest) (TokenLease, error)
    Refresh(ownerUserID int64) error
    RefreshAllActiveOwners(ctx context.Context) error
}
```

规则：

- User `/v1/*` 只能从自己的 `owner_user_id` 号池选 key。
- Admin/Service 如没有 `Act-As User`，默认不允许走代理，避免误用全局池；如果需要平台池，则明确使用 bootstrap/system owner。
- `active_stream_cap` 应支持用户级设置：默认来自全局设置，用户可被管理员覆盖。
- `token_runtime_state.active_streams` 仍按 token 维度，但查询和更新必须带 owner。
- `prompt_affinity_lanes`、`response_owner_bindings` 必须按 owner 隔离，避免不同用户的 `previous_response_id` 或 prompt cache 绑定串池。

### Store 层约束

禁止 handler 手写 owner 条件散落各处。所有 store 方法新增 scope 参数：

```go
type ResourceScope struct {
    OwnerUserID *int64
    AllowAll    bool
}
```

示例：

- `ListTokens(ctx, scope, opts)`
- `GetToken(ctx, scope, tokenID)`
- `DeleteToken(ctx, scope, tokenID)`
- `ListRequestLogsFiltered(ctx, scope, opts)`
- `CreateImportJob(ctx, scope, payloads)`

如果 `AllowAll=false` 且 `OwnerUserID=nil`，store 必须直接返回错误。

### API 设计

#### 身份与用户自助 API

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/api/me` | user/admin/service | 返回当前 principal、role、scopes、用户状态。 |
| `GET` | `/api/me/api-keys` | user | 查看自己的 API Key 列表，只返回 prefix，不返回明文。 |
| `POST` | `/api/me/api-keys` | user | 创建自己的 User API Key。 |
| `DELETE` | `/api/me/api-keys/{id}` | user | 撤销自己的 API Key。 |
| `GET` | `/api/me/usage` | user | 查看自己的请求量、成本、缓存率、成功率。 |
| `GET` | `/api/me/pool-summary` | user | 查看自己的号池摘要。 |

#### 用户自己的账号池 API

保留现有 `/admin/tokens` 兼容 Service API Key，同时新增更清晰的用户 API：

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/api/tokens` | user | 用户自己的 key 列表。 |
| `GET` | `/api/tokens/{id}` | user | 用户自己的 key 详情。 |
| `PATCH` | `/api/tokens/{id}` | user | 修改备注、启停、元数据。 |
| `DELETE` | `/api/tokens/{id}` | user | 删除自己的 key。 |
| `POST` | `/api/tokens/{id}/probe` | user | 测试自己的 key。 |
| `GET` | `/api/tokens/quota` | user | 查询自己的 key 额度。 |
| `POST` | `/api/tokens/quota-refresh` | user | 批量刷新自己的额度。 |
| `POST` | `/api/import/parse` | user | 服务端解析粘贴文本。 |
| `POST` | `/api/import/upload` | user | 上传文件解析。 |
| `POST` | `/api/import/jobs` | user | 创建自己的导入任务。 |
| `GET` | `/api/import/jobs` | user | 自己的导入批次列表。 |
| `GET` | `/api/import/jobs/{id}` | user | 自己的导入批次详情。 |
| `GET` | `/api/import/jobs/{id}/tokens` | user | 批次内自己的 key。 |
| `POST` | `/api/oauth/openai/sessions` | user | 创建自己的 ChatGPT OAuth 导入 session。 |

#### 管理员 API

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| `GET` | `/api/admin/users` | admin/service | 用户列表，支持状态、角色、搜索、排序、分页。 |
| `POST` | `/api/admin/users` | admin/service | 创建用户并可返回一次性初始 API Key。 |
| `GET` | `/api/admin/users/{id}` | admin/service | 用户详情。 |
| `PATCH` | `/api/admin/users/{id}` | admin/service | 修改状态、角色、备注、限额。 |
| `GET` | `/api/admin/users/{id}/api-keys` | admin/service | 查看用户 API Key 列表。 |
| `POST` | `/api/admin/users/{id}/api-keys` | admin/service | 为用户创建 API Key。 |
| `DELETE` | `/api/admin/users/{id}/api-keys/{key_id}` | admin/service | 撤销用户 API Key。 |
| `GET` | `/api/admin/users/{id}/tokens` | admin/service | 查看某用户账号池。 |
| `GET` | `/api/admin/users/{id}/requests` | admin/service | 查看某用户请求日志。 |
| `GET` | `/api/admin/users/{id}/usage` | admin/service | 查看某用户成本、缓存率、成功率。 |
| `GET` | `/api/admin/pool-summary` | admin/service | 全局号池总体情况。 |
| `GET` | `/api/admin/pool-summary/by-user` | admin/service | 按用户聚合号池状态。 |
| `GET` | `/api/admin/requests` | admin/service | 全局请求日志，默认必须分页。 |
| `GET` | `/api/admin/analytics/users` | admin/service | 用户维度用量排行。 |
| `GET` | `/api/admin/audit-logs` | admin/service | 审计日志。 |

#### `/v1/*` 代理 API

| 调用方 | 行为 |
| --- | --- |
| User API Key | 只使用该用户自己的账号池；日志写 `owner_user_id` 和 `api_key_id`。 |
| Admin API Key | 默认拒绝直接代理，除非带 `X-OAIX-Act-As-User`。 |
| Platform Service API Key | 可代理；如果带 `X-OAIX-Act-As-User` 用该用户池，否则用 platform/system 池。 |

所有 request log 必须记录：

- `owner_user_id`
- `api_key_id`
- `token_owner_user_id`
- `token_id`
- `request_id`
- `endpoint`
- `model_name`
- usage/cost/cache 字段

### 审计

新增审计事件：

- 用户创建、禁用、恢复、角色变更。
- API Key 创建、撤销、过期。
- 账号导入、删除、启停、备注、token secret 轮换。
- Admin act-as 用户执行代理请求。
- 全局设置变更、运维动作。

审计字段：

- `actor_user_id`
- `actor_api_key_id`
- `actor_role`
- `target_user_id`
- `target_type`
- `target_id`
- `action`
- `payload`
- `ip`
- `user_agent`
- `request_id`

### 安全边界

- 普通用户永远不能通过参数传 `owner_user_id` 越权。
- Admin 查询跨用户数据时必须走 `/api/admin/*`。
- 任何 secret 字段只允许写入，不允许明文读取；管理员也只能看到 prefix/hash/更新时间。
- API Key 明文只在创建时返回一次。
- Service API Key 的全局能力必须写入审计，尤其是 act-as。
- 所有 mutating API 支持 `Idempotency-Key`，避免用户重复提交导入任务。
- 所有列表默认分页，禁止默认全表 count；精确 count 需要显式 `include_total=true` 或异步统计。

## 前端设计

### 导航结构

当前侧边栏是：

- Key
- 导入
- 请求
- 设置
- 运行

多用户后改为分组导航：

```text
账号
  Key
  导入
  请求
  设置

用户
  我的账号
  我的 API Key

管理员（仅 admin/service 可见）
  用户状态
  号池总览
  全局请求
  审计日志
  运行
```

用户要求“管理员才能看到的页面，放在当前侧边栏用户页面下面”，对应实现为：`我的账号` / `我的 API Key` 之后显示 Admin 分组。普通用户不渲染 Admin 分组，直接访问 admin route 返回 403 页面。

### 前端鉴权状态

新增 `api.me()`：

```ts
type MeResponse = {
  principal_type: "service" | "user";
  user?: { id: number; email?: string; display_name?: string; role: string; status: string };
  role: "service" | "admin" | "readonly_admin" | "user";
  scopes: string[];
  capabilities: string[];
};
```

`App` 启动时不再只看 `service_key_protected`，而是：

1. 读取本地 API Key。
2. 调用 `/api/me`。
3. 根据 `role/capabilities` 决定导航项和默认首页。
4. 普通用户默认进入 `/keys?status=available`，只能看到自己的数据。
5. Admin 默认仍进入 `/keys`，但顶部可切换“全局 / 某用户”视角。

### 普通用户页面

#### 我的账号 `/account`

显示：

- 当前用户 email/display name/role/status。
- 用户自己的用量：请求数、成功率、缓存率、成本。
- 用户自己的号池摘要：总账号、有效、冷却、禁用、计划分布。
- 最近 24h 活跃状态。

#### 我的 API Key `/account/api-keys`

显示：

- API Key 列表：name、prefix、scopes、created_at、last_used_at、expires_at、revoked_at。
- 创建 API Key 弹窗。
- 撤销 API Key 确认框。
- 新 key 明文只显示一次，前端提示用户保存。

### 管理员页面

#### 用户状态 `/admin/users`

表格列：

- 用户 ID
- email / display name
- role / status
- API Key 数量
- 账号池：total / available / cooling / disabled
- 计划分布：free / plus / team / pro / unknown
- 请求量：24h requests、success rate、cache hit、cost
- last_seen_at
- 操作：查看、禁用、恢复、重置 API Key、查看号池

筛选：

- 搜索 email/name/id
- status
- role
- plan
- 有效账号数范围
- 24h 请求量范围
- 排序：最近活跃、账号数、请求量、成本、创建时间

#### 用户详情 `/admin/users/:id`

Tabs：

- 概览：状态、用量、账号池摘要。
- Key：该用户的账号池列表，可复用 `KeysPage` 的表格组件，所有 API 带 `user_id`。
- 导入：该用户导入批次。
- 请求：该用户请求日志。
- API Key：该用户 API Key 管理。
- 审计：该用户相关审计事件。

#### 号池总览 `/admin/pools`

卡片：

- 全平台账号总数、有效、冷却、禁用。
- 按用户聚合：Top users by active accounts / cooling / disabled / cost / requests。
- 按计划聚合：free / plus / team / pro / unknown。
- 异常账号：deactivated_workspace、401/403、usage_limit、quota fetch error。
- 最近导入任务状态。

表格：

- 用户维度号池列表，点击进入用户详情。
- 支持全局计划/状态/错误类型筛选。

#### 全局请求 `/admin/requests`

基于现有 `RequestsPage`，增加：

- 用户筛选。
- API Key prefix 筛选。
- token owner 筛选。
- request_id/detail drawer。
- 导出 CSV/JSON。

#### 审计日志 `/admin/audit`

显示：

- actor
- action
- target_user
- target_type / target_id
- request_id
- time
- payload 摘要

### 组件复用

需要把 `KeysPage`、`ImportsPage`、`RequestsPage` 拆成可复用的 resource-scoped 组件：

- `TokenTable`
- `TokenFilters`
- `ImportJobsTable`
- `RequestLogsTable`
- `PoolSummaryCards`
- `UserSelector`

这些组件接收 `scope`：

```ts
type ResourceScope = {
  mode: "self" | "admin-user" | "admin-all";
  userId?: number;
};
```

## 性能与鲁棒性要求

- 任何 admin 全局列表默认分页，不做全表精确 count。
- 用户维度聚合使用 `gateway_request_hourly_stats` 或新增 materialized summary，不直接扫 `gateway_request_logs`。
- token pool snapshot 按 active owner 懒加载，避免每 2 秒刷新所有用户。
- 活跃用户 owner set 从最近请求、最近导入、最近 token 变更里维护。
- 导入 worker 按 owner 限制并发，防止单用户大批导入拖垮全局。
- 请求日志写入队列必须记录 owner；写失败进 outbox，不阻塞 `/v1/*`。
- Service/Admin 跨用户操作必须有审计，不影响热路径。

## 测试矩阵

必须覆盖：

- User A 不能看到 User B 的 token/import/request/log。
- User A 不能用参数 `user_id=B` 越权。
- Admin 可以读取所有用户，但 readonly_admin 不能 mutate。
- Service API Key 可以操作所有资源，并可 act-as 用户。
- `/v1/responses` 使用调用者自己的号池。
- `previous_response_id` 和 prompt cache lane 不跨用户串池。
- User API Key 被 revoked 后立即不可用。
- 用户 disabled 后 `/v1/*` 和 mutating API 均拒绝。
- 旧 `SERVICE_API_KEYS` 兼容路径仍可管理旧资源。
- 旧资源迁移到 bootstrap 用户后，现有前端和线上流量不丢数据。

## 上线策略

1. 先落地数据模型和 AuthContext，但默认所有旧资源归属 bootstrap 用户。
2. 改 Store owner scope 和测试，确保单用户兼容。
3. 改 proxy token pool owner 分片，确保 `/v1/*` 不串池。
4. 新增用户/API Key 管理 API。
5. 前端接入 `/api/me`，普通用户与管理员导航分流。
6. 增加 admin-only 用户状态与号池总览页面。
7. 开启用户注册/创建流程。
8. 最后收紧权限，禁止普通用户访问旧 `/admin/*` 全局接口。

## TODO

### 1. 数据模型

- [x] 新增 `platform_users` 表。
- [x] 新增统一 `api_keys` 表。
- [x] 设计并实现从 `admin_api_keys` 到 `api_keys` 的迁移。
- [x] 新增 bootstrap platform 用户。
- [x] 给 `codex_tokens` 增加 `owner_user_id`。
- [x] 给 `token_secrets` 增加 `owner_user_id`。
- [x] 给 `token_scopes` 增加 `owner_user_id`。
- [x] 给 `token_refresh_history` 增加 `owner_user_id`。
- [x] 给 `codex_token_scoped_cooldowns` 增加 `owner_user_id`。
- [x] 给 `token_runtime_state` 增加 `owner_user_id`。
- [x] 给 `token_state_events` 增加 `owner_user_id`。
- [x] 给 `token_import_jobs` 增加 `owner_user_id`。
- [x] 给 `token_import_items` 增加 `owner_user_id`。
- [x] 给 `openai_oauth_sessions` 增加 `owner_user_id`。
- [x] 给 `token_quota_snapshots` 增加 `owner_user_id`。
- [x] 给 `gateway_request_logs` 增加 `owner_user_id`、`api_key_id`、`token_owner_user_id`。
- [x] 给 `gateway_request_token_costs` 增加 `owner_user_id`。
- [x] 给 `prompt_affinity_lanes` 增加 `owner_user_id` 并调整唯一约束。
- [x] 给 `response_owner_bindings` 增加 `owner_user_id` 并调整唯一约束。
- [x] 给 `admin_audit_logs` 增加 `actor_user_id`、`actor_api_key_id`、`target_user_id`、`ip`、`user_agent`。
- [x] 编写旧数据回填脚本，把现有资源归属 bootstrap 用户。
- [x] 为 owner scoped 查询增加必要索引。
- [x] 大表索引和 request log 回填拆成独立 job，禁止放进 gateway 启动迁移。

### 2. 鉴权与权限

- [x] 新增 `AuthContext` 类型。
- [x] 重构 `requireAuth`，统一识别 Service API Key、DB API Key、User API Key。
- [x] 新增 API Key hash/prefix 生成与校验工具。
- [x] 新增用户状态校验：disabled/suspended/deleted 阻断请求。
- [x] 新增 role/scopes 校验中间件。
- [x] 新增 `X-OAIX-Act-As-User` 支持，仅 service/admin 可用。
- [x] 新增 request scoped `AuthContext` 注入。
- [x] 所有 mutating API 写审计。
- [x] API Key last_used_at 异步更新，避免阻塞热路径。
- [x] 被 revoke 的 API Key 立即失效。

### 3. Store 层 owner scope

- [x] 新增 `ResourceScope`。
- [x] `ListTokens` 增加 scope 参数。
- [x] `GetToken` 增加 scope 参数。
- [x] `UpdateToken` 增加 scope 参数。
- [x] `DeleteToken` 增加 scope 参数。
- [x] `TokenCounts` 增加 scope 参数。
- [x] `TokenPlanCounts` 增加 scope 参数。
- [x] `ListImportJobs` 增加 scope 参数。
- [x] `GetImportJob` 增加 scope 参数。
- [x] `ListImportItems` 增加 scope 参数。
- [x] `CreateImportJob` 写入 owner。
- [x] `RetryImportJob` / `RetryImportItem` / `SkipImportItem` 增加 scope 校验。
- [x] `ListRequestLogsFiltered` 增加 scope 参数。
- [x] `RequestAnalytics` / `CacheAnalytics` / `ErrorAnalytics` / `LatencyAnalytics` 增加 scope 参数。
- [x] `CostAggregatesByToken` / `CostAggregatesByAccount` 增加 scope 参数。
- [x] prompt cache store 方法增加 owner scope。
- [x] response owner binding store 方法增加 owner scope。
- [x] settings 区分 global settings 与 user settings。

### 4. Token pool 与代理热路径

- [x] 把全局 token pool 改为 `TokenPoolManager`。
- [x] 按 `owner_user_id` 懒加载 token snapshot。
- [x] `Acquire` 必须接收 owner。
- [x] `/v1/responses` 根据 AuthContext 选择 owner 池。
- [x] `/v1/chat/completions` 根据 AuthContext 选择 owner 池。
- [x] `/v1/images/generations` 根据 AuthContext 选择 owner 池。
- [x] `/v1/images/edits` 根据 AuthContext 选择 owner 池。
- [x] `previous_response_id` owner binding 加 owner namespace。
- [x] prompt cache affinity lane 加 owner namespace。
- [x] 请求日志写入 owner/api_key/token_owner。
- [x] token cooldown 和 runtime active_streams 按 token+owner 安全更新。
- [x] active owner set 只刷新最近活跃用户，避免全量轮询。
- [x] Admin/Service 不带 act-as 时代理行为明确化并测试。

### 5. 用户 API

- [x] `GET /api/me`。
- [x] `GET /api/me/api-keys`。
- [x] `POST /api/me/api-keys`。
- [x] `DELETE /api/me/api-keys/{id}`。
- [x] `GET /api/me/usage`。
- [x] `GET /api/me/pool-summary`。
- [x] `GET /api/tokens`。
- [x] `GET /api/tokens/{id}`。
- [x] `PATCH /api/tokens/{id}`。
- [x] `DELETE /api/tokens/{id}`。
- [x] `POST /api/tokens/{id}/probe`。
- [x] `GET /api/tokens/quota`。
- [x] `POST /api/tokens/quota-refresh`。
- [x] `POST /api/import/parse`。
- [x] `POST /api/import/upload`。
- [x] `POST /api/import/jobs`。
- [x] `GET /api/import/jobs`。
- [x] `GET /api/import/jobs/{id}`。
- [x] `GET /api/import/jobs/{id}/tokens`。
- [x] `GET /api/import/jobs/{id}/items`。
- [x] `POST /api/oauth/openai/sessions`。
- [x] `GET /api/oauth/openai/sessions/{id}`。
- [x] `POST /api/oauth/openai/sessions/{id}/exchange`。

### 6. 管理员 API

- [x] `GET /api/admin/users`。
- [x] `POST /api/admin/users`。
- [x] `GET /api/admin/users/{id}`。
- [x] `PATCH /api/admin/users/{id}`。
- [x] `GET /api/admin/users/{id}/api-keys`。
- [x] `POST /api/admin/users/{id}/api-keys`。
- [x] `DELETE /api/admin/users/{id}/api-keys/{key_id}`。
- [x] `GET /api/admin/users/{id}/tokens`。
- [x] `GET /api/admin/users/{id}/import/jobs`。
- [x] `GET /api/admin/users/{id}/requests`。
- [x] `GET /api/admin/users/{id}/usage`。
- [x] `GET /api/admin/pool-summary`。
- [x] `GET /api/admin/pool-summary/by-user`。
- [x] `GET /api/admin/analytics/users`。
- [x] `GET /api/admin/requests`。
- [x] `GET /api/admin/audit-logs`。
- [x] `GET /api/admin/audit-logs/{id}`。
- [x] 保留旧 `/admin/*` 兼容 Service API Key，并明确普通 user 不可访问全局视角。

### 7. 前端鉴权和导航

- [x] 新增 `api.me()`。
- [x] 新增 `MeResponse` 类型。
- [x] App 启动先调用 `/api/me` 判断角色。
- [x] Service API Key 输入框文案改为 “API Key”，同时兼容 Service/User Key。
- [x] 侧边栏拆为账号、用户、管理员分组。
- [x] 普通用户隐藏管理员分组。
- [x] Admin/Service 显示管理员分组。
- [x] 直接访问无权限 route 显示 403 页面。
- [x] `RouteKey` 增加 `account`、`account_api_keys`、`admin_users`、`admin_user_detail`、`admin_pools`、`admin_requests`、`admin_audit`。
- [x] `router.ts` 增加上述路由解析。

### 8. 前端普通用户页面

- [x] 新增 `/account` 我的账号页面。
- [x] 展示用户身份、状态、角色。
- [x] 展示自己的 24h 请求量、成功率、缓存率、成本。
- [x] 展示自己的号池摘要和计划分布。
- [x] 新增 `/account/api-keys` 页面。
- [x] 用户可创建自己的 API Key。
- [x] 用户可撤销自己的 API Key。
- [x] API Key 明文只显示一次并提供复制按钮。
- [x] 用户 Key/导入/请求页面切到 `/api/*` 自助 API。

### 9. 前端管理员页面

- [x] 新增 `/admin/users` 用户状态页面。
- [x] 用户列表支持搜索、状态、角色、计划、活跃度筛选。
- [x] 用户列表显示账号池、请求量、成功率、缓存率、成本。
- [x] 新增 `/admin/users/:id` 用户详情页面。
- [x] 用户详情包含概览、Key、导入、请求、API Key、审计 tabs。
- [x] 新增 `/admin/pools` 号池总览页面。
- [x] 号池总览显示全局账号总数、有效、冷却、禁用、计划分布、异常类型。
- [x] 号池总览按用户聚合展示 Top users。
- [x] 新增 `/admin/requests` 全局请求页。
- [x] 全局请求页支持用户筛选、API Key 筛选、导出。
- [x] 新增 `/admin/audit` 审计日志页。
- [x] 管理员页面全部使用 COSS 现有组件风格。

### 10. 组件重构

- [x] 抽出 `TokenTable`。
- [x] 抽出 `TokenFilters`。
- [x] 抽出 `ImportJobsTable`。
- [x] 抽出 `RequestLogsTable`。
- [x] 抽出 `PoolSummaryCards`。
- [x] 抽出 `UserSelector`。
- [x] 抽出 `ApiKeyTable`。
- [x] 所有表格组件支持 `ResourceScope`。
- [x] 表格默认分页，避免前端请求全量。
- [x] 批量操作组件按权限隐藏或禁用。

### 11. 观测与聚合

- [x] request log 写入 owner/user/api_key 字段。
- [x] hourly stats 增加 owner 维度。
- [x] token cost aggregate 增加 owner 维度。
- [x] admin 用户列表用聚合表，不直接扫请求日志大表。
- [x] 增加用户维度缓存率统计。
- [x] 增加用户维度成功率统计。
- [x] 增加用户维度成本统计。
- [x] 增加用户维度账号池异常统计。
- [x] Fugue/日志中输出低基数 owner/user 诊断字段时避免泄露 email。

### 12. 测试

- [x] Store 层 owner scope 单元测试。
- [x] AuthContext 解析测试。
- [x] Service API Key 超级权限测试。
- [x] User API Key 隔离测试。
- [x] Admin 读取全局资源测试。
- [x] Readonly Admin 禁止 mutate 测试。
- [x] User A 不能读 User B token 测试。
- [x] User A 不能读 User B import job 测试。
- [x] User A 不能读 User B request log 测试。
- [x] User A 不能通过 query/path `user_id` 越权测试。
- [x] `/v1/responses` 按用户池选 key 测试。
- [x] `/v1/images/*` 按用户池选 key 测试。
- [x] prompt cache lane 不跨用户测试。
- [x] response owner binding 不跨用户测试。
- [x] API Key revoke 立即失效测试。
- [x] 用户 disabled 后代理和 mutate 均拒绝测试。
- [x] 前端普通用户不显示管理员导航测试。
- [x] 前端管理员可以看到用户状态和号池总览测试。
- [x] OpenAPI 契约测试。

### 13. 文档与运维

- [x] 更新 README 的鉴权模型。
- [x] 更新 `docs/api/admin-api.md`，区分 `/api/*` 与 `/api/admin/*`。
- [x] 更新 OpenAPI JSON。
- [x] 增加多用户迁移 runbook。
- [x] 增加 bootstrap user 创建 runbook。
- [x] 增加 API Key 轮换 runbook。
- [x] 增加用户禁用/恢复 runbook。
- [x] 增加故障排查：用户看不到账号、请求没有可用 key、导入卡住、缓存率异常。
- [x] 上线前准备 SQL 回滚脚本。
- [x] 上线后核对 owner 回填数量与旧数据数量一致。

## 验收标准

- Platform Service API Key 可查看和管理全部用户、全部账号池、全部导入任务、全部请求日志。
- 普通用户只能查看和管理自己的 API Key、自己的 ChatGPT/Codex 账号、自己的导入任务、自己的请求日志。
- 管理员前端能看到用户状态页面和号池总览页面，普通用户完全看不到这些入口。
- `/v1/*` 代理请求按调用用户选择该用户自己的号池，不跨用户借 key。
- 请求日志、成本、缓存率、成功率都能按用户聚合。
- 所有 secret 不在 API 响应中明文返回。
- 所有跨用户管理操作有审计日志。
- 现有单租户数据迁移后不丢失，归属 bootstrap 用户。
- 现有 Service API Key 仍可兼容旧自动化调用。
