# 管理员用户状态页偶发超时：调查与可观测性方案

状态：2026-09-09，仅方案，未修改应用代码、数据库或生产配置，未发布新版本。调查基线为 `71049f3e56adbc911909c8493ee8ef70f5594b83`。用户明确要求：无法完全确定根因时，不修改代码，转向系统性可观测性方案。

## 已证实的故障链

用户报告北京时间约 17:40 首次打开用户状态页出现 `load token owner costs: timeout: context deadline exceeded`，随后自动恢复。节点轮转日志实际记录的失败结束时间为 **2026-09-09 17:42:50 CST / 09:42:50 UTC**。

| 同一失败请求的阶段 | 实际耗时或结果 |
| --- | --- |
| `GET /api/admin/pool-summary/by-user` | 30,001ms，HTTP 503 |
| `addObservedCostsByOwnerLogs(..., pendingOnly=true)` | 29,713ms，context deadline exceeded |
| token 累计成本表聚合 | 286ms，成功 |
| token owner 用量 | 228ms，成功 |
| Sub2API 成本 | 183ms，成功 |
| token pool 汇总 | 1ms，成功 |

请求关联 ID 为 `ce97d77dc8aabb43f350f42ee79b3a4d`，pending SQL 的现有指纹为 `c538ffc21736`。PostgreSQL 同时记录该完整 SQL 被客户端取消，SQLSTATE `57014`；随后出现 Broken pipe / connection lost。这些连接错误在请求取消后发生，不能倒置成超时的原因。

紧接着请求 `bd20c51986c1167286315f2ab353771d` 总耗时 10,338ms、HTTP 200，同一 pending SQL 为 9,941ms。后续只读接口检查恢复到亚秒级。这个时间序列与用户描述一致，但不能单凭“第一次慢，之后快”证明冷缓存。

源码中的传播路径可完整解释为什么页面不显示用户：

1. `internal/store/request_admin.go` 的 `TokenObservedCostsByOwner` 读已完成的 reconciliation 标记，执行累计表聚合，再查询所有 finished、未聚合且有成本的日志。
2. `internal/httpapi/admin_user_api.go` 的 `loadPlatformPoolSummaryData` 将这项错误包装为 `load token owner costs`，整个 pool-summary handler 返回 503。包含 usage 的 handler deadline 为 30 秒。
3. `frontend/src/features/admin/AdminPages.tsx` 将用户列表和 pool-summary 放进 `Promise.all`，在两项都成功后才调用 `setUsers`。因此即使独立用户列表成功，summary 失败也会阻止首次渲染用户。初始 `users=[]` 同时满足“暂无用户”空态条件；它不能证明数据库没有用户。

因此，**超时发生在哪条 SQL、如何传播到空页面已经确定；该 SQL 当时为什么需要近 30 秒仍未确定**。不能将准确定位故障阶段等同于已经证明完整根因。

## 已核对的事实与证据边界

- 生产运行的源码与本地基线一致；应用和 PostgreSQL 均 Ready，当前应用容器 restart_count=0。
- 真正的标记键 `request_token_costs_go_recorded_reconciled_at` 为 completed=true。调查中曾误查一个不存在的键并怀疑全历史 fallback，已撤销该判断。数据库取消日志明确包含 `analytics_recorded_at is null`。
- 失败期间的进程级 pool acquire duration / empty acquire / canceled acquire 累计差值为零，查询 tracer 测到 29,713ms 的执行生命周期。现有 pool 差值是全进程计数，不能把 679 次 acquire 写成该请求执行了 679 次查询；该请求实际记录了 9 次 SQL。
- 事后 `EXPLAIN ANALYZE` 的 pending SQL 使用 `ix_gateway_request_logs_analytics_pending`，约 2.092ms、904 个 shared buffer hit，且观察到 hint/pruning 等可能产生的 dirty/WAL 数据。这个事后计划和 buffer 统计不能反推失败时计划或磁盘读量，也不能解释当时 29 秒全部耗时。
- 请求日志表约 833 万存活行、约 91.9 万 dead tuples；pending 部分索引约 28.4MB。全表 dead tuple 估计值不是该索引精确死项数量，更不是历史故障的因果证明。
- `track_io_timing=off`、`log_lock_waits=off`、`log_min_duration_statement=-1`；事故 PG 日志 query_id=0；`pg_stat_statements` 未安装。没有取得事故时的 plan、backend 等待序列或与该 backend 关联的 CPU/I/O 时间序列。
- Fugue logs 和该路由 request facts 的事故窗口查询为空，接口却报告 source available。节点原始日志存在该请求，说明“查询为空”不能作为没有事故的证据；具体采集、路由覆盖或导出原因仍需检查。
- `gateway_current_token_costs` 已覆盖全部 15,902 个 token，但与旧累计成本存在历史数值差异。直接切换不能视为纯性能改动，本次未实施。
- analytics 队列 backfill 标记为 completed=true。同快照比较旧 pending 集合和队列集合没有差异，但该样本两者均为空，证明力度有限。队列驱动查询是后续候选，不是已经证明可修复该事故的改动。

没有执行 cache flush、生产压测、重启、索引重建、vacuum、回填、标记修补、资源调整或查询超时调整。正常只读 SQL 可能产生 PostgreSQL 内部 hint/pruning/WAL，不能承诺调查对物理缓存完全无影响。

## 目标：让下一次异常可归因

建立同一次页面加载、HTTP 请求、summary 阶段、SQL 执行、PG backend 和节点资源的关联。采集只提供事实，不参与 serving 配置决策，不改变业务成本、超时、重试或返回数据。所有时间同时保留 UTC 与本进程单调耗时；跨机器先检查时钟偏差。

### 1. 页面与请求关联

- `requestJSON` 保留响应 `X-Request-ID`、status、route template 和开始/结束时间，在错误详情中提供可复制的诊断编号。网络错误没有响应 ID 时标为缺失。
- 每次 load 使用独立 page-load ID，分别记录 users 和 summary 的结果。只记录过滤器是否启用、limit、offset、hours；不记录搜索文本、邮箱、密码或凭证。
- 记录请求完成与页面是否采纳结果，区分首次失败、后台刷新失败、过期响应和自动恢复。空数据与加载失败应在后续独立 UI 修复中明确区分；本次不改 Promise.all 或显示策略。

### 2. Summary 与数据库阶段

在 `loadPlatformPoolSummaryData` 及成本 reader 分别记录：user-list、pool、usage、cost-reconcile-marker、cost-aggregate、cost-pending、sub2api-cost、response。每项含开始、结束、耗时、剩余 deadline、固定错误类别和完成状态；保存第一项实际失败，其他 sibling 的 cancellation 单独标为派生取消。

每次 SQL 分配 query-execution ID，附带 route、stage、逻辑 query-name、源码 revision、实例 epoch、request ID。分别记录 pool acquire、query start、first row、rows close/end 和总耗时。利用 pgx 的 acquire/query hooks 在同一 context 下记录，不能用全进程 pool 累计差值冒充单请求等待。保留当前全局 gauges 作为资源背景。

当前 fingerprint 对截断到 240 字符后的 SQL 形状取 hash，尾部过滤条件不同的 SQL 可能共享指纹。新契约应使用稳定逻辑 query-name，并对完整、正确去字面量后的规范形状取 hash；展示片段可继续限长。不采集 bind 值，也不输出任意 PG 错误 detail。记录 rows.Err 与 context cancel 的类型和时序。

### 3. PG backend 等待与节点资源

在既有 pgx 连接上读取已经持有的 backend PID，结合连接 epoch、数据库实例标识与 backend_start 避免 PID 复用。不要为每条业务 SQL 再执行一次 `pg_backend_pid()`。

使用独立、只读、有界的诊断连接和单 worker：仅对已登记且超过 500ms 的管理员查询采样，建议间隔 250ms、最多 8 个活跃目标、每个目标最多 120 条样本、单次查询预算 100ms；这些是待隔离 benchmark 验证的初始预算。连接不可用即丢弃并计数，不挤占业务池、不阻塞或取消目标查询。

采样 `pg_stat_activity` 的 state、query_start、wait_event_type、wait_event、query_id，以及必要时的 blocker PID/锁类型。只查询已登记 PID，不导出其他用户 SQL。保存首次到末次采样覆盖率；采样空白标为 unknown。wait_event 只能说明采样瞬间的等待，不能单独证明整段查询耗时分布。[PostgreSQL 活动与统计文档](https://www.postgresql.org/docs/18/monitoring-stats.html)

由节点运维采集器记录同窗口的 PostgreSQL cgroup CPU usage、quota/throttled 时间、memory events、I/O 与 PSI；后端进程 CPU/I/O 数据需正确映射容器 PID namespace。全节点压力只能提供背景，不能自动归因到某个 SQL。

### 4. 执行计划与 I/O 证据

针对管理员会话评估 `auto_explain` 的限量采样；先在隔离环境证明可加载、权限边界和开销。建议 JSON、参数值禁记、`log_timing=off`，按需采集 analyze/buffers/settings。`log_analyze` 会给执行本身增加开销，关闭逐节点 timing 可减少其一部分成本，不能未经测量全库开启。auto_explain 的原始 SQL/计划也可能含字面量，必须限制到参数化的允许列表查询并验证脱敏。[PostgreSQL auto_explain 文档](https://www.postgresql.org/docs/18/auto-explain.html)

`track_io_timing` 需先测量平台计时开销，再决定诊断会话的启用范围；现有事故没有该历史数据。[PostgreSQL 运行时统计配置](https://www.postgresql.org/docs/18/runtime-config-statistics.html)

取消、连接中断或进程退出可能来不及产生完整的执行计划，验收时必须测试这些路径并明确覆盖率，不能承诺 auto_explain 一定能记录超时查询。等待采样在结束前启动，作为补充。事后 EXPLAIN 只作为新实验，禁止自动重放生产慢查询来冒充原始计划；禁止为模拟冷缓存清理生产缓存。

### 5. 诊断完整性、保留与配置恢复

- 运维记录使用有界异步队列。初始预算：每进程最多 256 条待导出记录、每条最多 32KiB、每分钟最多 60 个慢/失败请求包；1% 正常管理员请求作为基线。精确预算由隔离性能验收确定。
- 记录 selected、recorded、exported、dropped、truncated、store/export errors、oldest queued age、last accepted/last queryable time。按已知 request ID 验证 Fugue 查询链，不能仅看 source available。
- stdout 保留短摘要；有界本地 spool 与 Fugue 运维存储负责有限期细节。建议默认 24h、单进程 spool 上限 64MiB，触顶淘汰并计数。事故证据可手动导出。账单和业务请求日志不依赖运维数据，也不为本方案修改业务日志大表。
- 观测策略与代码发布分离，默认关闭，通过独立管理接口持久化 enabled、route allowlist、采样和预算。观测组件挂掉时仍能经既有管理/控制面恢复策略；不将观测开关写死在新代码里。无有效策略时停止新采集；配置读取失败不改变流量/DNS artifact 或 positive LKG。
- label 只用 route template、stage、outcome 等有限枚举。request/query ID、PID、主机和用户标识只进受权限控制的诊断记录。凭证、DSN、Authorization、Cookie、搜索词、正文、API key 值全部排除。

## 验收与后续修复门槛

在隔离 PostgreSQL 与本地 HTTP fixture 中分别构造：pool 耗尽、锁等待、冷/热读取、错误计划、CPU 配额限制、结果读取变慢、客户端取消、handler deadline、sibling 取消、PID 复用和自动恢复。它们可能产生相同 deadline 字符串；验收要求新证据能区分实际阶段及已测到的资源事实。

另外覆盖：日志截断指纹冲突、数据脱敏诱饵、采集器超时、队列满、导出停机、策略开关、配置读取失败和旧新实例共存。比较观测开关前后的 HTTP 状态/正文、成本、数据库写入、重试/冷却、在途排空和配置摘要，保证采集失败不会改变业务结果。执行相关 Go、race、DB 集成测试与性能测量后再评审。

后续实施顺序为：观测契约和隔离测试 → 默认关闭的代码 → 少量管理员自然流量启用 → 验证完整证据可查询 → 取得下一次异常的原始样本 → 按样本证明根因并独立修复。出现观测开销回归立即用配置关闭新采集，无需切换 serving artifact 或重启数据库。

候选优化应保持为独立提案：队列驱动 pending 查询须验证回填未完成、缺失队列项、并发 drain、late update、canonical merge、无 token 的 owner 成本及历史保留语义；切换 current cost 表需先解释全部成本差异。不能把候选更快、故障不再出现或页面恢复当成根因已经证实。

## 本次交付

- [x] 关联用户报告、OAIX 原始请求/慢查询日志和 PostgreSQL 完整取消 SQL。
- [x] 明确已知阶段、未知原因、事后探针及候选查询的证据边界。
- [x] 保存受限的本地事故证据并编写本方案。
- [ ] 实施新的观测代码或变更生产采集策略（本次未执行）。
- [ ] 完整证明历史超时根因、发布修复（本次未满足门槛）。
