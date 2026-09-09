# 管理员查询观测运行说明

本功能为管理员用户状态页的偶发超时保留逐次证据，不改变成本 SQL、费用口径、handler deadline、业务请求重试、token 选择或前端列表加载策略。历史事件已定位到 pending 成本 SQL，但其当时变慢的数据库内部原因尚未证明。

## 范围与开关

默认关闭。仅跟踪 GET `/api/admin/users`、`/api/admin/pool-summary/by-user` 和 `/api/admin/analytics/users`。所有普通推理请求保持原有观测行为，不注册新等待采样目标。

- `GET /admin/query-observability`：持久化策略、当前实例策略、队列和导出状态、能力边界。
- `POST /admin/query-observability`：更新策略并立即在当前实例生效；其他实例每15秒重新读取。只读管理员不可修改；普通用户不可读诊断。
- `GET /admin/query-observations?request_id=<id>`：读取最多100条独立观察记录。一个复用的 request ID 可以关联多条独立 observation ID。

启用普通管理员流量的示例策略：

```json
{"enabled":true,"success_sample_percent":1,"max_records_per_minute":60}
```

独立关闭，无需代码发布或数据库重启：

```json
{"enabled":false,"success_sample_percent":0,"max_records_per_minute":0}
```

策略存于 `gateway_settings.admin_query_observability`。通用settings写/删接口拒绝修改这个键，使用专用接口验证完整策略。配置读取失败保留最后有效策略并增加policy_errors；从未取得有效策略时保持关闭。较旧reload不能覆盖新策略。诊断数据库读写分别使用最多1条独立连接，业务池耗尽不占用该诊断连接；诊断存储或导出失败不影响请求结果。

## 每次记录

用户状态页的users和summary请求共享一个新生成的page-load ID；不把搜索文本、邮箱或表单内容放进诊断字段。API错误保留经过字符和长度校验的响应 `X-Request-ID`，用户状态页在错误后显示该诊断编号。当前实现没有采集页面最终是否采纳响应，不应从服务器请求成功推断浏览器已渲染。

记录包括：独立observation ID、request/page-load/trace ID、实例epoch、源码revision（可用时）或实际二进制SHA256、route、UTC开始时间、单调耗时、HTTP结果、sample reason、事件/采样丢失和截断计数。

阶段包括user_list、pool、usage、cost、cost_reconcile_marker、cost_aggregate、cost_logs_pending或cost_logs_full、sub2api_cost。嵌套阶段可以重叠，不可直接相加作为墙钟耗时。每次acquire独立计时；每条SQL记录完整脱敏模板的fingerprint、backend PID、连接epoch、开始/结束、剩余deadline、错误类别和context状态。保留原异常传播顺序；多个stage同时取消时按实际时间和错误字段解读，不把所有canceled都当作最初原因。

查询指纹在完整规范化模板上计算，240字符展示片段不参与截断后的hash。诊断记录不保存SQL正文或参数；临时SQL模板仅在有界内存中用于核对pg_stat_activity里的同一个执行。旧stdout慢SQL日志继续使用脱敏展示片段。

## 等待采样与预算

仅对执行超过500ms的已登记SQL启动采样。一个worker每250ms最多检查8个backend，读连接在服务端开启read-only，statement_timeout为100ms、客户端调用预算150ms。只有PID和正在记录的SQL模板匹配时接受样本；结束后的迟到样本丢弃。样本包含backend_start、query_start、state、wait_event_type/event、PG query ID（可用时）及最多8个blocker PID。SQL正文不导出，backend_start与连接epoch用于辨别PID复用。

每进程最多16条并行请求记录、每条64个阶段/SQL/acquire事件、120条等待样本、32KiB；队列256条。每分钟最多准入60个请求（可调低），在分配/采样前实施限制。达到准入上限后即使失败也可能没有详细记录，dropped反映这一情况。准入请求中失败和≥1秒的慢请求保留，普通成功按配置百分比采样。达到事件或样本上限标记truncated，不承诺连续覆盖全部等待区间。

采样失败或同SQL匹配缺失分别计数。`pg_stat_activity`可能截断较长查询文本，无法完全匹配的记录标为sample_misses，不能猜测关联。state可包含active或idle；单次wait快照不代表整段SQL的耗时分布。长SQL读结果/执行计划与CPU归因仍需要额外证据。

## 存储与Fugue查询

Schema32只新建 `gateway_admin_observations` 及其两项索引。没有修改历史请求、token或配置表结构，没有扫描/回填历史成本。升级31→32在业务表被锁住的隔离fixture中仍能完成；较旧应用可继续读取原有schema，回滚不删除观测或配置。

诊断表按独立ID幂等写，保存24h，每分钟最多删除1000条过期诊断，跳过锁定行。数据库异常时清理可能落后，24h不是精确删除时点。记录写入与OTLP导出在后台单worker中执行，各有1秒预算；队列满即丢弃。进程崩溃时尚在内存的记录可能丢失，当前没有文件spool或崩溃恢复承诺。

OTLP沿用Fugue注入的telemetry endpoint，使用独立直连诊断transport，不调用账号代理。每个请求是admin_request span，stage/query/acquire为子span，等待样本附在对应query span上。保留可信traceparent或Fugue trace关联；缺失时创建新trace。非法/partial acknowledgement增加export_errors。stdout输出短 `admin_request_observation` 摘要，数据库记录和Fugue查询都能按request ID关联。

`/metrics` 输出 `oaix_admin_observation_*` 的started/enqueued/dropped/stored/store_errors/exported/export_errors/policy_errors/cleanup_errors和active/queue_depth。管理接口还提供oldest_queued_ms和last_stored_at。不要只看Ready或source available判定链路正常：至少取一个真实管理员请求，核验持久化记录、trace ID、Fugue spans及export计数。

## 明确未采集的能力

管理接口返回 `query_first_row_observed=false`、`execution_plan_observed=false`、`node_resources_observed=false`。pgx QueryTracer的结束是查询/结果处理生命周期的结束，不是首行时间或纯服务端CPU。当前没有全库auto_explain、track_io_timing开关变更、节点cgroup/进程采集、历史SQL自动重放、生产cache flush或压测。

这些能力需独立评估后再实现。仅凭新增wait采样也不能保证区分CPU限流、冷磁盘和每种计划退化；如果证据仍不足，继续标unknown，不据此修改费用查询或配置。

## 验证

本地测试覆盖：默认关闭、并行/准入/事件上限、stale policy、取消与迟到样本、存储/导出失败、OTLP partial ACK、完整SQL指纹尾部区分、字面量/注释脱敏；前端fetch fixture验证page ID传播和响应request ID校验。

PostgreSQL14与18隔离集成测试覆盖：schema31→32不锁业务表、重复迁移、配置保存、幂等存储、保留清理、只读采样连接、业务池耗尽、PgSleep、表锁及blocker、取消、管理员权限、开启后summary完整响应。测试数据库完全独立于生产。

发布门槛包括Go全量测试、vet、相关race、前端类型检查/构建和Linux编译。M1 Pro微基准的关闭入口为零分配、约14.5ns；开启后记录一个阶段与查询约1.4µs/1.2KiB（不含真实SQL、序列化、持久化或网络）。该数字不能替代线上延迟、数据库负载与导出验证。
