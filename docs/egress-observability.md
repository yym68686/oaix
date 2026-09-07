# 出站观测运行说明

出站观测记录每次账号尝试的配置快照、客户端传输阶段、正文和SSE终止事实。它不改变账号选择、代理绑定、请求头、响应正文、重试、冷却和计费。HTTP响应头重试残留在独立提交 `fff6575` 中修复：未提交的尝试恢复原网关头；流式keepalive已提交的头保持原状，后续尝试不再向其追加无法发送的头。

## 配置与恢复

默认关闭。策略存于现有 `gateway_settings.egress_observability`，由独立管理API更新，不通过环境变量、代码常量或账号代理配置开启。写入成功后当前进程立即应用；其他进程每15秒读取一次，较旧的读取结果不能覆盖新版本。读取失败保留最后有效的观测策略并增加计数，不改变serving配置。关闭观测无需部署或重启；已开始的尝试仍可完成其记录。

- `GET /admin/egress-observability`：持久化/运行中策略、队列与存储/导出计数、OTLP配置可用性。
- `POST /admin/egress-observability`：更新下面的完整JSON。仅管理员/服务凭证可写；只读管理员不可写。
- `GET /admin/egress-observations?request_id=<id>`：按请求ID读取最多100条尝试记录；仅管理员可读。
- `/livez` 的 `egress_observability` 与 `/metrics` 的 `oaix_egress_observation_*`：不含账号、地址和请求内容的聚合状态。

限定账号的初次启用示例（示例ID必须替换为操作目标）：

```json
{"enabled":true,"token_ids":[42],"success_sample_percent":10}
```

验证后全局小比例成功采样：

```json
{"enabled":true,"token_ids":[],"success_sample_percent":1}
```

独立关闭：

```json
{"enabled":false,"token_ids":[],"success_sample_percent":0}
```

空 `token_ids` 表示所有账号；最多100个正整数且不允许重复。成功采样为0–100整数，失败在预算内全部保留。通用settings写入/删除API拒绝此专用设置，避免绕过验证或让运行中策略不一致；恢复用专用API保存disabled策略。设置本身没有随诊断清理删除，代码回退也不删除代理配置或诊断表。

## 数据与限额

Schema 31只新增 `gateway_egress_observations` 和该空表的索引；不ALTER、回填或扫描历史业务请求大表。诊断worker独立于账单、业务日志和配置恢复。每条记录按 `attempt_id` 幂等保存，request_id索引用于查询。created_at索引用于24小时保留：每分钟最多删除1000条过期诊断，跳过锁定记录；多实例高流量下可观察清理积压，24小时不是承诺精确到秒的删除时间。

每进程队列最多256条，每尝试最多96个阶段事件、序列化最多16KiB，每分钟最多入队600条。超过事件/字节预算会在记录中标记truncated/dropped_events；队列满、限速和停止阶段丢弃均可计数。数据库与OTLP各有1秒预算，一个worker执行；慢导出不会让serving请求等待。进程退出时诊断队列可以丢弃且计数，不延迟在途业务排空或更改请求结果。异常进程终止的未导出数据无法恢复。

记录包括代理channel ID、binding/channel updated_at版本、实际代理选择是否被观察到、HTTP CONNECT响应、TLS与HTTP协议、每阶段事件、连接ID/复用、本地/远端endpoint、原始上游status、允许列表request ID/CF ray、本地status、body读取错误、字节/时间、SSE计数和终止标记。`downstream_request_id_header_count` 可验证响应头数量，不能修改已发送响应头。重复拨号和头事件分别保留；顶层连接/上游响应字段代表最后观察到的值。

记录没有Authorization、Cookie、代理用户名/密码、完整代理URL、账号邮箱、请求/响应正文、reasoning或turn-state。错误只保存固定类别，未识别类型为unknown。SSE未知事件名记录为other。连接endpoint只进入管理员诊断表；Prometheus没有请求ID/账号/IP高基数标签。

## Fugue关联

沿用平台注入的 `FUGUE_OBSERVABILITY_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT` 和平台资源身份，异步向 `/v1/traces` 发送OTLP JSON。出口是平台控制的诊断连接，不使用账号代理。优先使用有效W3C traceparent，其次使用X-Fugue-Trace-ID；两者缺失时，同进程相同request_id的attempt共享一个派生trace ID。

每attempt一个父span和有限数量阶段span。缺少start的阶段标记 `paired_start=false` 并使用瞬时事件，不能把它解释为零耗时的完整阶段。常规容器日志还输出简短 `egress_attempt`，包含request/trace/attempt/connection关联。attempt日志不会作为重复的顶层request fact写入，业务请求总时长仍以原业务日志为准。

上线验证应当按一条自然流量request_id读取诊断表，再用其中的trace_id执行 `fugue app traces oaix <trace_id> --json`。分别核验 `stored` 与 `exported`，不能只用Ready或队列为空判断观测接通。`store_errors` / `export_errors` / `policy_errors` / `cleanup_errors` / `dropped` 非零时先检查观测管道，不据此改变业务路由。

## 边界

- HTTP代理的CONNECT仅有响应回调；没有捕获CONNECT请求原始字节，也不将它的发送时刻伪装为已测量事实。
- HTTPS代理可能有代理与上游两次TLS，事件分别记录。SOCKS内部握手尚无细分钩子，显式标为 `socks_handshake_not_instrumented`；现有SOCKS认证和远端DNS行为保留。
- ConnectionID对本transport拥有的连接稳定。第三方包装或未知连接返回空ID，不猜测；重复连接事件仍含安全endpoint。
- body_bytes是交付给应用的字节数，自动解压时不是线上压缩字节数；多个HTTP往返/重定向的读取累计记录。HTTP/2读错误若无法识别保留unknown，不能猜测RST_STREAM原因。
- 收到2xx后unexpected_eof证明正文读取中断，不能独立证明第三方代理内部的关闭发起方。没有抓取生产payload、解密TLS、安装节点采集器或向供应商发送用户信息。
- 该版本实现应用/客户端核心观测；供应商session、关闭原因和按需节点元数据采集仍需相应外部证据。它们是根因调查后续手段，不是本版本已提供的数据。

## 验证

新增隔离测试覆盖：真实CONNECT407、HTTPS隧道成功及正文短读、连接复用、HTTP/2、TLS验证失败、gzip截断、EOF重试结果和最终响应头一致、SSE完成事件、错误/凭证脱敏、队列/速率/事件上限、异步导出阻塞、迟到回调、管理员权限、策略持久化、诊断幂等与保留。

PG14/18验证Schema30→31和重复启动；迁移时锁住业务日志、账号和代理配置表仍能完成，证明增量不访问这些热表。基线代理解析、失败不直连、凭证隔离及SOCKS测试继续运行。全量Go测试、vet、相关race及Linux构建为发布门槛。隔离benchmark只反映观测代码开销，不代表生产P99或整机CPU收益。


记录schema_version=2修正了初次线上验收发现的观测细节：DNS拨号的UDP/TCP按实际network区分；CONNECT/TLS失败在GotConn之前也保留已建立socket的ID和端点；原始transport错误与安全包装后的结果分别保存。同端点并行拨号无法从标准钩子唯一配对时，span标记paired_start=false，不伪造耗时。schema_version=1的历史记录不回写，其DNS阶段的tcp标记不能单独作为实际TCP证据。
