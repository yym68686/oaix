# 出站失败的逐次观测方案

状态：设计稿，尚未实现或部署。调查基线为 `e76bb93`；本分支只增加本文档。

## 问题和决策

目前，非流式 Responses 请求可以在收到上游成功响应头后，读取 SSE 正文时遇到 `unexpected EOF`。`writeResponsesJSONFromSSE` 将读取失败记为本地 502，并允许后续账号尝试。`gateway_request_attempts.status_code=502` 和 `outcome=upstream_5xx` 因此不足以证明上游实际返回了 HTTP 502。

现有记录不能确定 HTTP 正文被截断的发起方。代理传输在响应头之前的错误也被统一替换为一条安全提示，原始错误类型和阶段没有保留。历史连接没有逐次代理配置快照、连接标识、CONNECT 结果和正文读取事实时，不应把失败归因为代理供应商，也不应凭错误字符串更换 HTTP 协议、延长超时、扩大并发或改变重试策略。

本方案先补充观测，保持请求语义、账号选择、计费、冷却、代理配置和在途连接生命周期不变。本文不授权实现或发布；须在后续明确执行时按下述验收门槛推进。即使补齐客户端观测，也不能保证独立识别第三方代理内部或其远端上游的所有故障；那部分仍需要供应商侧证据。

## 已确认的缺口

| 位置 | 当前事实 | 后果 |
| --- | --- | --- |
| `internal/egress/proxy.go: ForToken` | 只把解析后的 URL/错误放入 context | 请求记录无法区分配置意图与该次实际执行路径 |
| `internal/egress/proxy.go: proxyTransport.RoundTrip` | 对账号代理的传输错误使用新的通用错误 | DNS、拨号、CONNECT、TLS、写请求和读响应头失败无法区分 |
| `internal/transport/client.go` | 没有逐次 `httptrace` 或正文读计数 | 缺少连接复用、阶段耗时和中断位置 |
| `internal/proxy/compat_payload.go: collectResponsesJSONFromSSE` | 非流式收集路径未生成 `StreamDeliveryTrace` | 缺少最后事件、完成事件和终止原因 |
| `internal/proxy/pipeline.go: recordGatewayAttempt` | 保存处理后的 status/outcome，缺少独立原始上游状态 | 本地合成 502 会与上游 HTTP 502 混淆 |
| `internal/proxy/pipeline.go: copyResponseHeaders` | 使用 `Header.Add`，重试前已复制的上游头可能继续留在 writer | 两次请求的 ID、额度和日期可能同时出现在最终响应头 |
| `internal/httpapi/http_observability.go` | 最终成功的普通推理请求不会因早期 attempt 失败而打印该 attempt 的日志 | 只检索容器日志无法补回失败链 |

最后一项响应头问题是独立、已确认的行为，不是 EOF 的根因。修正头部提交时机应单独实现与验收，不混入纯观测发布。

## 逐次记录契约

每次 OAIX attempt 分配不可复用的 `attempt_id`，关联业务 `request_id`、`attempt_index`、可信的 trace/span ID、实例启动 epoch 和进程内连接 ID。传输库内部的多次拨号/重发以子序号保存，不能覆盖为最后一次。继续保留业务日志作为用户请求历史主存储；运维采集失败不影响它的写入和语义。

建议增加可空、带 `schema_version` 的 `egress_trace`。先采用独立新增列或独立诊断表，迁移前比较表锁、行宽、写放大、保留策略及旧版本兼容；不对历史大表回填，不在热路径建立 JSON 大索引。新的诊断结果不得覆盖旧 `status_code`、`success`、`outcome` 或收费字段。

| 分组 | 最小字段 | 语义 |
| --- | --- | --- |
| 配置快照 | route kind、proxy channel ID、配置 revision、binding revision、resolved_at | 保存该 attempt 实际解析的意图；不在请求结束后重读当前绑定代替历史快照 |
| 路由执行 | used proxy、协议、经脱敏的 host:port、拨号子序号 | 区分账号代理、默认环境代理和默认直连；缺字段表示未知 |
| 连接 | connection ID、reused、was_idle、idle_ms、local/remote endpoint | IPv4/IPv6 和多个候选分别记录；端点只进受控运维记录 |
| 阶段 | pool acquire、DNS、TCP、CONNECT、TLS、write request、headers、first body byte、last body byte、body end | UTC 用于关联，单调时钟用于耗时；缺失阶段不能记为零耗时 |
| CONNECT | status、供应商允许列表中的会话 ID、阶段错误类别 | 非 200 和无响应区分；绝不保存代理认证头 |
| 上游响应 | 原始 status、HTTP protocol、content type、content length、transfer encoding、content encoding、允许列表的 request ID / CF ray | 与本地合成结果分开；保留头字段的多值语义并限定长度 |
| 正文 | transport-delivered bytes、first/last read time、read error class、close reason、context error | 解压后的字节不能称为 wire bytes；Close 调用不等于对端先关闭 |
| SSE | parser events、last type/sequence、completed/failed counts、terminal_seen | 非流式收集与流式转发都记录；不保存文本、reasoning 或事件正文 |
| 下游 | committed、write/flush结果、cancel time、客户端取消类别 | 区分可安全换账号的失败和已提交后不能重播的失败 |
| 完整性 | sampling decision、truncated、dropped events、observer error、export result | 采集缺失本身可见，不能把空 trace 当作连接正常 |

配置 revision 必须由配置写入事实产生，不应包含明文凭证或未加密的凭证摘要。代理记录改名与连接相关配置变更应可以区分。配置恢复不依赖诊断程序、数据导出或新代码版本；解析配置失败继续执行原有的 fail-closed 行为，绝不因为观测降级绕过账号代理。

## 采集位置及实现约束

1. 在 `ResolveTokenProxy` / `ForToken` 的同次解析中取得安全配置标识，与本 attempt 的 URL 一起固定。观测副本不成为 serving 配置的所有者，也不增加独立同步数据库读取。
2. 在原有 dial / transport 上组合 `httptrace.ClientTrace`，保留原回调，处理并发回调和请求返回后的回调。自定义代理 DNS 使用 `LookupNetIP` 再拨 IP，应显式记录这段解析，不能只依赖 `net.Dialer` 的 DNS hooks。
3. 用 `Transport.OnProxyConnectResponse` 记录 CONNECT 响应；观测回调自身不得返回错误。已有业务回调的返回语义必须保留。HTTP CONNECT、HTTPS 代理的双层 TLS、SOCKS 握手、连接复用分别标注支持程度，不能把缺失事件解释为成功。
4. 在原始错误被安全提示替换前，将其分类为固定枚举，例如 dns、dial、connect_auth、connect_rejected、tls、write_request、read_headers、body_read、context_cancel、deadline、unknown。内部类型链可在内存中判断，但禁止直接记录 `url.Error.Error()` 或任意错误全文。
5. 在响应 `Body` 包装器里记录读计数和终止事件；保持每次 `Read` 的 `(n, err)`、返回时机和 `Close` 语义。不得后台预读、增加 buffer、延长生命周期或主动取消连接。只实现原接口真实支持的能力，避免改变 `io.Copy` 等调用路径。
6. 在 parser 调用者记录语义事件和读取终止原因。已完成事件、干净 EOF、正文截断、SSE 解析错误、上游 `response.failed`、下游取消分别记录；此阶段只观测，不修改原有成功或重试判断。
7. 把记录送入有界异步运维管道：建议每 attempt 最多 96 个阶段事件、序列化后最多 16 KiB；失败保留到预算上限，成功按明确采样策略记录。队列满即计数丢弃，不阻塞 serving，也不使用无限 goroutine 或无界内存。上线前测定并调整具体容量。
8. 以一条自然流量请求验证 `Fugue edge trace -> OAIX request -> attempt -> connection` 的可查询关联。先检查 Go 版本是否实际发出 spans，再验证 exporter 和 ingestion；不能只依据 `source.available=true` 宣称应用已接入。

所有运维输出都排除 Authorization、Proxy-Authorization、Cookie、API key、代理用户名/密码、完整 URL、账号邮箱、请求正文和 `x-codex-turn-state`。请求 ID 只作为日志/trace 字段，不能成为高基数 Prometheus 标签。指标按阶段、协议、错误类别和 route kind 聚合；具体账号/代理的诊断使用受权限控制的记录。

## 因果证据门槛

| 证据 | 可以确认 | 仍不能确认 |
| --- | --- | --- |
| 明确 DNS/dial/CONNECT/TLS 失败且含对应 attempt 关联 | 该次客户端失败阶段和错误类别 | 远端供应商内部的最终根因 |
| CONNECT 已成功、收到上游 2xx、正文短读 | 隧道建立后响应读取失败 | 谁发起断开、为何断开 |
| 节点上对应 socket 的对端 FIN/RST | 对端连接的终止事实 | 对端是否只转发了更远端的关闭 |
| 关联到供应商会话的主动关闭原因和上下游 socket 记录 | 代理做出了哪项关闭决策 | 无远端证据时的上游内部故障 |
| 上游 request ID 对应的服务端日志 | 上游服务记录的结束/取消/错误 | 与不同请求或不同时间窗无法关联的其他现象 |
| 隔离 fixture 重现一个错误字符串 | 某条代码路径可以产生该字符串 | 历史线上事件就是该原因 |

只有同一请求、同一连接、同一时间窗的正向证据才能归因。成功的事后探针不能排除历史故障，失败的事后探针也不能反推历史故障。账号 A/B 的错误率差异不等于代理的随机对照实验。

如客户端观测仍不足，应取得供应商允许导出的 CONNECT/session ID、上下游连接时间、出口、关闭原因和字节计数，并按上游 request ID 对照服务端记录。没有供应商日志的访问能力时明确标记 unknown，不擅自向外发送用户数据或提交工单。

必要的节点观测应作为独立运维组件设计：默认关闭，只针对选定应用 cgroup 和代理目的端口采集 TCP 状态元数据；容量、时间窗和落盘上限固定，记录 collector epoch、丢包和覆盖缺口。CONNECT 明文含认证信息，不得抓取带 payload 的代理流量，也不导出 TLS 会话密钥。既有入口 relay 抓包不能替代这条出站链路的证据。

## 验收和发布顺序

先做隔离故障注入，所有外部端点使用本机 fixture，不调用生产模型、不消耗生产账号额度。覆盖：

- DNS 失败、多个 IP 候选、TCP 拒绝/超时、CONNECT 407/502、TLS 握手失败。
- HTTP/1.1 Content-Length 短读、chunk 数据不完整、终止 chunk 缺失、gzip 截断、HTTP/2 关闭；证明分别记录事实而非仅保留同一个 EOF 字符串。
- 上游明确 `response.failed`、完成事件后正常关闭、完成前失败、超大/损坏 SSE、客户端取消、deadline，以及响应已提交后的失败。
- 连接复用和不复用、并发、池内部重试、重定向、HTTP/HTTPS/SOCKS 代理、默认环境代理、实际绑定在两次请求间变更。
- 回调乱序、晚回调、导出失败、满队列、字段超限、异常退出及旧/新版本并行；用 race 检查共享观测状态。
- 凭证诱饵测试，验证所有错误分类、日志、JSON、指标和供应商头允许列表都不泄漏。

比较观测开关前后的业务响应、字节、headers、retry次数、claim释放、冷却、费用和配置哈希。Go 全量测试、vet、相关 race 与必要的 PG14/18 增量迁移验证通过后，再做低负载隔离 benchmark，确定 CPU、内存和延迟预算。测试通过不能被写成线上故障已经归因。

后续若获准实施，顺序为：仅数据库兼容增量（如确实需要）→ 应用观测代码但采集关闭 → 少量指定账号/请求开启观测 → 自然失败样本验证关联 → 扩大采样。每步验证 Ready、重启、在途请求排空、错误率、TTFB/延迟、claim/fd和导出队列；出现观测导致的回归先关闭采集，保持原代理绑定和正常 serving 路径。

修复根因应成为后续独立提交，以新证据关联的失败 fixture 验收。响应头跨 attempt 残留也单独处理：仅在决定提交该 attempt 时复制头，或撤销本 attempt 添加的上游头；保留 CORS、OAIX 标识、合法多值头和 keepalive 行为，禁止用清空所有响应头的方式修复。

## 执行状态

- [x] 源码审查，区分本地合成 502 与实际上游 HTTP 状态。
- [x] 定义逐次配置、传输、正文、协议和观测完整性契约。
- [x] 明确客户端无法单独证明第三方内部故障的边界。
- [ ] 实现观测采集和兼容存储。
- [ ] 隔离故障注入、性能和无行为变化验收。
- [ ] 获准后逐步发布并通过自然流量验证。
- [ ] 取得完整因果证据后确定、修复并验证根因。
