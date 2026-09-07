# 用户代理渠道

用户导航中的「代理配置」位于 `/account/proxies`。点击「添加代理」打开模态框；编辑渠道复用同一模态框。支持自动解析：

- `proxy.example.com:9999:username:password`（默认 HTTP）
- `proxy.example.com:8080`
- `http://username:password@proxy.example.com:9999`
- `https://proxy.example.com:443`
- `socks5://username:password@proxy.example.com:1080`，也支持 `socks5h`
- IPv6 地址须加方括号。简写格式的密码可以包含冒号；URL 格式中的特殊字符使用百分号编码。

每个渠道可以测试、编辑和删除。测试固定通过代理访问 `https://chatgpt.com/cdn-cgi/trace`，最多等待 12 秒，返回出口 IP、耗时和上游 HTTP 状态。只有收到有效的成功响应和出口 IP 才报告可用；这不代表具体账号的凭证或额度有效。账号可用性使用账号详情原有的测试按钮确认。测试结果是当前页面的运行事实，刷新后清除，不修改渠道配置。

账号详情的「账号设置 → 账号代理」支持选定所属用户的渠道，或选择「不使用账号代理（默认出站）」。配置从下一次出站请求生效，在途请求继续使用其已解析的配置。请求转发、模型列表、额度查询/重置、OAuth 凭证刷新及 Agent Identity 任务注册均接入代理。代理错误或配置读取错误会使该次出站失败，不能回退到默认出站。同一账号被 OAIX marketplace 选择时也遵循该账号的绑定。外部 Sub2API 系统的代理配置仍由该系统独立管理。

## 数据及权限

- `proxy_channels` 保存用户意图，完整 URL 用既有的 `API_KEY_ENCRYPTION_SECRET` 加密。API 响应和审计记录不包含代理用户名或密码。
- `token_proxy_bindings` 保存账号绑定，数据库外键禁止删除仍被绑定的渠道；账号删除时清理绑定。
- 每个用户最多 100 个渠道。用户只能操作自己的渠道；管理员为账号配置代理时也只能选择该账号所属用户的渠道。只读管理员不能修改或测试。
- 公网代理地址才可使用；连接时检查全部 DNS 结果并直接拨号已验证的 IP，拒绝私网、回环和元数据地址。不同代理凭证的连接池由 Go HTTP transport 分隔。
- 增量 schema 29 仅增加这两张配置表和索引，不重放旧迁移。回退代码不删除配置表；旧版本不执行代理绑定，恢复新版后继续读取保存的配置。因此使用代理后如需回退代码，应先评估默认出站行为。

## API

| 方法及路径 | 行为 |
| --- | --- |
| `GET /api/proxies` | 当前用户的渠道及绑定账号数 |
| `POST /api/proxies/parse` | 解析 `{proxy}`，只返回非敏感预览 |
| `POST /api/proxies` | 创建 `{name, proxy}` |
| `POST /api/proxies/{id}` | 更新；`proxy` 留空保留凭证 |
| `DELETE /api/proxies/{id}` | 删除未被绑定的渠道 |
| `POST /api/proxies/{id}/test` | 测试并返回 `{ok, message, duration_ms, checked_at, exit_ip?, status_code?}` |
| `GET /api/tokens/{id}/proxy` | 读取绑定与可选渠道 |
| `POST /api/tokens/{id}/proxy` | 保存 `{proxy_channel_id}`；`0` 取消代理 |
| `GET/POST /api/admin/tokens/{id}/proxy` | 管理员访问账号代理设置 |

## 验证

单元测试覆盖解析、特殊字符、认证隔离、HTTPS CONNECT、SOCKS5 远端 DNS、私网拒绝、凭证脱敏和失败不直连。数据库测试 `TestUserProxyChannelsIsolationPersistenceAndRouting` 覆盖权限、加密保存、绑定、并发删除保护、增量迁移保留配置、代理失败和解绑。

本次完整 PostgreSQL 套件还暴露了三个既有 fixture 问题；在未修改的 `58663dd` 上同样复现：`TestBlackboxCompatibilityFixture` 的图像请求返回 503，`TestDeleteDisabledTokensScopeWithDatabase` 重读已经关闭的 response body，`TestPostgresRepriceFastRequestCosts` 的扫描数量依赖数据库中已有请求行。这些失败不能归因为代理功能回归。
