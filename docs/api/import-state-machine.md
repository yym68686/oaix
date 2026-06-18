# oaix Import State Machine

更新时间：2026-06-19

## Job 状态

- `queued`：job 已创建，等待 worker claim item。
- `running`：至少一个 item 已进入验证/发布流程。
- `completed`：所有 item 已到终态。
- `failed`：job 级失败。
- `canceled`：用户取消，未完成 item 会被标记为 canceled。

## Item 状态

- `queued`：待验证。
- `validating`：worker 正在换取/校验 token。
- `validated`：校验完成，待发布。
- `published`：已写入正式 token 池。
- `failed`：该 item 校验或发布失败。
- `skipped`：用户或规则跳过。
- `canceled`：job 取消导致 item 取消。

## API 流程

1. `POST /admin/import/parse` 或 `POST /admin/import/upload` 解析输入，返回规范化 item。
2. `POST /admin/import/jobs` 创建异步 job，返回 job id。
3. 客户端轮询 `GET /admin/import/jobs/{id}` 或订阅 `GET /admin/import/jobs/{id}/events`。
4. 失败项通过 `GET /admin/import/jobs/{id}/failed-items` 查看。
5. 可用 `POST /admin/import/jobs/{id}/retry`、`POST /admin/import/items/{id}/retry`、`POST /admin/import/items/{id}/skip` 处理失败项。
6. 删除前可先 `DELETE /admin/import/jobs/{id}?dry_run=true` 预览将删除的 key。
