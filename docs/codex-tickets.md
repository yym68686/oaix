# Codex turn-state tickets

OAIX automatically caches and injects `x-codex-turn-state` tickets for
`gpt-6-astra` and `gpt-5.6-sol`. This adapts the account/model ticket lifecycle
from [sub2api #7315](https://github.com/Wei-Shaw/sub2api/pull/7315).

The default is enabled. Each active, available OAuth/access-token account uses
its existing proxy for both harvesting and business requests. Administrators
can select a separate harvest proxy without changing the business proxy.
Agent Identity and personal access tokens retain their existing behavior.

Only HTTP 200 responses with a 292-character `gAAAAA` ticket are accepted. Tickets
are isolated by token, owner/account identity and exact upstream model. They
expire after one hour and refresh ten minutes before expiry. Receiving the same
ticket again does not extend its expiry. A failed refresh preserves the current
valid ticket. Business responses can also supply tickets, without a synchronous
database write on the response path. Account/model selection is checked again
on every outbound attempt, including failover and compact requests.

Harvest requests use a separate HTTP/1 transport, close their connection after
reading the headers, and reject redirects. They are limited to two concurrent
probes and count against each account's normal concurrency cap. A probe has a
25-second deadline. Misses start with a six-second retry delay and back off to
384 seconds; 401/403/429 responses wait at least five minutes, respecting longer
`Retry-After` values up to one hour. The scheduler runs every five seconds, so
actual retry intervals round up to its next available slot. Probes do not enter
business request logs, usage billing or token-health transitions. An existing
proxy does not necessarily rotate IPs or produce a valid ticket.

Unlike the referenced PR, OAIX defaults to **fail open**: missing tickets leave
the existing request path available, with any client-supplied ticket removed
for these models because it may belong to another selected account. An explicit
`fail_closed` policy restricts selection to accounts with valid tickets and
rechecks expiry before forwarding. With no eligible accounts it returns 503.
Unconfigured models and a disabled policy preserve existing forwarding.

## Live settings

The administrator Settings page includes a Codex tickets panel. Changes are
stored independently in `gateway_settings.codex_tickets`, applied immediately
on the receiving process, and reloaded by other processes every five seconds.
Failed reads retain the last successfully loaded policy. On initial startup,
background work waits for a successful policy read.

`GET /admin/codex-tickets` returns the saved/runtime policy, counters and up to
200 account/model status summaries. It never returns ticket blobs. Proxy
passwords are masked. `POST /admin/codex-tickets` accepts partial updates:

```json
{
  "enabled": true,
  "fail_closed": false,
  "models": ["gpt-6-astra", "gpt-5.6-sol"],
  "harvest_proxy_url": "",
  "clear_harvest_proxy": false
}
```

An omitted/empty or unchanged masked proxy preserves the saved proxy.
`clear_harvest_proxy: true` restores account proxy selection. HTTP, HTTPS,
SOCKS5 and SOCKS5h proxies use OAIX's existing public-address validation. These
endpoints require admin/service access; readonly admins cannot mutate settings.
The generic settings APIs cannot update or delete this protected setting.

## Persistence and rollout

Schema 34 adds `codex_turn_tickets` without altering account records. Ticket
blobs and independent proxy URLs use the existing authenticated encryption key.
Monotonic upserts prevent stale workers from overwriting newer tickets. Account
edits/imports cannot write ticket state. Current tickets reload after a restart;
expired records cannot be injected. Rolling back code does not delete settings
or tickets. Background probes stop and join before the database closes.

Monitor `GET /admin/codex-tickets`, `/healthz`, runtime pod readiness/restarts,
and natural request outcomes. Check `harvested`, `injected`, `persist_errors`,
`reload_errors`, and status `last_probe_status`/`last_header_length`. Successful
HTTP probes without a valid-length ticket are misses, not proof of readiness.
Use Fugue's authenticated internal request command without printing service keys:

```sh
fugue app request oaix GET /admin/codex-tickets --header-from-env X-API-Key=SERVICE_API_KEYS
```
