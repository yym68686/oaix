# sub2api Codex account import and query compatibility

OAIX accepts the sub2api account-import HTTP formats using existing OAIX keys.
This is a format adapter; it does not install a sub2api scheduler, group model,
billing model, OAuth/privacy worker, or configuration store. No schema migration
or additional service configuration is required.

## Authentication and account ownership

Set `x-api-key` to an existing OAIX key (or use `Authorization: Bearer <key>`).
When both headers are supplied, `x-api-key` takes precedence, as in sub2api.

- A user's API key imports into that user's pool. Request-body `user_id`,
  `owner_user_id`, group IDs, and credential extras cannot change the owner.
- An OAIX service key imports into the existing platform bootstrap admin pool
  by default. Native admin/service impersonation via `X-OAIX-Act-As-User` is
  available to authorized administrators; ordinary user keys cannot use it.
- Ownerless OAIX administrator keys default to the platform bootstrap pool;
  administrator keys attached to a user default to that user's pool.
- Expired, revoked, disabled-user, and readonly keys cannot import. This
  compatibility permission grants no access to other platform admin APIs.

## Endpoints

All endpoints use the source envelope: HTTP 200 with
`{"code":0,"message":"success","data":...}` on success. Actual invalid input,
failed credentials, storage failures, and permission errors still report errors.
Unsupported optional policies alone do not produce errors.

| Method and path | Request | Successful `data` |
| --- | --- | --- |
| `POST /api/v1/admin/accounts` | A sub2api account object | sub2api-shaped account DTO with the real OAIX token ID; credential secrets omitted |
| `POST /api/v1/admin/accounts/batch` | `{"accounts":[...]}` | `success`, `failed`, `results` with `name`, `success`, `id` or `error` |
| `POST /api/v1/admin/accounts/data` | `{"data":{"type":"sub2api-data","version":1,"exported_at":"...","proxies":[],"accounts":[...]},"skip_default_group_bind":true}` | `proxy_created`, `proxy_reused`, `proxy_failed`, `account_created`, `account_failed`, optional `errors` |
| `POST /api/v1/admin/accounts/import/codex-session` | `{"content":"...","contents":["..."],"name":"...","notes":"...","proxy_id":123}` | `total`, `created`, `updated`, `skipped`, `failed`, `items`, optional `warnings`/`errors`; item indices start at 1 |

Data bundles also accept `sub2api-bundle` and omitted type/version, matching the
source header rules. `accounts` and `proxies` must be arrays, including empty
arrays. An empty bundle is a capability check and creates no import job.
Requests are limited to 32 MiB and 1,000 accounts (and 1,000 proxy entries).
Native synchronous imports keep the existing 30-second deadline.

## Field mapping

Only `platform: "openai", type: "oauth"` accounts are supported by the account
and data endpoints. Session imports assume Codex; an explicitly different
platform is rejected per item. Other providers and API-key upstream accounts
are outside this adapter.

| Input | OAIX behavior |
| --- | --- |
| `credentials.access_token`, `refresh_token`, `id_token`, `client_id` | Native Codex credentials and OAuth refresh path |
| `chatgpt_account_id` / `account_id`, `email`, `plan_type`, related identity fields | Native identity metadata; display names never become email identity |
| Codex `tokens` object, session `accessToken`, `user.email`, `account.id`, snake/camel spellings | Flattened to the same native credential fields |
| `agent_identity` / agent-identity credentials | Existing native agent-identity import validation and storage |
| `notes`, or `name` when notes are absent | Native account remark; no separate foreign account-name model |
| `credentials.disabled` | Native `is_active` flag |
| `proxy_id` on single/batch/session requests | An existing OAIX proxy-channel ID owned by the receiving user; `0` unbinds, absent/null preserves the binding |
| Bundle `proxies` and `proxy_key` | Proxy URL/credentials become native encrypted proxy channels, reused by normalized URL within the owner; bundle-local keys resolve to those channels |

Native proxy validation still applies. A referenced proxy that cannot be imported
fails that account; it never silently changes the account to direct egress.
Credentials, remark, and binding commit in the same transaction. A binding
failure rolls back the credential change and preserves the prior binding.
Unreferenced successfully imported proxy channels remain in the owner's native
inventory, consistent with the source bundle's partial-import behavior.

The following are accepted as compatibility placeholders and do not set native
configuration: `group_ids`, `priority`, `concurrency`, `rate_multiplier`,
`load_factor`, account `expires_at` / `auto_pause_on_expired`,
`skip_default_group_bind`, `confirm_mixed_channel_risk`,
`upstream_billing_probe_enabled`, `credential_extras`, `extra`, proxy
status/expiry/fallback policies, and unknown optional fields. In particular,
sub2api request concurrency is not OAIX's active-stream cap. The account DTO
retains the corresponding scalar fields; group collections and `extra` are
empty placeholders. There is no persistence or round-trip API for foreign
policies.

OAIX identity matching, deduplication, credential validation and refresh, pool
activation, and audit behavior remain native. `update_existing` is accepted but
does not override native upserts. Repeated credentials do not create independent
sub2api-style copies; session results report the actual native `updated` action.
The data format has no updated counter, so `account_created` counts successful
imports including native updates. The endpoints report completed imports rather
than pretending an asynchronous queued import already succeeded. No native
sub2api user-sync or usage-sync integration is invoked by this adapter.

## Example

Use the OAIX base URL where an importer expects the sub2api base URL, and its
existing OAIX API key where the importer asks for the admin key.

```sh
curl https://oaix.fugue.pro/api/v1/admin/accounts \
  -H "x-api-key: $OAIX_API_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"name":"codex account","platform":"openai","type":"oauth","credentials":{"access_token":"<access token>","refresh_token":"<refresh token>"},"group_ids":[],"concurrency":3,"priority":50,"rate_multiplier":1}'
```

Account, proxy and group queries are described below. sub2api JWT login is
not implemented; authentication uses OAIX keys throughout.

## Account query compatibility

The account read surface is also available. It uses the same keys and owner
selection as imports. Readonly OAIX admin keys can query their default pool but
still cannot import; user keys never gain platform administration privileges.
All query replies are JSON and `Cache-Control: private, no-store`. Unknown
compatibility GET paths return a JSON 404 instead of the SPA HTML page.

The adapter covers all 31 GET routes from the source account, OpenAI quota,
proxy and group route groups, plus the two account batch-query POST routes:

| Method and path (prefix `/api/v1/admin`) | Mapping |
| --- | --- |
| `GET /accounts` | Owner's native Codex tokens, filtered and paginated |
| `GET /accounts/:id` | Owner-scoped account details; foreign/missing IDs return 404 |
| `GET /accounts/:id/usage` | Latest persisted native quota snapshot, `UsageInfo` shape |
| `GET /openai/accounts/:id/quota` | Same snapshot, OpenAI quota shape and reset-credit count |
| `GET /accounts/:id/stats` | Native request statistics, daily history, models and endpoints; `days=1..90`, default 30 |
| `GET /accounts/:id/today-stats` | Native request/token/cost totals for today |
| `POST /accounts/usage/batch` | `{"account_ids":[...]}` → `usage` and per-ID `errors` maps |
| `POST /accounts/today-stats/batch` | `{"account_ids":[...]}` → `stats` map containing owned accounts |
| `GET /accounts/:id/temp-unschedulable` | Native short upstream retry backoff |
| `GET /accounts/:id/models` | Native model catalog filtered by the owner's plan policy and cached plan capabilities |
| `GET /accounts/data` | Source-shaped metadata bundle; optional `ids` and `include_proxies` |
| `GET /accounts/upstream-billing-rates` | Same filtered account page, real account IDs, `snapshot: null` |
| `GET /accounts/upstream-billing-probe/settings` | Disabled settings placeholder |
| `GET /accounts/ollama-cloud-usage/settings` | Disabled settings placeholder |
| `GET /accounts/:id/ollama-cloud-usage` | Owned account ID with eligibility/configuration flags false |
| `GET /accounts/antigravity/default-model-mapping` | Empty mapping |
| `GET /proxies`, `/proxies/all`, `/proxies/:id` | Native owner-scoped proxy inventory and account counts; credential fields redacted |
| `GET /proxies/:id/accounts` | Accounts with that native proxy binding |
| `GET /proxies/:id/stats` | Native account/available counts; source's traffic-statistic placeholders |
| `GET /proxies/data` | Source-shaped proxy metadata bundle |
| `GET /groups`, `/groups/all` | Empty paginated result / empty array |
| `GET /groups/usage-summary`, `/groups/capacity-summary` | Empty arrays |
| `GET /groups/live-capability` | `supported: false` |
| `GET /groups/:id` | Inactive group DTO placeholder; no group is created |
| `GET /groups/:id/models-list-candidates` | `{"models":[]}` |
| `GET /groups/:id/composite-routes`, `/groups/:id/rate-multipliers` | Empty arrays |
| `GET /groups/:id/stats` | Zero-valued source statistics shape |
| `GET /groups/:id/api-keys` | Empty paginated result; never exposes native keys |

`GET /accounts` accepts `page` (default 1), `page_size` or `limit` (default 20,
maximum 1000), `search`, `platform`, `type`, `status`, `sort_by`, `sort_order`, and
`lite`. It returns `data.items`, `total`, `page`, `page_size`, and `pages` (at
least 1, including an empty pool). `total` counts the whole matching pool, not
only the current page. Source-sized pages above 500 use bounded native reads.
Search maps to native remark, email and account ID. The query `name` comes from
remark, then email, then account ID. Native remark is also returned as `notes`.

Supported sorting maps name, ID, creation time, last-use time and status to
native ordering; unsupported policy-based sorting has no effect on native
policy. `group`, `privacy_mode`, and scheduler-score options are accepted as
placeholders and never exclude otherwise matching native accounts. Different
platforms and non-OAuth account types return empty results because this adapter
only manages Codex accounts.

| Source status filter | Native facts |
| --- | --- |
| `active` | Enabled, credentials present (OAuth or agent identity), not disabled or cooling |
| `rate_limited` | Enabled account with usable credentials in native cooldown, excluding short retry backoff |
| `temp_unschedulable` | Enabled account with credentials in short native retry backoff |
| `inactive` / `disabled` | Disabled account without a recorded error |
| `error` | Disabled account with an error, or enabled account missing usable credentials |
| `unschedulable` | Empty: there is no independent foreign scheduler switch in OAIX |

Cooling/backoff account DTOs retain `status: "active"` and expose their native
reset/backoff timestamps. `schedulable` reports actual credential/cooldown
readiness, without reserving a slot or promising availability for every model.
`concurrency` reads OAIX's effective account/owner/global stream cap, and
`current_concurrency` reads existing runtime counters. The foreign import
`concurrency` option remains a no-op; reading a native cap does not create a
foreign write mapping. Error descriptions are sanitized categories.

Quota queries are passive reads even with `force=true` or `source=active`; they
do not refresh credentials, redeem credits, probe upstreams, or activate or
disable accounts. Missing quota data returns null/omitted windows, not invented
zero usage. `updated_at` / `fetched_at` identifies snapshot age. Statistics use
UTC day boundaries and retained native request logs, scoped to the token owner
at the time of the request. They include requests using that owner's shared
accounts; they exclude a token's history under another owner. Cost fields map
to OAIX's local estimated USD cost, with no sub2api billing multiplier or remote
usage synchronization. Missing upstream-endpoint analytics remain an empty
array.

Exports preserve OAIX's existing metadata-only export boundary: token secrets,
agent private keys, raw error payloads and proxy passwords are never returned.
Bundles set `credentials_redacted: true`; they are inventory exports, not
restorable credential backups. Repeated or comma-separated `ids` parameters
select owned records, and omitted IDs export the matching owned inventory.
No native schema migration or foreign background worker is introduced.

Example readiness query:

```sh
curl 'https://oaix.fugue.pro/api/v1/admin/accounts?platform=openai&type=oauth&status=active&page=1&page_size=1000' \
  -H "x-api-key: $OAIX_API_KEY"
```

Use `data.total` to decide whether the selected key's pool meets the desired
healthy-account count. A service key counts the platform admin's pool; a user's
key counts that user's own pool.
