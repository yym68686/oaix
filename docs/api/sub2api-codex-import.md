# sub2api Codex account import compatibility

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

No optional proxy/group preflight, export/list API, or sub2api JWT login is
implemented. Authentication uses OAIX keys throughout.
