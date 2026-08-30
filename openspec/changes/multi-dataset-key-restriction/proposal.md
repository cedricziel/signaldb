## Why

Followup to #1441 (one MCP session can now hold credentials for several
tenants, and every tool call must say explicitly which tenant/dataset it
targets). That change surfaced a gap one level down: a database-backed API
key can be restricted to at most **one** dataset (`dataset_id: Option<String>`
on `ApiKeyRecord`/`ApiKeyAuthRecord`), and an OAuth token issued through the
DCR consent flow is restricted only to a whole **tenant** — the consent
screen lets a user pick a tenant but has no dataset selection at all, so a
granted connector reaches every dataset in that tenant. An operator who wants
to hand out a credential for "production and staging, but not archive" has
no way to express that today; the only options are one dataset or the whole
tenant.

## What Changes

- API keys: the single optional `dataset_id` restriction becomes a set,
  `dataset_ids: Option<Vec<String>>` — unset means unrestricted (today's
  "whole tenant" behavior), an empty explicit set is rejected as invalid (use
  unset instead), and a non-empty set restricts the key to exactly those
  datasets. Every surface that creates or updates a key (admin API,
  management API, SDK, CLI, MCP tools, the UI key-creation form) accepts and
  displays a set instead of a single value.
- OAuth consent: after the user picks a tenant, the consent screen shows that
  tenant's datasets as a multi-select (default: none checked, meaning
  unrestricted — matches today's behavior so existing connectors reauthorizing
  see no surprise); the chosen set (or "unrestricted") is bound to the issued
  authorization code, access token, and refresh token, the same way tenant is
  today.
- Enforcement: `Authenticator::authenticate_from_database`'s single-dataset
  comparison (`common/src/auth/authenticator.rs:391-397`) becomes a
  set-membership check. The OAuth path (`authenticate_oauth_token` /
  `resolve_user_tenant`), which today never restricts by dataset, gains the
  same check against the token's stored dataset set.
- Config-based (TOML) tenants/keys are unaffected — `ApiKeyConfig` has no
  dataset restriction concept today and this change does not add one there;
  the feature is database-tenant-only, matching the existing scope of
  `dataset_id`.

No **BREAKING** changes: an existing key's single `dataset_id` migrates to a
one-element `dataset_ids` set with identical enforcement; an existing OAuth
token (issued before this change, no dataset set stored) is treated as
unrestricted, exactly as today.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `api-key-management`: "Scopes are selectable on every key-management
  surface" and "An existing key's scopes can be updated" gain a dataset
  *set* restriction (replacing the single optional one); a new requirement
  defines set semantics (unset = unrestricted, empty explicit set rejected).
- `mcp-oauth`: "Authorization with human login and consent-time tenant
  selection" gains dataset-set selection alongside tenant selection; "Token
  issuance with PKCE and refresh" carries the set through refresh; "Tenant is
  bound to the token and absent from the agent surface" gets a sibling
  dataset-set requirement.
- `mcp-tool-surface`: the API-key and tenant-API-key tool families
  (`create_api_key`, `update_api_key_scopes`, `tenant_create_api_key`,
  `tenant_update_api_key`) take `dataset_ids` instead of `dataset_id`.

## Impact

- **common**: `catalog.rs` — `api_keys.dataset_id` TEXT column becomes
  `dataset_ids` TEXT (JSON array, same pattern as the existing `scopes`
  column), both SQLite and Postgres branches of `Catalog::init()`; new
  `dataset_ids`-bearing columns on `oauth_authorization_codes`,
  `oauth_access_tokens`, `oauth_refresh_tokens` (none exist today).
  `ApiKeyRecord`/`ApiKeyAuthRecord` structs; `upsert_scoped_api_key`,
  `update_api_key_scopes`, `create_access_token`/`create_refresh_token`/
  `create_authorization_code` signatures. `auth/authenticator.rs`
  (`authenticate_from_database`, `authenticate_oauth_token`,
  `resolve_user_tenant`). `auth/mod.rs` — `TenantContext.api_key_dataset_id`
  becomes `api_key_dataset_ids: Option<Vec<String>>`.
- **router**: `endpoints/admin.rs`, `endpoints/management.rs` (create/update
  API key handlers, response mapping), `endpoints/oauth.rs` (`ConsentContext`
  gains each tenant's datasets, `ConsentDecision` gains `dataset_ids`, token
  issuance persists them); OpenAPI descriptions; regenerate SDK + UI client.
- **signaldb-sdk**: regenerated from the OpenAPI spec (`dataset_id` →
  `dataset_ids` on the relevant request/response types) — do not hand-edit.
- **signaldb-cli**: `commands/api_key.rs` (`--dataset` becomes repeatable).
- **mcp-server**: `CreateApiKeyParams`, `UpdateApiKeyScopesParams`,
  `TenantCreateApiKeyParams`, `TenantUpdateApiKeyParams` (`dataset_id` →
  `dataset_ids: Option<Vec<String>>`).
- **src/ui**: API-key creation/update form's single dataset selector becomes
  a multi-select; the OAuth consent page (`features/consent/ConsentView.tsx`)
  gains a per-tenant dataset multi-select shown once a tenant is chosen.
- **tests-integration**: dataset-set enforcement e2e for both API keys and
  OAuth tokens (allowed dataset succeeds, disallowed dataset is refused,
  unrestricted key/token reaches every dataset); `query_parity.rs` unaffected
  (no new/removed operations, only field shape changes).
- **docs/skills**: `docs/users/authentication.md`, `docs/users/mcp.md`,
  `.claude/skills/multi-tenancy`.
