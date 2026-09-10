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
  `dataset_ids: Option<Vec<String>>` — omitted/`null` means unrestricted on
  creation or "leave unchanged" on update (today's "whole tenant" behavior),
  a bare empty array (`[]`) is rejected as invalid everywhere (there is a
  dedicated `clear_dataset_restriction` flag for update instead — see
  `design.md` D1a), and a non-empty set restricts the key to exactly those
  datasets. Every surface that creates or updates a key (admin API,
  management API, SDK, CLI, MCP tools, the UI key-creation form) accepts and
  displays a set instead of a single value.
- OAuth consent: after the user picks a tenant, the consent screen presents
  an explicit "all datasets" (default, matches today's only behavior) vs.
  "only these datasets" choice, with a checklist that only appears — and
  only submits — for the second option (`design.md` D5); the chosen set (or
  "unrestricted") is bound to the issued authorization code, access token,
  and refresh token, the same way tenant is today, and a refresh preserves
  it by reading it back from the stored refresh-token record (D6).
- Enforcement: `Authenticator::authenticate_from_database`'s single-dataset
  comparison (`common/src/auth/authenticator.rs:391-397`) becomes a
  set-membership check, with an explicit, no-default-guessed rejection when
  a multi-dataset-restricted credential's request names no dataset at all
  (`design.md` D4). The OAuth path (`authenticate_oauth_token` /
  `resolve_user_tenant`), which today never restricts by dataset, gains the
  same check against the token's stored dataset set. A dataset-restricted
  credential also loses access to the management API entirely (`design.md`
  D9) — a narrower grant does not get a workaround path to widen itself by
  creating or updating other credentials — and the tenant self-service
  dataset-listing surfaces (`discover_datasets`, `whoami`) filter to the
  caller's restriction so a restricted credential can't enumerate datasets
  it cannot reach (`design.md` D10; the management API's own dataset
  listing needs no separate filter, since D9 already refuses it outright to
  any restricted credential).
- Config-based (TOML) tenants/keys are unaffected — `ApiKeyConfig` has no
  dataset restriction concept today and this change does not add one there;
  the feature is database-tenant-only, matching the existing scope of
  `dataset_id`.

**This change includes a BREAKING request-schema change**, marked as such
per OpenSpec convention: the `dataset_id` request field on every
create/update API-key endpoint and the OAuth `ConsentDecision` becomes
`dataset_ids`. This is deliberately a hard break, not a silent one: every
affected request DTO rejects an unrecognized `dataset_id` field outright
(a validation error naming the field and pointing at `dataset_ids`) rather
than deserializing permissively and dropping it — permissive deserialization
here is actively dangerous, not just an inconvenience, because a caller
that sent `dataset_id` meaning "restrict this key" would otherwise get an
**unrestricted** key back with no error at all. Every producer of a
create/update request must move to `dataset_ids` before this change reaches
it; there is no silent-fallback path. The blast radius is deliberately
narrowed on the read side only: responses keep
a deprecated, best-effort `dataset_id` field for one release (`design.md`
D8) so an existing reader that only ever saw a single dataset or
"unrestricted" sees no change; an existing *key's* stored restriction and
an existing *OAuth token*'s (lack of one) both keep working with identical
enforcement without any caller action, via the dual-read/dual-write
migration in `design.md` D2.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `api-key-management`: "Scopes are selectable on every key-management
  surface" and "An existing key's scopes can be updated" gain a dataset
  *set* restriction (replacing the single optional one); a new requirement
  defines set semantics (omit/`null` = unrestricted or unchanged, `[]`
  rejected everywhere, `clear_dataset_restriction` for the update-only
  clear case) and requires every named dataset to belong to the target
  tenant.
- `mcp-oauth`: "Authorization with human login and consent-time tenant
  selection" is modified in place to add the all-vs-restricted dataset
  choice at consent (D5), rather than added as a separate requirement, so
  the one consent step is described once; "Token issuance with PKCE and
  refresh" carries the set through refresh, reading it from the refresh
  token's own stored record rather than the prior access token (D6);
  "Tenant is bound to the token and absent from the agent surface" gets a
  sibling dataset-set requirement.
- `mcp-tool-surface`: the API-key and tenant-API-key tool families
  (`create_api_key`, `update_api_key_scopes`, `tenant_create_api_key`,
  `tenant_update_api_key`) take `dataset_ids`/`clear_dataset_restriction`
  instead of `dataset_id`; `discover_datasets` is called out as exempt from
  the tool family's "tenant and dataset are required" rule (it always was,
  in the actual implementation — the wording is tightened here since this
  change is the reason it's being re-read closely) and, along with
  `tenant_list_tables`, filters its results to a restricted caller's
  dataset set (D10).
- `api-key-management`'s existing "A tenant:manage scope grants tenant
  self-management to API keys" requirement (`openspec/specs/
  api-key-management/spec.md:150-188`) gains a carve-out: a key carrying
  `tenant:manage` AND a non-empty `dataset_ids` restriction loses management
  access entirely rather than the two combining (D9) — the same rule
  extends, in `mcp-oauth`'s existing token-boundary requirement, to a
  dataset-restricted OAuth-authenticated human session (`tenant:manage`
  itself already can't be granted through OAuth consent, but a tenant-admin
  *role* reaches the management API through a separate path `can_manage`
  also authorizes, which D9 closes the same way).

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
  becomes `api_key_dataset_ids: Option<Vec<String>>`, and its
  `with_api_key_restrictions` callers fan out beyond `authenticator.rs` into
  `acceptor/src/lib.rs`, `acceptor/src/handler/prometheus_handler.rs`, every
  `acceptor/src/services/otlp_*_service.rs`, `router/src/read_scope.rs`,
  `router/src/endpoints/query.rs`, and `router/src/endpoints/discovery.rs`
  (D3) — a rename, not new logic, but real compile-time fan-out. A new
  `[auth].dataset_restriction_rollout_complete` config key (default
  `false`) gates the mixed-version-unsafe cases (D2's operational
  constraint) at the request boundary rather than relying on operator
  discipline alone.
- **router**: `endpoints/admin.rs`, `endpoints/management.rs` (create/update
  API key handlers, response mapping, and `authorize_tenant`/`can_manage`
  refusing a dataset-restricted principal per D9), `endpoints/oauth.rs`
  (`ConsentContext` gains each tenant's datasets, `ConsentDecision` gains
  `dataset_ids`, token issuance and refresh both persist/propagate it per
  D6), `endpoints/session.rs` (`whoami` filters its dataset list per D10);
  OpenAPI descriptions; regenerate SDK + UI client after each schema change
  (not only once at the end).
- **signaldb-sdk**: regenerated from the OpenAPI spec (`dataset_id` →
  `dataset_ids` plus the new `clear_dataset_restriction` field on the
  relevant request types, a deprecated `dataset_id` alongside `dataset_ids`
  on response types per D8) — do not hand-edit.
- **signaldb-cli**: `commands/api_key.rs` (`--dataset` becomes repeatable,
  plus a new `--clear-dataset-restriction` flag on `update` — a plain
  repeatable flag can't distinguish "not passed" from "explicitly cleared").
- **mcp-server**: `CreateApiKeyParams`, `UpdateApiKeyScopesParams`,
  `TenantCreateApiKeyParams`, `TenantUpdateApiKeyParams` (`dataset_id` →
  `dataset_ids: Option<Vec<String>>`, the two update-params structs also
  gain `clear_dataset_restriction: bool`); `discover_datasets` and
  `tenant_list_tables` filter to the caller's restriction (D10).
- **src/ui**: API-key creation/update form's single dataset selector becomes
  a multi-select with an explicit clear affordance; the OAuth consent page
  (`features/consent/ConsentView.tsx`) gains the all-vs-restricted choice
  per tenant, checklist only rendered (and only enabled to submit) in
  restricted mode (D5).
- **tests-integration**: dataset-set enforcement e2e for both API keys and
  OAuth tokens (allowed dataset succeeds, disallowed dataset is refused,
  unrestricted key/token reaches every dataset, a multi-element restriction
  with no request dataset is rejected per D4, a dataset-restricted
  `tenant:manage` key is refused by the management API per D9);
  `query_parity.rs` unaffected (no new/removed operations, only field shape
  changes).
- **docs/skills**: `docs/users/authentication.md`, `docs/users/mcp.md`,
  `.claude/skills/multi-tenancy`.
