## Context

See proposal.md — Why. Current state, verified in code:

- `api_keys` table: SQLite `common/src/catalog.rs:198-232` (`dataset_id TEXT`
  column at 203, backfilled via `has_api_key_column`/`ALTER TABLE ... ADD
  COLUMN` at 218-221), Postgres mirror `catalog.rs:525-546` (column at 530,
  `ADD COLUMN IF NOT EXISTS` at 538-540). `scopes` on the same table is
  already a JSON-array-in-TEXT column (`catalog.rs:2202-2207`, decoded via
  `decode_json_vec_opt`) — the precedent this change follows for
  `dataset_ids` rather than a join table.
- Enforcement: `Authenticator::authenticate_from_database`
  (`auth/authenticator.rs:359-430`), exact single-dataset comparison at
  391-397, resolution order `dataset_id.or(api_key.dataset_id.as_deref())`
  then tenant default (398-405). Config-based tenants go through
  `resolve_dataset` (`authenticator.rs:433-467`) instead — `ApiKeyConfig`
  (`config/mod.rs:793-798`) has no dataset field at all, so config keys are
  and remain unrestricted; this change does not touch that path.
  `TenantContext.api_key_dataset_id: Option<String>`
  (`auth/mod.rs`), set via `with_api_key_restrictions` and read wherever a
  dataset-scoped request is authorized.
- OAuth: `authenticate_oauth_token` (`authenticator.rs:192-244`) resolves
  tenant only, via `resolve_user_tenant` (276-356); line 243 hardcodes
  `with_api_key_restrictions(Some(record.scopes), None)` — no dataset
  restriction is possible today because none is stored. `oauth_clients`,
  `oauth_authorization_codes`, `oauth_access_tokens`, `oauth_refresh_tokens`
  (SQLite `catalog.rs:406-482`, Postgres `catalog.rs:710-782`) carry
  `tenant_id`/`scopes`/`resource` but no dataset column.
- Consent: `GET /oauth/consent/context` → `ConsentContextResponse {
  client_name, tenants: Vec<ConsentTenant> }` (`router/src/endpoints/
  oauth.rs:614-621`, handler 640-715) — no dataset list, not even for
  display. `POST /oauth/authorize/decision`'s `ConsentDecision`
  (`oauth.rs:395-417`) has no dataset field. UI `src/ui/src/features/
  consent/ConsentView.tsx` renders/selects `context.tenants` only
  (84, 157, 169, 197-219).
- No migrations framework exists anywhere in the repo (`sqlx::migrate!`/
  `MIGRATOR`: zero hits). Schema changes are additive inline DDL in
  `Catalog::init()`, guarded to be idempotent on every boot — `CREATE TABLE
  IF NOT EXISTS` for new tables, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
  on Postgres, a `PRAGMA table_info` gate on SQLite (no native `IF NOT
  EXISTS` for columns there).

## Goals / Non-Goals

**Goals:** an API key or OAuth grant can be restricted to a named set of
datasets within its tenant; the existing single-dataset and whole-tenant
behaviors keep working unchanged as the two boundary cases of the new model
(one-element set, unset/empty-meaning-unrestricted); every surface that
creates/updates/displays a key or drives OAuth consent exposes the set.

**Non-Goals:** cross-tenant restriction (a key/token is still bound to
exactly one tenant, unchanged); config-based (TOML) tenant API keys gaining
any restriction (they have none today, on either axis, and stay that way);
changing what scopes exist or how they're validated; changing the OAuth
consent UI's tenant-picker mechanics themselves (D1 in
`mcp-oauth-dcr`/`management-api-key-scope`, unaffected).

## Decisions

**D1 — Representation: `dataset_ids: Option<Vec<String>>`, `None` =
unrestricted.** Mirrors how `scopes` already round-trips as a JSON array in
a TEXT column (`catalog.rs:2202-2207`), so the storage change is additive
and low-risk: one new/renamed column per table, decoded with the existing
`decode_json_vec_opt` helper. `Some(vec![])` denying every dataset is a
foot-gun with no legitimate use case, so it is never given that meaning (see
D1a for what it means instead, which differs between create and update).

**D1a — `dataset_ids: Some(vec![])` means different, both well-defined,
things on create vs. update.** On **create** there is no prior state to
target, so an empty explicit set is simply invalid input — rejected with a
validation error. On **update**, three distinct intents need three distinct
wire values, and today's single-`dataset_id` update endpoint actually can't
express "clear the restriction" at all (`Option<String>` collapses "not
provided" and "explicitly clear" into the same `None`, and
`update_api_key_scopes`'s `COALESCE(?, dataset_id)` leaves the column
untouched when the parameter is `None` — verified in
`catalog.rs:2348+/2363/2374`): omitting `dataset_ids` leaves the key's
current restriction unchanged (today's behavior, kept); a non-empty set
replaces it entirely; and — newly, since sets make the missing "clear it"
case worth fixing — an explicit empty set `[]` clears the restriction back
to unrestricted. This needs no double-`Option` JSON trick: create and update
are different operations with different valid vocabularies for the same
wire shape, exactly as `scopes: []` already means "no scopes" only in
contexts where scopes are required to be non-empty at creation.

**D2 — Migration: rename the column, backfill from the old single value.**
`dataset_id TEXT` → `dataset_ids TEXT` (JSON array) on `api_keys`, following
the exact `has_api_key_column`/`ALTER TABLE ADD COLUMN` pattern already used
for `dataset_id` itself (SQLite `catalog.rs:198-232`, Postgres `525-546`): add
`dataset_ids`, backfill any existing non-null `dataset_id` as
`["<value>"]`, leave `dataset_id` in place unread by new code (dropping a
column needs care on SQLite — a follow-up cleanup, not blocking this change).
`oauth_authorization_codes`/`oauth_access_tokens`/`oauth_refresh_tokens` each
gain a new nullable `dataset_ids TEXT` column (no prior data to backfill —
every existing token is `None`/unrestricted, which is exactly today's
behavior, so this is a pure no-op for tokens issued before the change).

**D3 — Enforcement is one shared helper, called from both auth paths.** Add
`fn dataset_allowed(restriction: Option<&[String]>, requested: &str) -> bool`
(`restriction.is_none() || restriction.unwrap().iter().any(|d| d ==
requested)`) in `common::auth`. Replace the inline comparison at
`authenticator.rs:391-397` with a call to it, and add the same call inside
`authenticate_oauth_token`'s tenant/dataset resolution — today that function
never looks at dataset at all, so this is new enforcement, not a
generalization of existing logic. `TenantContext.api_key_dataset_id:
Option<String>` becomes `api_key_dataset_ids: Option<Vec<String>>`
(`with_api_key_restrictions` signature updates to match; every call site is
in `authenticator.rs`, no fan-out).

**D4 — Resolution order when a request also carries an explicit dataset
becomes: explicit request dataset, checked against the restriction (reject
if outside it); no restriction and no explicit dataset falls through to
tenant default exactly as today.** Unlike the MCP tools' `tenant` argument
(a confirmation only, never a selector — #1441), the restriction set is a
real authorization boundary — this doesn't change the *meaning* of the
existing `X-Dataset-ID`/MCP `dataset` argument, only adds a check against it.

**D5 — Consent UI: datasets appear once a tenant is selected, default
unchecked = unrestricted.** Matches the existing tenant radio-list UX
(`ConsentView.tsx:197-225`) with a checkbox list underneath the chosen
tenant, sourced from a new `datasets: Vec<ConsentDataset>` field added to
`ConsentContextResponse` per tenant (mirrors `ConsentTenant`). Leaving every
box unchecked keeps today's "whole tenant" behavior — no user of an existing
connector sees a behavior change on reauthorization. *Alternative
considered:* require at least one dataset checked — rejected, it would make
every existing connector re-consent decision stricter than what it already
had, a regression disguised as a security default. This deliberately reads
`dataset_ids: []` differently from D1a's API-key-create rule: a consent
decision is a one-shot form submission with no "omit the field" affordance
and no prior state to distinguish "unchanged" from — every checkbox is
either checked or not, so empty is the only way the form can express "no
restriction," and that is exactly what it means here. API-key creation, by
contrast, is a programmatic call where omitting the field is available and
preferred, leaving `[]` free to be rejected as a caller mistake.

**D6 — Refresh preserves the grant exactly, same as tenant/scopes today.**
`issue_tokens`/refresh-grant handling copies `dataset_ids` from the
authorization code (or prior access token, on refresh) onto the new token
record — no re-consent, matching "Token issuance with PKCE and refresh"
behavior for tenant and scopes already.

**D7 — API surfaces: `dataset_id` request/response fields become
`dataset_ids`, a plain array.** Admin API (`endpoints/admin.rs:586-620`
create, `656-737` update, `752-761` response), management API
(`endpoints/management.rs:403-433`, `509-536`, `573-580`, `603-668`), CLI
(`signaldb-cli/src/commands/api_key.rs:30-32,44-46` — `--dataset` becomes a
repeatable flag, `Vec<String>`), MCP (`server.rs` — `CreateApiKeyParams`,
`UpdateApiKeyScopesParams`, `TenantCreateApiKeyParams`,
`TenantUpdateApiKeyParams`, each `dataset_id: Option<String>` →
`dataset_ids: Option<Vec<String>>`). SDK (`signaldb-sdk/src/generated.rs`) is
regenerated from the OpenAPI spec, never hand-edited — this is a breaking
Rust-type change for SDK consumers (CLI, MCP, tests-integration) but not a
wire-protocol break for existing callers who never set `dataset_id` (it was
optional and stays optional, just plural).

## Risks / Trade-offs

- [A key/token silently denied everything by a client sending `[]` where
  they meant "no restriction"] → D1a gives `[]` a single, non-denying
  meaning on both operations (rejected on create, "clear to unrestricted" on
  update) — it never means "deny every dataset" anywhere.
- [`dataset_id` column left in place after migration is dead weight] →
  acceptable for this change; a follow-up can drop it once every backend has
  run the backfill at least once (can't drop-and-recreate safely across a
  live SQLite deployment in one step).
- [Two independent enforcement call sites (API key, OAuth) could drift] →
  D3's shared `dataset_allowed` helper is the single source of truth; both
  paths call it, tested from both.
- [OAuth consent screen grows a second selection step] → D5 keeps it
  low-friction (all-unchecked = no change from today), and it only appears
  after a tenant is already chosen, so single-tenant users see one extra,
  skippable control.

## Migration Plan

Additive, in the existing `Catalog::init()` idempotent-DDL style (D2): new
columns land empty/backfilled on next boot, old `dataset_id` values keep
meaning what they meant via the backfill, no operator action required.
Rollback = revert; the new columns are simply unread by the old code path
they're rolled back to (SQLite/Postgres both tolerate an extra unused
column).
