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
  then tenant default (398-405) — verified against
  `database_key_on_config_tenant_resolves_catalog_dataset`
  (`authenticator.rs:877-934`), which pins that a key's own bound dataset
  wins over the tenant default when no header is sent. Config-based tenants
  go through `resolve_dataset` (`authenticator.rs:433-467`) instead —
  `ApiKeyConfig` (`config/mod.rs:793-798`) has no dataset field at all, so
  config keys are and remain unrestricted; this change does not touch that
  path. `TenantContext.api_key_dataset_id: Option<String>` (`auth/mod.rs`),
  set via `with_api_key_restrictions` and read wherever a dataset-scoped
  request is authorized.
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
- Management API: `authorize_tenant`/`can_manage` (`router/src/endpoints/
  management.rs:61-80`) authorize purely on role/scope (tenant-admin,
  instance-admin, or an API key with the `tenant:manage` scope) — a human
  session or OAuth token is treated identically to any other tenant-admin
  principal, with no notion of a narrower grant. `create_api_key`
  (`management.rs:501-554`) and `manage_delete_dataset`
  (`management.rs:374-407`) accept whatever the caller supplies with no
  reference to any restriction the caller's own credential might carry —
  there is nothing to reference today because OAuth tokens carry no dataset
  restriction, but this changes once D9 below exists.
- Enumeration surfaces that list datasets/tables without regard to any
  restriction: `whoami`/`GET /api/v1/whoami` (`router/src/endpoints/
  session.rs:455-496,513`), `manage_list_datasets`
  (`management.rs:235-243`), and the MCP `discover_datasets` tool
  (`mcp-server/src/server.rs`, reads `audit::CallerTenant` and lists every
  dataset via `list_tenant_tables`).
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
(one-element set, unset-meaning-unrestricted); every surface that
creates/updates/displays a key or drives OAuth consent exposes the set;
restricting a credential is enforced everywhere that credential can reach
data or administer the tenant, not only on the data-plane query path.

**Non-Goals:** cross-tenant restriction (a key/token is still bound to
exactly one tenant, unchanged); config-based (TOML) tenant API keys gaining
any restriction (they have none today, on either axis, and stay that way);
changing what scopes exist or how they're validated; changing the OAuth
consent UI's tenant-picker mechanics themselves (D1 in
`mcp-oauth-dcr`/`management-api-key-scope`, unaffected); letting a
dataset-restricted credential *also* hold full tenant-management power (D9
resolves the conflict by refusing the combination rather than reconciling
it).

## Decisions

**D1 — Representation: `dataset_ids: Option<Vec<String>>`, `None` =
unrestricted.** Mirrors how `scopes` already round-trips as a JSON array in
a TEXT column (`catalog.rs:2202-2207`), so the storage change is additive
and low-risk: one new column per table, decoded with the existing
`decode_json_vec_opt` helper.

**D1a — A bare empty array (`[]`) is invalid everywhere it could name a
dataset set; "clear" is its own explicit signal, never `[]`.** Giving `[]`
a context-dependent meaning (invalid on create, "clear" on update,
"unrestricted" from the consent form) is exactly the ambiguity CodeRabbit's
review and independent design review both flagged (a `null`-vs-empty-array
mixup is a well-known footgun, and it recurred at every call site in the
first draft of this proposal). Instead, one invariant holds system-wide,
on every surface (API-key create, API-key update, OAuth consent):

- `dataset_ids` omitted or explicit JSON `null` → unrestricted on create;
  **leave the current restriction unchanged** on update (there is nothing
  to leave unchanged on create, so "omit" only has one meaning there).
- `dataset_ids: [<one or more names>]` → restrict to exactly that set,
  replacing whatever the previous restriction was, on both create and
  update.
- `dataset_ids: []` → **rejected with a validation error**, unconditionally,
  on every surface. It is never interpreted as "unrestricted," "clear," or
  "deny everything." A caller that means "no restriction" omits the field or
  sends `null`; a caller that means "remove an existing restriction" uses
  the explicit clear signal below. Duplicate names within a non-empty set
  (e.g. `["production", "production"]`) are also rejected — a set is
  logically deduplicated already, and silently accepting duplicates would
  let a set that is semantically single-element (once deduplicated) slip
  past validation while still being treated as multi-element by D2's
  legacy-column projection (`ids.len() == 1`), which is exactly the kind of
  mismatch this proposal exists to close, not reopen.
- **Clearing an existing restriction back to unrestricted** (update only)
  is its own explicit boolean, `clear_dataset_restriction: bool` (default
  `false`), not a magic value of `dataset_ids`. The only two valid
  combinations are: `clear_dataset_restriction: false` (or omitted) with
  `dataset_ids` set to whatever the caller intends (omit/`null` = keep,
  non-empty = replace); or `clear_dataset_restriction: true` with
  `dataset_ids` omitted or explicit `null` (never a non-empty array — that
  specific combination, "clear" and "set" both requested at once, is the
  one rejected as contradictory). `clear_dataset_restriction: true` paired
  with `dataset_ids: null`/omitted is not a conflict, it is the expected
  shape of a clear request. At the Rust type level this is
  `enum DatasetRestrictionUpdate { Keep, Clear, Set(Vec<String>) }`,
  constructed once at the HTTP boundary from `(dataset_ids,
  clear_dataset_restriction)` and passed down to the catalog layer — see
  D2b. The HTTP-boundary validation and the catalog-layer enum construction
  must implement this exact same rule, not two independent approximations
  of it.

This is a genuine, deliberate change from the first draft (which read `[]`
as "clear" on update and "unrestricted" from consent): a single value can no
longer mean opposite things depending on which endpoint received it, and no
client can accidentally clear a restriction by sending `[]` where it meant
"no change" (the CLI failure mode CodeRabbit's review named directly).

**D2 — Migration: add `dataset_ids`, dual-read and dual-write against the
legacy `dataset_id` for as long as both may be observed.** `ALTER TABLE
api_keys ADD COLUMN IF NOT EXISTS dataset_ids TEXT` (SQLite via the existing
`has_api_key_column` gate, Postgres via `ADD COLUMN IF NOT EXISTS`),
following the exact pattern already used for `dataset_id` itself (SQLite
`catalog.rs:198-232`, Postgres `525-546`). The naive version of this
migration — backfill once, then have new code read only `dataset_ids` and
never touch `dataset_id` again — has two failure modes an independent
review caught: a restriction cleared under new code can be *resurrected* by
a later boot's idempotent backfill (which still sees the untouched legacy
`dataset_id` and rewrites `dataset_ids` from it), and a rollback to old code
silently reverts every key updated under new code to whatever its legacy
`dataset_id` said before the update (or to unrestricted, if it never had
one). Both are the new representation silently changing a live security
boundary, not just stale unread data. The fix:

- **Read:** if `dataset_ids` is non-NULL, it is authoritative. If it is
  NULL, derive from `dataset_id` exactly as today (`Some(vec![id])` or
  `None`). This is unconditional, not a one-time backfill — old and new
  representations both stay live inputs for as long as `dataset_id` exists.
- **Write:** every create or update under new code writes `dataset_ids`
  *and* keeps `dataset_id` in sync as a derived, best-effort projection:
  `dataset_id = ids[0]` when the new set has exactly one element,
  `dataset_id = NULL` otherwise (empty set is unreachable per D1a; zero
  elements never gets here). Clearing a restriction (D1a's explicit signal)
  writes `dataset_ids = NULL, dataset_id = NULL` — both columns, so there is
  nothing left for a later backfill to resurrect from.
- **Backfill** (one-time per row, run from `Catalog::init()`, already
  idempotent by construction): select every `api_keys` row where
  `dataset_id IS NOT NULL AND dataset_ids IS NULL`, and for each, `UPDATE`
  it with `dataset_ids` bound to `serde_json::to_string(&[dataset_id])` —
  the same pattern this codebase already uses everywhere else it writes a
  JSON-array-in-TEXT column (e.g. `scopes`, `catalog.rs:2202-2207,3459`):
  encode the array in Rust and bind it as a plain string parameter, never
  an in-SQL JSON constructor function. (An earlier draft of this design
  used a SQL-side `json_array(...)` call, which doesn't match this
  repo's `scopes` precedent and would need version-specific handling
  between SQLite's and Postgres's differing JSON function surfaces to work
  at all — reusing the existing Rust-side encoding sidesteps that entirely
  and keeps one code path for every JSON-array column in this table.) This
  only ever touches a row untouched since before this change (new code
  always leaves both columns consistent, so the `WHERE` clause never
  matches a key new code has written), which is what makes it safe to
  leave unconditional rather than gating it behind a one-shot marker this
  repo has no mechanism for.

**Residual, documented limitation — and it is wider than API keys alone.**
The legacy `dataset_id` column structurally cannot represent a
multi-element restriction, and dual-write only helps a credential type that
*has* a legacy column to write to. That's true for API keys, and it is
**not** true for OAuth tokens:

- **API keys:** dual-write keeps an *old*-code node's view of any key
  correct for the two cases old code understands (unrestricted, single
  dataset) — including keys created or updated under new code — but a key
  restricted to two or more datasets is seen as **unrestricted** by an
  old-code node, because `dataset_id` for such a key is `NULL` and old
  code has no other column to consult. Single-dataset and unrestricted
  keys are safe throughout a mixed-version rollout and safe to roll back
  at any point; only a genuinely new multi-element restriction requires
  the rollout to finish first.
- **OAuth tokens:** there is no legacy `dataset_id` column on
  `oauth_access_tokens`/`oauth_refresh_tokens`/`oauth_authorization_codes`
  to dual-write against, because dataset restriction never existed for
  OAuth before this change — there is nothing for old code to have been
  reading. This is a stricter problem than the API-key case, not an
  exempt one (an earlier draft of this design claimed OAuth was exempt
  from the mixed-version constraint; that claim was wrong and is
  retracted here): an old `authenticate_oauth_token` binary doesn't
  consult any dataset restriction at all — it wasn't checking one before,
  and it has no code path added for one — so a request authenticated by a
  *new-code-issued*, dataset-restricted token that happens to be served by
  an *old-code* node (a plausible load-balanced router replica during a
  rolling deploy) is treated as fully unrestricted, for **any** restriction
  size, not only a multi-element one.

The combined operational constraint, stated once: **do not issue or update
a credential to carry a non-empty dataset restriction until every node
that authenticates that credential type is running the new binary** — for
API keys, this only matters once the restriction has two or more elements
(single-element and unrestricted are always safe, rollback included); for
OAuth tokens, it matters for *any* non-empty restriction, because no old
binary has ever enforced one. In practice this means: complete the OAuth
consent-flow deployment (router) before enabling the "only these datasets"
choice on the consent screen in production, or accept that a
dataset-restricted connector authorized mid-rollout may reach more than it
was granted until the rollout finishes.

**D2b — The tri-state update at the catalog layer.** Today's
`update_api_key_scopes` takes `dataset_id: Option<&str>` and writes it with
`COALESCE(?, dataset_id)` (`catalog.rs:2348-2384`) — a two-state
parameter that can express "leave unchanged" (`None`) or "set" (`Some`),
but never "clear," because `COALESCE` never lets a caller write `NULL` over
an existing value. D1a's `DatasetRestrictionUpdate` enum is not just a
handler-layer convenience: the catalog function's SQL has to change shape
to accept it, from a single nullable parameter with `COALESCE` to a branch
per variant (`Keep` → don't touch the columns at all; `Clear` → `SET
dataset_ids = NULL, dataset_id = NULL`; `Set(ids)` → `SET dataset_ids =
?, dataset_id = ?` with the D2 projection applied). Test all three
variants directly against the catalog, not only through the HTTP layer —
this is exactly the gap that let the first draft's "`Some(vec![])` is
rejected at the catalog layer" phrasing paper over the fact that a
`COALESCE`-shaped column can't express "clear" at all, at any layer.

**D3 — Enforcement is one shared helper, called from both auth paths.** Add
`fn dataset_allowed(restriction: Option<&[String]>, requested: &str) -> bool`
(`restriction.is_none() || restriction.unwrap().iter().any(|d| d ==
requested)`) in `common::auth`. Replace the inline comparison at
`authenticator.rs:391-397` with a call to it, and add the same call inside
`authenticate_oauth_token`'s tenant/dataset resolution — today that function
never looks at dataset at all, so this is new enforcement, not a
generalization of existing logic. `TenantContext.api_key_dataset_id:
Option<String>` becomes `api_key_dataset_ids: Option<Vec<String>>`
(`with_api_key_restrictions` signature updates to match). Call sites are
not confined to `authenticator.rs`: `TenantContext` is also constructed
directly (not through `with_api_key_restrictions`) in the acceptor crate's
ingest paths (`acceptor/src/lib.rs:1139-1154`,
`acceptor/src/handler/prometheus_handler.rs:573`, and each
`acceptor/src/services/otlp_*_service.rs` OTLP handler) and in the router's
own read-scope/query/discovery endpoints
(`router/src/read_scope.rs:57`, `router/src/endpoints/query.rs:1301`,
`router/src/endpoints/discovery.rs:474`) — every one of these needs its
`api_key_dataset_id: None` literal updated to `api_key_dataset_ids: None`
and re-verified to compile; they don't need new logic since the acceptor
paths never had a restriction to enforce differently, but a rename this
size touching a struct field used across two crates fans out further than
`authenticator.rs` alone.

**D4 — Resolution order when the request carries no explicit dataset and
the credential carries a multi-element restriction is a rejection, not a
guess.** The full order, replacing the single sentence in the first draft
that only covered "no restriction" and "explicit dataset given":

1. An explicit request dataset (header or MCP `dataset` argument) is always
   checked against the restriction via `dataset_allowed`; outside the set
   is refused, regardless of how many elements the restriction has. This is
   unchanged from the first draft.
2. No explicit request dataset, **no restriction** → tenant default, exactly
   as today.
3. No explicit request dataset, restriction is a **single-element set**
   → resolve to that element. This is not new behavior — it is today's
   exact behavior for a single-`dataset_id`-bound key
   (`authenticator.rs:877-934` pins it), preserved because a legacy key
   migrates to a one-element set under D2 and must keep working identically
   without a header.
4. No explicit request dataset, restriction has **two or more elements** →
   reject with a client error naming the tenant and asking the caller to
   specify `X-Dataset-ID` (HTTP) or the `dataset` argument (MCP) explicitly.
   There is no principled default among several allowed datasets, and
   silently picking one (the tenant default if it happens to be in the set,
   or the first element) is exactly the kind of ambiguity that turns into a
   security assumption nobody wrote down. This case cannot arise for any
   credential created under the old single-`dataset_id` model; it only
   exists once an operator deliberately grants a multi-dataset restriction,
   at which point every one of that credential's callers must already know
   to send a dataset.

Unlike the MCP tools' `tenant` argument (a confirmation only, never a
selector — #1441), the restriction set is a real authorization boundary —
this doesn't change the *meaning* of the existing `X-Dataset-ID`/MCP
`dataset` argument, only adds a check against it and, per case 4, sometimes
requires it where it was previously optional-with-a-default.

**D5 — Consent UI: an explicit all-vs-restricted choice, not a checklist
that means "everything" when empty.** The first draft's "checkbox list,
default unchecked = unrestricted" was flagged by both reviews as an
inverted default: a user who checks two datasets, reconsiders, and
unchecks both — intending to pause, not to change their mind about scope —
approves a grant to the *whole tenant*, silently wider than what they were
just looking at. The corrected UX is a two-state choice per tenant, once a
tenant is selected (still matching the existing tenant radio-list UX,
`ConsentView.tsx:197-225`):

- **"All datasets in &lt;tenant&gt;"** — selected by default, matching
  today's only behavior exactly (no existing connector sees a change on
  reauthorization). Submits `dataset_ids: null` (omitted).
- **"Only these datasets:"** — reveals a checklist of that tenant's
  datasets (sourced from a new `datasets: Vec<ConsentDataset>` field on
  `ConsentContextResponse` per tenant, mirroring `ConsentTenant`). Selecting
  this mode with zero boxes checked is a client-side validation error (the
  submit control is disabled) and, redundantly, a server-side one (D1a: an
  empty array is rejected everywhere, consent included — the server does
  not trust the client's disabled-button enforcement alone). Submits
  `dataset_ids: [<checked names>]`, always non-empty by construction.

This keeps the low-friction default (no user of an existing connector is
forced to touch the new control) while making "restrict" and "everything"
two states a user actively picks between, rather than "everything" being
what happens when a checklist is left empty. `ConsentDecision.dataset_ids`
is `Option<Vec<String>>` and MUST carry `#[serde(default)]` so that a
consent request from a client built before this change (which omits the
field entirely) continues to work exactly as it does today, rather than
422ing on a newly-required field.

**D6 — Refresh reads the restriction from the stored refresh-token record,
not the prior access token, and propagates it to *both* tokens the refresh
mints.** The first draft said refresh "copies `dataset_ids` from the
authorization code (or prior access token, on refresh)" — but a refresh
request only guarantees the presence of a valid refresh token; the access
token it was issued alongside may already be expired or otherwise
unavailable, so there is nothing reliable to copy *from* on that path.
D2's new `dataset_ids` column on `oauth_refresh_tokens` is the actual
source of truth for a refresh. The existing refresh-grant handler
(`router/src/endpoints/oauth.rs`) revokes the presented refresh token and
mints a *replacement pair* — a new access token and a new refresh token,
matching how tenant and scopes already rotate — so `dataset_ids` has to
propagate to both of those, not only the access token: the
authorization-code path copies the code's `dataset_ids` onto both the
initial access token and the initial refresh token at issuance (unchanged
from the first draft); the refresh-grant path copies `dataset_ids` from
the *presented* `oauth_refresh_tokens` row onto both the new access token
and the new replacement refresh token it mints, so a second, later refresh
of the same lineage sees the restriction again from the row it now reads.
No re-consent either way, matching "Token issuance with PKCE and refresh"
behavior for tenant and scopes already.

**D7 — API surfaces: `dataset_id` request/response fields become
`dataset_ids`, a plain array — this is a breaking change for direct HTTP
and unregenerated-SDK callers, and is marked as such (see D8 for the
mitigation on responses).** Admin API (`endpoints/admin.rs:586-620` create,
`656-737` update, `752-761` response), management API
(`endpoints/management.rs:403-433`, `509-536`, `573-580`, `603-668`), CLI
(`signaldb-cli/src/commands/api_key.rs:30-32,44-46` — `--dataset` becomes a
repeatable flag, `Vec<String>`, plus a new `--clear-dataset-restriction`
flag on `Update` per D1a), MCP (`server.rs` — `CreateApiKeyParams`,
`UpdateApiKeyScopesParams`, `TenantCreateApiKeyParams`,
`TenantUpdateApiKeyParams`, each `dataset_id: Option<String>` →
`dataset_ids: Option<Vec<String>>`, plus `clear_dataset_restriction: bool`
on the two update-tool params). SDK (`signaldb-sdk/src/generated.rs`) is
regenerated from the OpenAPI spec, never hand-edited.

**D8 — Responses keep a deprecated, best-effort `dataset_id` field for one
release; requests do not, and a legacy request field is rejected loudly,
never absorbed silently.** D7's request-side rename is unavoidably
breaking, and this proposal picks the loud failure mode deliberately: every
create/update request DTO (`CreateApiKeyRequest`, `UpdateApiKeyRequest`,
`ConsentDecision`) rejects an unrecognized `dataset_id` field with a
validation error naming it and pointing at `dataset_ids`, rather than
deserializing permissively and dropping the field. The alternative —
silently ignoring an unknown `dataset_id` field — is not merely
unhelpful, it is actively dangerous here: a caller sending `dataset_id`
meaning "restrict this key to one dataset" would otherwise get back an
**unrestricted** key with no error, the exact opposite of what it asked
for. This proposal does not soften the break — every producer of a
create/update request must move to `dataset_ids`. Responses are different:
add `dataset_id: Option<String>` back onto `ApiKeyResponse`/
`CreateApiKeyResponse`/`ManageApiKeyResponse`/`ManageCreatedApiKey`,
computed as `dataset_ids.as_ref().filter(|v| v.len() == 1).map(|v|
v[0].clone())` — `Some` for exactly the single-dataset and unrestricted
(`None`) cases every pre-existing reader already understands, `None` for a
genuinely new multi-element restriction (which such a reader had no way to
represent anyway). This is scaffolding, not a permanent dual-field API:
`tasks.md` 7.2 files the follow-up to drop it once callers have migrated,
and the OpenAPI description marks it deprecated from the day it ships.

**D9 — A dataset-restricted credential cannot use the management API at
all, regardless of role or scope.** The management API
(`authorize_tenant`/`can_manage`, `management.rs:61-80`) grants full
tenant administration — creating other API keys with arbitrary scopes and
restrictions, deleting datasets, managing memberships — to any principal
with the tenant-admin role, the instance-admin flag, or a key/token
carrying `tenant:manage`. Once OAuth tokens can carry a dataset
restriction, an unmodified `can_manage` lets a token consented to
`["production"]` only call `tenant_create_api_key` with no `dataset_ids`
and receive a key restricted to nothing — administering the whole tenant
through a grant that was supposed to be narrower. This is not a gap in
enforcement *coverage* the way D3's data-plane check is; it's a
capability the management API was never designed to narrow, and
retrofitting per-operation dataset scoping onto a dozen endpoints
(`tenant_create_dataset`, `tenant_delete_dataset`, `tenant_create_api_key`,
`tenant_upsert_membership`, `tenant_get_schema`, ...) is a materially larger
change than this proposal's stated scope. The decision: `can_manage`
returns `false` whenever the principal's own credential carries a
non-empty dataset restriction, full stop — a restricted grant gets the
data-plane access it asked for and nothing that administers the tenant.
*Alternative considered:* allow the subset of management operations that
are themselves dataset-scoped (create/delete a dataset within the caller's
own set, create a key whose own restriction is a subset of the caller's)
— rejected for this change as materially larger scope than the stated
gap (a restriction was never expected to interact with `tenant:manage` at
all; today's only credential that can carry both is nonexistent, since
API keys are the only thing with a restriction today and `tenant:manage`
plus `dataset_id` is a combination nothing prevents but that has no
existing user to preserve behavior for), tracked as a possible follow-up
if a real need for "manage only my slice of the tenant" shows up.

**D10 — Restricted credentials see only their own datasets in every
listing they can still reach.** `discover_datasets` (MCP) and `whoami`
(`GET /api/v1/whoami`) today return every dataset in the tenant
unconditionally. Once a credential can be restricted, an unfiltered
listing leaks the *names* of datasets the credential cannot query — not
their data, but their existence, which is information the restriction was
meant to withhold. Both of these are tenant *self-service* endpoints
(`/api/v1/whoami`, and `discover_datasets`'s underlying `list_tenant_tables`
call) reachable by any valid tenant credential, not gated by `can_manage`
— which is exactly why they need their own filter rather than relying on
D9. `manage_list_datasets` (`management.rs:235-243`) is deliberately
**not** in this list: it lives under `/api/v1/manage`, so D9 already
refuses it outright for any dataset-restricted credential before a
listing (filtered or not) would ever be computed — filtering it here
would be dead code behind an already-closed door, and would wrongly imply
a restricted `tenant:manage` key can reach the management API in a
reduced capacity, which D9 rules out entirely. Each of the two remaining
call sites filters its dataset (and, for `discover_datasets`, per-dataset
table-count) list through the caller's `api_key_dataset_ids`/token
restriction when one is present; unrestricted credentials see the
unfiltered list, unchanged. `discover_datasets` itself takes no `dataset`
argument in the existing implementation and is not subject to the
`tenant`/`dataset`-required rule `mcp-tool-surface`'s "MCP tools cover the
full client capability set" requirement states for query/schema-lookup
tools (that rule was written before this proposal and, on inspection,
never applied to `discover_datasets` in the actual tool signature — this
change tightens the requirement's wording to say so explicitly, since
CodeRabbit's review read the existing text as implying otherwise).

## Risks / Trade-offs

- [A key/token silently denied everything, or silently made unrestricted,
  by a client that meant something else with `[]`] → D1a removes every
  context-dependent meaning of a bare empty array; it is rejected
  everywhere, and "clear" and "unrestricted" each have their own explicit,
  unambiguous signal instead.
- [`dataset_id` column stays load-bearing indefinitely as a dual-write
  target] → acceptable and deliberate: D2's dual-write is what makes
  rollback and mixed-version operation safe, not incidental debt. Dropping
  it is future work gated on every node running new code (documented as
  the mixed-version constraint in D2), not on this change landing.
- [Two independent enforcement call sites (API key, OAuth) could drift] →
  D3's shared `dataset_allowed` helper is the single source of truth for
  the data-plane check; D9 closes the separate control-plane gap that
  `dataset_allowed` alone can't reach.
- [OAuth consent screen grows a second selection step] → D5 keeps it
  low-friction (the default radio choice matches today's only behavior
  exactly), and it only appears after a tenant is already chosen, so
  single-tenant users see one extra, skippable control.
- [A multi-dataset restriction is unsafe to create mid-rollout] → documented
  explicitly in D2 as an operational constraint, not silently risked: single
  -dataset and unrestricted credentials are safe throughout a mixed-version
  deploy and a rollback at any point; only genuinely new multi-element
  restrictions require the rollout to finish first.
- [`dataset_id` response field removal breaks an existing reader] → D8's
  deprecated derived field covers every case such a reader already
  understood (single dataset, unrestricted); only a caller that starts
  creating multi-element restrictions needs to move to reading
  `dataset_ids`, and by then it has necessarily already moved to writing
  it.

## Migration Plan

Additive, in the existing `Catalog::init()` idempotent-DDL style: new
columns land on next boot; the one-time backfill (D2) only ever touches a
row untouched since before this change, because new code keeps both
columns consistent on every write it makes. Rollback = revert to the prior
binary; because of D2's dual-write, old code's reads of `dataset_id` remain
correct for every key it created or updated (unrestricted and
single-dataset), and for every key new code touched *unless* it was set to
a multi-element restriction, per the documented mixed-version limitation
above. Operators completing a rollout should avoid creating or updating any
key/token to a multi-element restriction until it is complete; nothing else
about this migration requires a maintenance window or manual intervention.
