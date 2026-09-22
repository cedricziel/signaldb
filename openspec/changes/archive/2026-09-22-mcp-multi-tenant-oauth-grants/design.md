## Context

See `proposal.md` for motivation. The current implementation, concretely:

- `oauth_authorization_codes`, `oauth_access_tokens`, and `oauth_refresh_tokens`
  (`src/common/src/catalog.rs`) each carry a single `tenant_id TEXT NOT NULL`
  (FK to `tenants(id) ON DELETE CASCADE`) plus one `dataset_ids TEXT` JSON
  column. `OAuthTokenRecord` mirrors this as `tenant_id: String`.
- The consent endpoints (`src/router/src/endpoints/oauth.rs`) build one
  authorization code from one `tenant_id` posted by the consent form; token
  issuance and refresh copy that single `tenant_id`/`dataset_ids` straight
  through.
- `GET /api/v1/whoami` and every other tenant-scoped route (query, `/manage`,
  `/schema`, `/connection`) are mounted under the same `.nest("/api/v1",
...).layer(auth_layer)` (`src/router/src/lib.rs`), so they all go through
  the identical generic `common::auth::middleware::auth_middleware` →
  `Authenticator::authenticate_oauth_token` → `TenantContext` pipeline.
  `TenantContext.tenant_id` is a mandatory, non-optional `String`
  (`src/common/src/auth/mod.rs`): there is no "authenticated, but no tenant
  resolved yet" outcome anywhere in this pipeline today, for any credential
  type. `authenticate_oauth_token` resolves it entirely from the token row;
  `extract_auth_headers`'s OAuth branch (`src/common/src/auth/middleware.rs`)
  deliberately returns early without ever reading `X-Tenant-ID` for an OAuth
  bearer ("neither required nor consulted") — the header is dropped before
  it reaches the authenticator at all.
- The MCP server's `mcp_auth_middleware` (`src/mcp-server/src/lib.rs`) calls
  the router's `whoami()` **once per HTTP request, before the JSON-RPC body
  is parsed**, using only the inbound `Authorization`/`X-Tenant-ID` headers.
  For an OAuth bearer it sends no `X-Tenant-ID` and stores the single
  resolved tenant as `audit::CallerTenant`. Every tool's `tenant` argument is
  then checked in `check_tenant_scope` (`server.rs`) against that one value
  — a local confirmation, not a selector — and the actual downstream router
  call reuses the same forwarded headers (`FORWARDED_HEADERS` =
  `["authorization", "x-tenant-id", "x-dataset-id"]`).
- Consequence 1: the MCP middleware cannot derive "which tenant is this call
  for" from the tool call's JSON arguments, because it runs before the body
  is parsed. Any design that lets one OAuth credential reach several tenants
  has to resolve the tenant **inside the tool handler**, after the argument
  is known — not in the request-level middleware.
- Consequence 2: because `TenantContext.tenant_id` is mandatory everywhere,
  there is no route-agnostic way to make exactly one endpoint (`whoami`)
  succeed with "no tenant resolved, here is the whole set" while every other
  endpoint behind the same generic middleware keeps requiring exactly one.
  Making `tenant_id` optional to accommodate that one route would ripple
  into every one of the dozens of handlers that currently assume a concrete
  tenant. This change does not do that (see D4).

## Goals / Non-Goals

**Goals:**

- Let one OAuth-authorized MCP connector (one URL, one claude.ai/ChatGPT
  connector entry) reach every tenant a human user consents to, without a
  second connector.
- Keep single-tenant grants — the overwhelming majority, and everything
  issued before this ships — byte-for-byte behaviorally identical: no new
  header, no new failure mode, no consent-screen change beyond checkboxes
  replacing radio buttons.
- Keep the router as the sole authority: the MCP server's per-call tenant
  check is a client-side courtesy, never the enforcement point.
- Never let one tenant's deletion revoke access to other tenants that
  happen to share a multi-tenant grant.

**Non-Goals:**

- Changing API-key authentication, Claude Code's header-based multi-identity
  session path, or anything about ingest/query execution/storage.
- A "switch active tenant" stateful MCP tool. The `tenant` argument every
  tool already requires is sufficient once it can select, so no new
  session-side state machine is needed.
- Per-tenant connector URLs (considered and rejected — see Decisions).
- Making `TenantContext`/`authenticate_oauth_token` support an "authenticated
  but no tenant chosen" outcome. Every resource-API route keeps the
  existing contract: success always means exactly one concrete tenant.
- A CLI/human-friendly "list my OAuth tenants" experience. OAuth tokens are
  the Claude.ai/ChatGPT connector credential, not something pasted into
  `signaldb-cli` (which uses API keys); grant discovery is a
  machine-to-machine concern solved by the new introspection endpoint (D4).

## Decisions

### D1: Generalize the grant to a set, not per-tenant connector URLs

Considered instead: mint each tenant its own MCP resource path (e.g.
`/mcp/tenants/{id}`) so claude.ai treats each as a distinct connector,
leaving the one-tenant-per-token model untouched. Rejected for this change
because it only works for clients whose "connector" concept is keyed by
URL; it does nothing for a client that additionally cannot register the
same server twice for any reason, and it does not give a human a single
sign-in that already reflects every tenant they belong to (they'd
separately add, sign in to, and consent on N connectors, once per tenant,
forever). The set-based grant fixes the actual constraint — one connector,
every tenant the user chooses — and composes with per-tenant URLs later if
some other host needs that instead. Not mutually exclusive, just not
needed to solve the stated problem.

### D2: One JSON grant column per table, no DB-level FK on grant tenants

**Revised twice from earlier drafts** — first from three new FK'd child
tables (see below for why that was rejected), then from an
`ALTER ... ADD COLUMN tenant_grants TEXT NOT NULL` that doesn't actually
work (see below).

Add one new column, `tenant_grants TEXT` (**nullable**, no default), to
each of `oauth_authorization_codes`, `oauth_access_tokens`, and
`oauth_refresh_tokens` — a JSON array of `{tenant_id, dataset_ids}`. This is
added via the same plain `ADD COLUMN` this codebase already uses for
exactly this kind of retrofit (`ensure_sqlite_text_column`,
`catalog.rs:35-52`, the same mechanism `dataset_ids` and `scopes` were
added with) — nullable because neither SQLite nor Postgres allows adding a
`NOT NULL` column with no default to a table that already has rows, and a
live deployment (e.g. hive) has existing OAuth tokens. "Every row this
change's code creates has it populated" is enforced at the **application**
layer, not the database — the same treatment `dataset_ids` already gets
(existence-checked against the tenant registry at consent/refresh time, no
DB-level foreign key to `tenants`).

The existing `tenant_id` column is made nullable (dropping `NOT NULL`) and,
together with `dataset_ids`, is **never written by new code after this
change ships**, for single-tenant or multi-tenant grants alike — new rows
leave `tenant_id` `NULL`, which trivially satisfies the existing
`FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE` (a NULL
foreign-key value has nothing to cascade from; the constraint itself is
untouched). A migration backfills `tenant_grants` for every pre-existing
row from its current `tenant_id`/`dataset_ids`, so every row — old and new
— can be read uniformly through `tenant_grants` from this change forward.

Dropping `NOT NULL` from `tenant_id` is a native one-step
`ALTER COLUMN ... DROP NOT NULL` on Postgres, but SQLite has no equivalent
— this is not a cosmetic detail, it changes what the migration actually
has to do. This codebase already has the technique for exactly this
situation (`rebuild_users_table_sqlite`, `catalog.rs:97-160`, used to drop
`NOT NULL` from `users.password_hash`): create a new table without the
constraint, copy every row across, drop the old table, rename the new one
into place, all inside one transaction. The migration for these three
OAuth tables follows that same pattern on SQLite — this is real,
budgeted work (see tasks.md 1.1-1.3), not an "ordinary ALTER TABLE."

**Why not the three-child-table design first drafted here:** it kept the
legacy `tenant_id` column's `FOREIGN KEY ... ON DELETE CASCADE` on the
_parent_ row and populated it with "the grant's first tenant" as a mirror.
Because that FK is on the parent row itself, deleting the one tenant that
happened to land in the mirror column cascade-deleted the _entire_ token —
access token, refresh token, or auth code — silently revoking every other
tenant in that same grant along with it, disproportionate to what was
actually deleted and dependent on migration/insertion order of which
tenant got mirrored. A single JSON column with no DB-level FK — the same
treatment `dataset_ids` already gets, with no DB-level FK to `datasets` —
makes this structurally impossible: nothing in the schema can ever cascade
through `tenant_grants`, because there is no foreign key to cascade
through. It's also lighter (one additive column vs. three new tables with
real FKs) and matches this codebase's own established idiom for exactly
this "single value → set" generalization (`dataset_ids`, `scopes`), unlike
the child-table design, which didn't actually match that precedent despite
citing it.

Alternative also considered: a single shared `oauth_grant_tenants(kind,
token_id, tenant_id, dataset_ids)` table keyed by a `kind` discriminator.
Rejected on the same grounds as the three-table design (still an FK'd
child table, still heavier than the JSON-column precedent) and, in
addition, SQLite/Postgres can't express a polymorphic FK against `kind`
anyway.

### D3: A grant entry naming a deleted tenant simply fails to resolve

Because `tenant_grants` has no DB-level FK, deleting a tenant never touches
existing OAuth rows at all — there is no cascade, so a grant set can never
be silently pruned or emptied by a tenant deletion. What can happen: a
grant still names a tenant that no longer exists in the registry.
Resolution (D4) already has to look up the selected tenant to build
`TenantContext`; if that lookup fails because the tenant is gone, that
specific entry is simply unresolvable — the same outcome as selecting a
tenant outside the grant set, not a distinct "empty grant" state requiring
its own handling. A token whose every entry points at a deleted tenant is
functionally inert (nothing it names can ever resolve) without needing any
special-cased emptiness check or cleanup job.

### D4: Grant discovery via a new introspection endpoint, not a `whoami` exception

An earlier draft made `whoami` the "one exception": called with no
`X-Tenant-ID`, it would return the full grant set, while every other
tenant-scoped route required a selector. That doesn't work: `whoami` is
mounted behind the exact same generic `auth_middleware` as every other
`/api/v1/*` route (see Context), which unconditionally resolves exactly one
concrete `TenantContext.tenant_id` or fails the request before any handler
runs — there is no route-specific carve-out available at that layer, and
the Non-Goals rule out adding one by making `tenant_id` optional.

Instead:

- Add `POST /oauth/introspect` (RFC 7662-shaped) on the **authorization
  server** side (`src/router/src/endpoints/oauth.rs`), alongside the
  existing token/registration endpoints it already implements directly
  against the catalog — not behind the resource-API's generic
  `auth_middleware`. Given a bearer token, it returns `{active, user_id,
tenants: [{tenant_id, dataset_ids}], scopes, aud, exp}` without resolving
  or requiring any single tenant. This is the natural home for "what can
  this token reach": it needs the same catalog-backed token lookup token
  issuance already does, just read-only, and it sidesteps the resource
  API's single-tenant contract entirely rather than bending it.
- The MCP server's `mcp_auth_middleware` calls this endpoint instead of
  `whoami()` for OAuth credentials, to learn the full grant set upfront
  (`active: false` doubles as the existing invalid/expired/revoked check
  `whoami()`'s failure previously served). This is a genuine fork, not a
  drop-in call swap: today every credential type stores one
  `audit::CallerTenant(single tenant id)` extension, and every consumer
  (`check_tenant_scope`, `audit.rs`, `discover_datasets`, `server_info`,
  `sdk_client_for`'s header derivation) reads that one value. An API-key
  credential or a single-tenant OAuth credential keeps doing exactly that,
  unchanged. Only a multi-tenant OAuth credential (introspection reports
  more than one tenant) additionally stores a new, separate
  `CallerTenants(Vec<TenantGrant>)` extension and binds the session to the
  **credential** (token hash / user_id) rather than a tenant. Each of those
  five consumers needs its own branch on which extension is present; none
  of them can treat `CallerTenants` as a drop-in replacement for
  `CallerTenant`, since the single-tenant path must keep working exactly as
  it does today.
- `check_tenant_scope` becomes a set-membership check instead of equality.
- The per-tool-call router client (`sdk_client_for`) sets `X-Tenant-ID:
<p.tenant>` (and `X-Dataset-ID` if given) explicitly on the outgoing
  request when the credential is OAuth and its grant set has more than one
  tenant — the only case where the router needs a selector at all.
- `extract_auth_headers`'s OAuth branch (`src/common/src/auth/middleware.rs`)
  must change to read `X-Tenant-ID` for an OAuth bearer too (it currently
  hard-discards it) and pass it into `authenticate_oauth_token`.
- `authenticate_oauth_token` re-validates that selector against the token's
  actual DB-stored `tenant_grants` on every call: a single-tenant grant
  resolves its one tenant regardless of the header (ignoring it exactly as
  today, including when absent); a multi-tenant grant requires the header,
  rejects a request without one, and rejects a selector naming a tenant
  outside the set. `TenantContext.tenant_id` stays mandatory and concrete
  in every case — no change to its contract, per the Non-Goals. A bug in
  the MCP server's local check can therefore only narrow access, never
  widen it: the router is still the sole authority, per the Goals.
- `whoami` itself is **not** special-cased any further: it resolves exactly
  one tenant the same way every other route does (implicit for a
  single-tenant grant, selector-required for a multi-tenant one), and, when
  the underlying grant has more than one tenant, additionally includes the
  full grant set informationally alongside the resolved `tenant` — useful
  context once a selector was already supplied, not a discovery mechanism
  on its own.

### D5: `whoami` response shape

`tenant` keeps its current shape and meaning: the one concrete tenant this
request resolved to (unchanged for every credential type, since resolution
itself is unchanged per D4 — `whoami` always requires a selector under the
same rule as any other route). Add `granted_tenants: [{tenant,
dataset_ids}]` alongside it — named to be unmistakably distinct from the
pre-existing `memberships` field (`WhoamiResponse`, `session.rs:600-611`),
which already lists every tenant the authenticated _human user_ belongs to
regardless of what this specific credential was consented for (confirmed
populated for OAuth sessions today too — pre-existing behavior, unchanged
by this design). `granted_tenants` is scoped strictly to this token's own
grant: for a single-tenant credential (API key or single-tenant OAuth
grant), it's a one-element array equal to `tenant`, so existing callers of
`tenant` see no change; for a multi-tenant OAuth credential, it lists every
tenant in the grant, so a caller that already knows which tenant it's
asking as can also see its siblings without a second round trip. Primary
grant discovery for a not-yet-selected multi-tenant credential goes through
the new introspection endpoint (D4), not `whoami`.

### D6: Consent UX

Replace the tenant radio-button list with a checklist (same visual pattern
already used for the per-tenant dataset picker); checking a tenant reveals
its own all-datasets/some-datasets sub-choice, unchanged from today except
it now repeats per checked tenant instead of appearing once. Scopes stay
grant-wide (one checklist, applies to every tenant in the set) — no user
has asked for per-tenant scopes, and splitting them would multiply the
screen's complexity for no requested benefit.

## Risks / Trade-offs

- **[Broader blast radius of one compromised token]** → A stolen
  multi-tenant token now reaches every tenant it was granted, not just one.
  Mitigation: this is the same set the user explicitly checked at consent
  (no scope creep beyond what they granted), tokens remain revocable by row
  delete exactly as before, and `access_token_ttl` bounds exposure the same
  way it does today.
- **[Middleware/tool-handler split adds a second place tenant logic lives]**
  → `check_tenant_scope` (client-side, best-effort) and
  `authenticate_oauth_token` (server-side, authoritative) must agree.
  Mitigation: D4 keeps the router authoritative by construction — the MCP
  server's check can only be stricter, never a substitute; integration
  tests exercise the router path directly, not just through the MCP server.
- **[A new unauthenticated-adjacent introspection endpoint]** → `POST
/oauth/introspect` answers "what can this bearer token reach" outside the
  usual per-tenant authorization path. Mitigation: it discloses nothing the
  token's own holder couldn't already learn by trying each tenant it
  actually holds against any other endpoint; it still requires presenting
  the valid bearer token itself (RFC 7662's standard trust model — the
  caller already possesses the credential), and it performs no data access
  of its own, only a catalog lookup of the token's own stored grant.
- **[Legacy columns still exist post-migration]** → `tenant_id`/
  `dataset_ids` remain in the schema, unused by new code, as long as any
  pre-migration row hasn't expired. Mitigation: confirmed only
  `catalog.rs`'s own OAuth functions read them today, so nothing external
  depends on them; a follow-up cleanup change drops them once every
  pre-migration row has naturally expired (bounded by `access_token_ttl`/
  `refresh_token_ttl`), the same two-step pattern
  `remove-dataset-id-legacy-shims` used.

## Migration Plan

1. Add `tenant_grants TEXT` (nullable) to the three OAuth tables via a
   plain `ADD COLUMN` (additive, works against a populated table on both
   backends). Separately, drop `NOT NULL` from their existing `tenant_id`
   column: a native `ALTER COLUMN ... DROP NOT NULL` on Postgres, but on
   SQLite the create-new-table/copy-rows/swap technique this codebase
   already uses for `users.password_hash` (`rebuild_users_table_sqlite`) —
   budgeted as real, non-trivial work, not a one-line ALTER. No column
   removal and no FK change either way.
2. Backfill: for every existing row, populate `tenant_grants` with the
   single-element array built from that row's current `tenant_id`/
   `dataset_ids`, so every row — old and new — reads uniformly through
   `tenant_grants` from this point on.
3. Ship consent, token issuance/refresh, the new introspection endpoint,
   and `authenticate_oauth_token`/`extract_auth_headers` changes together —
   the single-tenant path is unchanged output for unchanged input, so this
   is safe to roll out in one release like other additive OAuth changes in
   this codebase (`mcp-oauth` itself, `multi-dataset-key-restriction`'s
   OAuth phase).
4. Update `docs/users/mcp.md` (Claude.ai/ChatGPT connector section) and
   `docs/users/authentication.md` in the same change.
5. Rollback: revert the code. The additive `tenant_grants` column and the
   now-nullable `tenant_id` are both harmless to leave in place; no
   destructive rollback step is required.
