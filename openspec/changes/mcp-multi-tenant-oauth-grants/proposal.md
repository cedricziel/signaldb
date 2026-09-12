## Why

Claude.ai (and ChatGPT) allow only one MCP connector per remote server URL,
but a SignalDB OAuth token is bound to exactly one tenant chosen at consent.
Today's documented workaround — "add the connector a second time and grant
the other tenant" — is unusable on these hosts, so anyone who needs SignalDB
data from more than one tenant through Claude.ai cannot do it with the OAuth
connector today. The tenant model already supports a user belonging to
several tenants (`tenant_memberships`); the OAuth grant is the only place
still assuming one tenant per credential.

## What Changes

- Consent screen changes from picking one tenant to a multi-select checklist
  of the user's tenant memberships; each selected tenant still gets its own
  all-datasets/some-datasets choice.
- An OAuth access/refresh token can carry a **grant set**: one or more
  `(tenant_id, dataset_ids | unrestricted)` entries instead of a single
  tenant and dataset restriction. **BREAKING** (OAuth wire contract): the
  token's authorized scope is no longer expressible as one tenant — clients
  that inspect `whoami` for a single `tenant` field must be updated to read
  the new `granted_tenants` list (see Impact).
- For an OAuth credential resolving to exactly one tenant, all observed
  behavior is unchanged: no `X-Tenant-ID` needed, `whoami` still returns
  that tenant. This is the common case and stays free of any new mechanics.
- For an OAuth credential resolving to more than one tenant, every
  tenant-scoped request (query tools, tenant self-management tools,
  `whoami` included) SHALL require an explicit tenant selector, validated
  against the grant set — reusing the `tenant` argument every MCP tool
  already requires, which today is a pure confirmation check and becomes,
  for multi-tenant grants only, the actual selector.
- A new authorization-server endpoint, `POST /oauth/introspect` (RFC
  7662-shaped), reports a token's full grant set without requiring a
  selector — the mechanism the MCP server uses to learn what a multi-tenant
  credential can reach before any tenant has been chosen for a given call.
- The MCP server forwards the tool call's `tenant`/`dataset` arguments as
  `X-Tenant-ID`/`X-Dataset-ID` to the router for OAuth-authenticated calls
  when the credential's grant covers more than one tenant (today it forwards
  no such headers for OAuth at all, relying on the token resolving a single
  tenant server-side).
- `discover_datasets`/`server_info` report every tenant (and its datasets)
  in the credential's grant, not just one.
- Refresh tokens carry the same grant set forward as the token they refresh.

## Capabilities

### New Capabilities

(none — this generalizes existing OAuth and tool-surface behavior)

### Modified Capabilities

- `mcp-oauth`: consent, authorization codes, and tokens move from a single
  bound tenant to a bound set of tenants (each with its own optional dataset
  restriction); tenant resolution for a multi-tenant token requires an
  explicit selector instead of being implicit.
- `mcp-tool-surface`: the `tenant` argument's role changes from
  confirmation-only to a real selector when the calling credential's grant
  covers more than one tenant; `discover_datasets`/`server_info` enumerate
  the full grant set instead of one tenant.
- `mcp-server`: the per-session identity lock binds to the OAuth
  **credential**, not a fixed tenant, when the credential's grant covers
  more than one tenant — later calls on the same session may legitimately
  select a different tenant from that same grant. The server derives the
  `X-Tenant-ID` it forwards downstream from the tool call's own `tenant`
  argument for such credentials, rather than forwarding an inbound header
  (OAuth requests never carry one). Single-tenant credentials (OAuth or API
  key) are unaffected: the lock still binds tenant and credential together.

## Impact

- **Affected crates**: `router` (`src/router/src/endpoints/oauth.rs` —
  consent context/decision, token issuance/refresh, new
  `POST /oauth/introspect`; `src/router/src/endpoints/session.rs` —
  `whoami`; `src/common/src/auth/` — `authenticate_oauth_token`,
  `extract_auth_headers` (starts reading `X-Tenant-ID` for OAuth bearers,
  which it currently discards)), `mcp-server` (`src/mcp-server/src/lib.rs`
  — auth middleware calls the new introspection endpoint instead of
  `whoami` for OAuth credentials, and header forwarding;
  `src/mcp-server/src/server.rs` — `check_tenant_scope`,
  `discover_datasets`, `server_info`), `common` (catalog schema: one new
  `tenant_grants` JSON column per OAuth table, replacing the single
  `tenant_id`/`dataset_ids` columns as the source of truth, plus a
  migration).
- **Database migration**: add a nullable `tenant_grants TEXT` column (a
  JSON array of `{tenant_id, dataset_ids}`, validated at the application
  layer like `dataset_ids` already is — no DB-level FK) to
  `oauth_access_tokens`, `oauth_refresh_tokens`, and
  `oauth_authorization_codes` via a plain, populated-table-safe
  `ADD COLUMN`; drop `NOT NULL` from their existing `tenant_id` column and
  stop writing it going forward (see design.md D2 for why a DB-level FK on
  the grant's tenants is unsafe, and why dropping `NOT NULL` needs a real
  table-rebuild migration on SQLite, not a one-line `ALTER`). A migration
  backfills `tenant_grants` for every existing row from its current
  `tenant_id`/`dataset_ids`. No column removal, so no rolling-upgrade
  hazard like `remove-dataset-id-legacy-shims` had.
- **API surface**: new `POST /oauth/introspect` (RFC 7662-shaped) reports a
  token's full grant set; `GET /api/v1/whoami` gains a `granted_tenants`
  array (superset of today's single `tenant` field, named distinctly from
  the pre-existing, differently-scoped `memberships` field) but keeps
  requiring a selector for a multi-tenant credential exactly like every
  other route; `POST /oauth/authorize/decision` accepts a list of tenant
  grants instead of one.
- **Docs**: `docs/users/mcp.md` (Claude.ai/ChatGPT connector section),
  `docs/users/authentication.md` (OAuth token model).
- **Not affected**: API-key authentication, Claude Code's header-based
  multi-identity session path, ingest, query execution, storage layout.
