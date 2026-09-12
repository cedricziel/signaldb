## 1. Catalog schema

- [x] 1.1 Write a failing catalog test asserting `oauth_access_tokens`, `oauth_refresh_tokens`, and `oauth_authorization_codes` each gain a nullable `tenant_grants TEXT` column, added via a plain `ADD COLUMN` against a table that already has rows (SQLite and Postgres variants, mirroring the existing dual-backend migration tests in `catalog.rs`)
- [x] 1.2 Add nullable `tenant_grants TEXT` (via `ensure_sqlite_text_column`-style `ADD COLUMN`, matching how `dataset_ids`/`scopes` were added — no `NOT NULL`, no default) to all three tables, in both the SQLite and Postgres migration code paths in `src/common/src/catalog.rs`
- [x] 1.3 Write a failing test: `tenant_id` on all three tables is nullable, and a row can be inserted with `tenant_id = NULL`
- [x] 1.4 Drop `NOT NULL` from `tenant_id` on all three tables: a native `ALTER COLUMN ... DROP NOT NULL` on Postgres; on SQLite, the create-new-table/copy-rows/swap technique this codebase already uses for `users.password_hash` (`rebuild_users_table_sqlite`, `catalog.rs:97-160`) — budget this as real migration work, not a one-line ALTER
- [x] 1.5 Write a failing test: a migration/backfill step populates `tenant_grants` for a pre-existing single-tenant row from its `tenant_id`/`dataset_ids`
- [x] 1.6 Implement the backfill
- [x] 1.7 Extend `OAuthTokenRecord` (and the authorization-code equivalent) to carry `tenant_grants: Vec<TenantGrant { tenant_id, dataset_ids: Option<Vec<String>> }>` instead of a single `tenant_id`/`dataset_ids`; stop writing `tenant_id`/`dataset_ids` on any newly-created row (leave them `NULL`)
- [x] 1.8 Update the catalog functions that insert/read/delete authorization codes, access tokens, and refresh tokens to use `tenant_grants` as the sole source of truth, validating each `tenant_id` against the tenant registry at write time (same pattern `dataset_ids` already uses against a tenant's datasets) — no DB-level FK, and enforce "non-empty" at the application layer since the column itself is nullable

## 2. Router: consent and authorization

- [x] 2.1 Write a failing test: consent context (`GET /oauth/consent/context`) lists every tenant the user is a member of as independently selectable, each with its own dataset list
- [x] 2.2 Write a failing test: approving with two or more tenants checked issues one authorization code bound to all of them, each with its own dataset restriction
- [x] 2.3 Write a failing test: approving with zero tenants checked is rejected, no code issued
- [x] 2.4 Update `POST /oauth/authorize/decision` to accept a list of `{tenant_id, dataset_ids | null}` and persist the resulting grant set on the authorization code
- [x] 2.5 Update the explore-UI consent page (served at root) from a tenant radio list to a multi-select checklist, each checked tenant revealing its own all-datasets/some-datasets sub-choice (design D6)

## 3. Router: token issuance, refresh, and resolution

- [x] 3.1 Write a failing test: exchanging a multi-tenant authorization code yields an access token and refresh token bound to the full grant set
- [x] 3.2 Write a failing test: refreshing a multi-tenant token yields a new access token with the same grant set, scopes, and audience
- [x] 3.3 Update token issuance/refresh in `src/router/src/endpoints/oauth.rs` to carry the full grant set forward (already satisfied by 9e530f18's plumbing — both tests above pass unmodified, confirmed rather than assumed)
- [x] 3.4 Write a failing test: `extract_auth_headers` (`src/common/src/auth/middleware.rs`) now reads `X-Tenant-ID` for an OAuth bearer instead of discarding it, and passes it through to `authenticate_oauth_token`
- [x] 3.5 Implement the `extract_auth_headers` change
- [x] 3.6 Write a failing test: `authenticate_oauth_token` resolves a single-tenant grant exactly as before, ignoring any `X-Tenant-ID`
- [x] 3.7 Write a failing test: `authenticate_oauth_token` on a multi-tenant grant requires `X-Tenant-ID`, rejects a request with none, rejects a tenant outside the grant set (naming it), rejects a tenant that no longer exists in the registry (design D3), and applies the selected tenant's own dataset restriction on a match
- [x] 3.8 Implement the generalized resolution in `Authenticator::authenticate_oauth_token` (`src/common/src/auth/`); `TenantContext.tenant_id` stays mandatory and concrete — no optionality introduced
- [x] 3.9 Write a failing test: `GET /api/v1/whoami` with a single-tenant credential is unchanged (same `tenant` field, plus a new one-element `granted_tenants` array)
- [x] 3.10 Write a failing test: `GET /api/v1/whoami` with a multi-tenant credential and no `X-Tenant-ID` is rejected exactly like any other tenant-scoped route; with `X-Tenant-ID` set, it returns that tenant plus the full `granted_tenants` array (distinct from the pre-existing `memberships` field)
- [x] 3.11 Implement the `whoami` response change in `src/router/src/endpoints/session.rs`
- [x] 3.12 Write a failing test: `POST /oauth/introspect` on an active token reports `active: true` plus its full grant set (every tenant and dataset restriction), scopes, audience, and expiry, without requiring `X-Tenant-ID`; on an invalid/expired/revoked token it reports `active: false` and no other detail
- [x] 3.13 Implement `POST /oauth/introspect` in `src/router/src/endpoints/oauth.rs`, resolving the token directly against the catalog (not through the resource-API's `auth_middleware`)

## 4. HTTP API surface

- [x] 4.1 Update the OpenAPI spec for `GET /api/v1/whoami` (new `granted_tenants` field) and `POST /oauth/authorize/decision` (list of tenant grants). `POST /oauth/introspect` is deliberately excluded from the generated spec, matching the existing precedent that `/oauth/token` and `/oauth/register` in the same file also carry no `#[utoipa::path]`
- [x] 4.2 Regenerate the Rust SDK (`src/signaldb-sdk`) from the updated spec
- [x] 4.3 Regenerate the TypeScript client (`src/ui/src/api/gen`) from the updated spec

## 5. MCP server

- [x] 5.1 Write a failing test: an OAuth session whose credential grants two tenants is not identity-locked to either at session establishment (design D4)
- [x] 5.2 Write a failing test: a multi-tenant OAuth session's later call selecting a different granted tenant than an earlier call succeeds, scoped to the newly selected tenant
- [x] 5.3 Write a failing test: a multi-tenant OAuth session's call naming a tenant outside the grant set is rejected before any router call
- [x] 5.4 Change `mcp_auth_middleware` (`src/mcp-server/src/lib.rs`) to call the new `POST /oauth/introspect` instead of `whoami()` for an OAuth credential whose grant covers more than one tenant, storing a new, separate `CallerTenants(Vec<TenantGrant>)` extension alongside (not replacing) the existing `CallerTenant`/`CallerDatasetIds` used by every single-tenant credential (API key or single-tenant OAuth), and bind the session to the credential rather than a fixed tenant only in that multi-tenant case
- [x] 5.5 Change `check_tenant_scope` (`src/mcp-server/src/server.rs`) to branch on which extension is present: equality against `CallerTenant` (unchanged) for a single-tenant credential, set-membership against `CallerTenants` for a multi-tenant one
- [x] 5.6 Change `sdk_client_for`/`router_client` to set `X-Tenant-ID`/`X-Dataset-ID` explicitly from the tool call's own arguments when the credential is OAuth with a multi-tenant grant
- [x] 5.7 Write a failing test: `discover_datasets` on a multi-tenant credential returns one top-level entry per granted tenant with that tenant's own datasets nested beneath it
- [x] 5.8 Update `discover_datasets` and `server_info` to enumerate the full grant set
- [x] 5.9 Update the MCP audit log (`src/mcp-server/src/audit.rs`) so `tenant_id` names the tenant the specific call resolved to (unchanged for single-tenant, now per-call for multi-tenant)

## 6. Documentation

- [ ] 6.1 Update `docs/users/mcp.md` (Claude.ai/ChatGPT connector section): multi-select consent, `tenant` argument as selector for multi-tenant grants, `discover_datasets`/`server_info` behavior
- [ ] 6.2 Update `docs/users/authentication.md`: OAuth token model now carries a grant set, plus the new introspection endpoint

## 7. Integration testing

- [ ] 7.1 Add an end-to-end test (`tests-integration`) covering the full flow: register a client, consent to two tenants with different dataset restrictions, exchange the code, call a query tool against each granted tenant via the MCP server, and confirm a third tenant is refused
- [ ] 7.2 Add an end-to-end test: delete one tenant from a two-tenant grant and confirm the other tenant's access is unaffected (regression test for the FK-cascade hazard the JSON-column design (D2) avoids)

## 8. Finishing

- [ ] 8.1 Run the `simplify` skill on the full diff and apply its findings
- [ ] 8.2 Squash into clean semantic commits, one concern per commit
