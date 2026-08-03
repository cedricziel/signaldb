## 1. Catalog schema & config foundation

- [x] 1.1 Write failing `common` catalog tests for the new tables: registered OAuth clients, authorization codes, access tokens, refresh tokens — each storing a token/secret **hash** (never the raw value), subject user, tenant, granted scopes, audience, and expiry (`cargo test -p common`).
- [x] 1.2 Add forward-only catalog migrations + row types for those tables (SQLite + PostgreSQL), making 1.1 pass.
- [x] 1.3 Add `[mcp]`/auth config for the AS issuer URL, resource URL, and token/code lifetimes following config precedence; document in `signaldb.dist.toml` (commented). _(folded into the router phase where it is consumed)_

## 2. Read-scope model (extends the existing scope machinery)

- [x] 2.1 Write failing `common` tests for `TenantContext::can_read(signal)` mirroring `can_ingest`: `<signal>:read` required when scopes are `Some`; `None` = legacy-unrestricted; role gate respected (`cargo test -p common`).
- [x] 2.2 Implement `can_read` and the `traces:read`/`logs:read`/`metrics:read` scope constants, making 2.1 pass.

## 3. Opaque token primitives & store

- [x] 3.1 Write failing `common` tests for opaque generation + hashing of access/refresh/code values (reusing the `generate_session_token`/`hash_session_token` pattern) and for store operations: insert, lookup-by-hash, single-use consume, revoke (delete), expiry rejection (`cargo test -p common`).
- [x] 3.2 Implement the token/code generation, hashing, and catalog-backed store operations, making 3.1 pass.

## 4. Authorization Server metadata discovery (RFC 8414)

- [x] 4.1 Write a failing `router` test that `GET /.well-known/oauth-authorization-server` returns registration/authorization/token endpoints, `code_challenge_methods_supported` ⊇ `S256`, and `grant_types_supported` ⊇ `authorization_code`,`refresh_token` (`cargo test -p router`).
- [x] 4.2 Implement the metadata handler, making 4.1 pass.

## 5. Dynamic Client Registration (RFC 7591)

- [x] 5.1 Write failing `router` tests: unauthenticated `POST /register` with valid `redirect_uris` persists a client and returns a unique `client_id`; missing/malformed `redirect_uris` is rejected with no client persisted (`cargo test -p router`).
- [x] 5.2 Implement the registration endpoint + persistence, making 5.1 pass.

## 6. Authorize + consent (login, tenant selection, PKCE)

- [ ] 6.1 Write failing `router` tests: `/authorize` without `code_challenge` is rejected; an approved request issues a single-use code bound to the selected tenant, scopes, client, redirect URI, PKCE challenge, and resource; a code can never bind a tenant the user is not a member of (`cargo test -p router`).
- [ ] 6.2 Implement `/authorize` reusing the existing login/session, the consent decision handler, and consent-time tenant selection restricted to the user's memberships, making 6.1 pass.

## 7. Token endpoint (code exchange + refresh)

- [x] 7.1 Write failing `router` tests: code + matching `S256` verifier (with matching `client_id`/`redirect_uri`) yields access + refresh tokens and consumes the code; PKCE mismatch → `invalid_grant`; reused/expired code → `invalid_grant`; refresh mints an access token with the same tenant/scopes/audience (`cargo test -p router`).
- [x] 7.2 Implement `/token` for the `authorization_code` and `refresh_token` grants, making 7.1 pass.

## 8. Authenticator OAuth credential path & audience binding

- [x] 8.1 Write failing `router`/`common` tests: a valid opaque access token resolves to a `TenantContext` with tenant + scopes from the token (not from `X-Tenant-ID`); an `X-Tenant-ID` naming a different tenant is ignored; expired/revoked tokens → unauthenticated; a token whose recorded audience ≠ the resource is rejected (`cargo test -p router`, `cargo test -p common`).
- [x] 8.2 Add the opaque-OAuth-token credential path to `Authenticator` (tenant + scopes from the token record; audience check), making 8.1 pass — leaving the API-key and session paths unchanged.

## 9. Read-scope enforcement on the query/MCP surface

- [ ] 9.1 Write failing tests that a token holding `traces:read` may run trace-read tools while a token lacking `metrics:read` is denied metrics reads with an authorization error, scoped to the token's tenant (`cargo test -p router`).
- [ ] 9.2 Enforce `can_read(signal)` on the query paths the MCP read tools use, making 9.1 pass.

## 10. MCP sidecar: resource-origin responsibilities

- [ ] 10.1 Write failing `mcp-server` tests: unauthenticated MCP request → `401` with `WWW-Authenticate: Bearer resource_metadata="…"`; `GET /.well-known/oauth-protected-resource` returns a doc whose `authorization_servers` names the router AS and `resource` equals the MCP URL; an OAuth session no longer requires `X-Tenant-ID`, while an API-key session still does (`cargo test -p mcp-server`).
- [ ] 10.2 Implement the PRM document route + `401` challenge, and relax the `X-Tenant-ID` requirement for OAuth-authenticated sessions while keeping it for API-key sessions; keep forwarding the bearer verbatim — making 10.1 pass.

## 11. Consent UI (UI surface)

- [ ] 11.1 Build the consent screen reusing the explore-UI login: shows the requesting client, requested scopes, and the user's grantable tenants for selection; approve/deny posts the consent decision to the router. The UI consumes the generated TypeScript client, not raw fetch.
- [ ] 11.2 Add a UI test/interaction check that only the user's member tenants are selectable and that denial returns an OAuth error to the client.

## 12. API contract regeneration (HTTP API surface)

- [ ] 12.1 Annotate any OAuth/discovery endpoints surfaced in the code-first OpenAPI document and update `api/signaldb-api.json`; keep the golden test `openapi_spec_is_up_to_date` green.
- [ ] 12.2 Regenerate the Rust SDK (`src/signaldb-sdk`) and the TypeScript client (`src/ui/src/api/gen`) consumed by the consent UI. (CLI connector-management surface — list/revoke granted connectors — is scoped out of v1; note it as a follow-up.)

## 13. End-to-end integration coverage

- [ ] 13.1 Add a `tests-integration` test that drives the full connector flow against router + sidecar: discovery → DCR → authorize+consent (tenant selected) → PKCE token exchange → authenticated MCP tool call returning only the bound tenant's data.
- [ ] 13.2 Add integration assertions for isolation and lifecycle: a second-tenant attempt on a token stays scoped to its bound tenant; revoking the token’s record makes the next MCP call `401`; a token minted for another resource/audience is rejected.

## 14. Docs & skills

- [ ] 14.1 Document the OAuth connector flow and operator setup (issuer/resource URLs, TLS requirement for non-loopback origins, one-connector-per-tenant behavior) — route via the docs skill; update the `multi-tenancy` and MCP/`tempo-api` docs.
- [ ] 14.2 Update the `multi-tenancy` skill (and the MCP skill/notes) to describe OAuth tokens, read scopes, and tenant-from-token so described behavior matches.

## 15. Pre-commit gate

- [ ] 15.1 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, and `cargo machete --with-metadata`; resolve findings before finishing.
