## 1. Catalog and config foundations

- [ ] 1.1 Failing tests in `common::catalog`: users carry nullable `(oidc_issuer, oidc_subject)` with a unique pair constraint; `password_hash` nullable (SQLite + Postgres)
- [ ] 1.2 Additive `users` migration + record types (`password_hash: Option<String>`); lookup `find_user_by_oidc_identity`
- [ ] 1.3 Failing tests in `common::catalog` for source-keyed memberships: pre-existing rows migrate as `granted_by = 'local'`; a `local` and an `oidc_mapping` row coexist for one `(user_id, tenant_id)`; `upsert_tenant_membership`/`remove_tenant_membership` touch only `local` rows; `sync_oidc_memberships` upserts/deletes only `oidc_mapping` rows in one transaction; `get_tenant_membership` returns the higher role and its source; list operations expose `granted_by` (SQLite + Postgres)
- [ ] 1.4 Implement the `tenant_memberships` migration (Postgres: add column + swap PK; SQLite: idempotent table rebuild in a transaction) and the source-aware catalog operations
- [ ] 1.5 Failing tests in `common::config`: `[auth.oidc]` parses (issuer_url, client_id, client_secret, redirect_url?, display_name?, allowed_email_domains?, group_claim?, group_mappings[], disable_password_login); flag without provider = config error
- [ ] 1.6 Implement `OidcConfig` with env-var overrides (`SIGNALDB__AUTH__OIDC__*`)

## 2. RP core (router)

- [ ] 2.1 Add `openidconnect` workspace dependency; failing tests against a wiremock discovery endpoint: invalid `[auth.oidc]` fails startup naming the setting; unreachable/invalid discovery starts the instance with OIDC `unavailable` and logs the issuer; background retry with backoff flips it to available without restart
- [ ] 2.2 Implement startup discovery + the `unavailable`/`available` provider state and retry task
- [ ] 2.3 Failing tests: `GET /ui/session/oidc/start` 302s to the IdP with state/nonce/PKCE and sets the signed pending-login cookie with `HttpOnly; Secure; SameSite=Lax; Path=/ui/session/oidc; Max-Age=300` (assert `SameSite=Lax`, never `Strict`); 404 when OIDC unconfigured; 503 naming the issuer while discovery is `unavailable`
- [ ] 2.4 Implement the start endpoint (callback URL from request origin, `redirect_url` override)
- [ ] 2.5 Failing tests against wiremock IdP: callback reads state/PKCE verifier from the pending-login cookie and exchanges the code, validates ID token, clears the pending cookie; rejects missing pending cookie, bad state, bad nonce, bad signature, expired token, `email_verified: false` on link path — all without session creation and with a generic error
- [ ] 2.6 Implement the callback: validation → identity resolution ((issuer,subject) → verified-email link → JIT with allowlist) → session issuance via the existing session path; disabled users refused
- [ ] 2.7 Failing test: JWKS rotation (unknown `kid` triggers refetch and succeeds)

## 3. Provisioning, mapping, password switch

- [ ] 3.1 Failing tests: JIT create carries subject/email/name and no password; allowlist refusal creates no row and leaks nothing; email link attaches identity to the existing user
- [ ] 3.2 Failing tests for mapping sync: mapped membership created at the mapped role; lost group removes only `granted_by='oidc_mapping'` rows; local rows never touched; no-mapping config = no membership writes; instance-admin flag never written
- [ ] 3.3 Implement per-login mapping sync on top of `sync_oidc_memberships`; `TenantContext` resolution uses the effective (higher) role when local and mapped rows coexist
- [ ] 3.4 Failing test in `router::endpoints::session`: a user with `password_hash = NULL` and `disable_password_login = false` gets the generic 401, `verify_password` is not invoked, no session row is created
- [ ] 3.5 Implement the null-password short-circuit in the password session endpoint
- [ ] 3.6 Failing tests: with `disable_password_login`, the password session endpoint refuses for every user with a named reason; API keys, `admin_api_key`, and CLI bootstrap unaffected
- [ ] 3.7 Implement the switch

## 4. Contract, clients, UI

- [ ] 4.1 Failing test: OpenAPI document lists `/ui/session/config`, `/ui/session/oidc/start`, `/ui/session/oidc/callback` with schemas, all three with an empty security requirement; probe schema has `password_enabled: bool` and nullable `oidc: {name}`; probe returns `{"password_enabled": true, "oidc": null}` without `[auth.oidc]` and `oidc: null` while discovery is `unavailable`
- [ ] 4.2 Implement `GET /ui/session/config`; add all three to `paths(...)`; regenerate `api/signaldb-api.json`, the Rust SDK, and the UI TypeScript client
- [ ] 4.3 Failing UI tests: login panel renders from the probe via the generated client — both doors, SSO-only, password-only (`oidc: null`); SSO button navigates (no XHR); membership views show `granted_by`
- [ ] 4.4 Implement the login-panel changes
- [ ] 4.5 tests-integration: full flow against a Dex (or Keycloak) testcontainer — SSO login, whoami, JIT user, mapped membership, MCP OAuth consent over an SSO session (Docker-gated)

## 5. Docs and skills

- [ ] 5.1 `docs/users/authentication.md`: SSO login section (flow, JIT, linking, password switch, break-glass)
- [ ] 5.2 `docs/operations/`: IdP setup guide (redirect URL/reverse proxy first, Authentik + Keycloak examples, allowlist, group mapping, degraded-startup behaviour, rollback including the clear-mapped-rows step before a binary rollback); update `signaldb.dist.toml`
- [ ] 5.3 Update `multi-tenancy` and `configuration` skills; run the docs-freshness gate after committing
- [ ] 5.4 Verify Definition of Done: probe/endpoints in OpenAPI with both clients regenerated and consumed; surface parity reviewed (SSO is inherently UI+HTTP; CLI/MCP scoped out — MCP benefits via OAuth consent, CLI has no browser)
