## 1. Catalog and config foundations

- [ ] 1.1 Failing tests in `common::catalog`: users carry nullable `(oidc_issuer, oidc_subject)` with a unique pair constraint; `password_hash` nullable; `tenant_memberships.granted_by` defaults to `local` (SQLite + Postgres)
- [ ] 1.2 Additive migrations + record types; lookup `find_user_by_oidc_identity`
- [ ] 1.3 Failing tests in `common::config`: `[auth.oidc]` parses (issuer_url, client_id, client_secret, redirect_url?, display_name?, allowed_email_domains?, group_claim?, group_mappings[], disable_password_login); flag without provider = config error
- [ ] 1.4 Implement `OidcConfig` with env-var overrides (`SIGNALDB__AUTH__OIDC__*`)

## 2. RP core (router)

- [ ] 2.1 Add `openidconnect` workspace dependency; startup discovery with fail-hard on bad issuer (failing test with wiremock discovery endpoint first)
- [ ] 2.2 Failing tests: `GET /ui/session/oidc/start` 302s to the IdP with state/nonce/PKCE and sets the signed pending-login cookie; 404 when OIDC unconfigured
- [ ] 2.3 Implement the start endpoint (callback URL from request origin, `redirect_url` override)
- [ ] 2.4 Failing tests against wiremock IdP: callback exchanges code, validates ID token; rejects bad state, bad nonce, bad signature, expired token, `email_verified: false` on link path — all without session creation and with a generic error
- [ ] 2.5 Implement the callback: validation → identity resolution ((issuer,subject) → verified-email link → JIT with allowlist) → session issuance via the existing session path; disabled users refused
- [ ] 2.6 Failing test: JWKS rotation (unknown `kid` triggers refetch and succeeds)

## 3. Provisioning, mapping, password switch

- [ ] 3.1 Failing tests: JIT create carries subject/email/name and no password; allowlist refusal creates no row and leaks nothing; email link attaches identity to the existing user
- [ ] 3.2 Failing tests for mapping sync: mapped membership created at the mapped role; lost group removes only `granted_by='oidc_mapping'` rows; local rows never touched; no-mapping config = no membership writes; instance-admin flag never written
- [ ] 3.3 Implement transactional per-login mapping sync; higher-role-wins resolution when local and mapped rows coexist
- [ ] 3.4 Failing tests: with `disable_password_login`, the password session endpoint refuses for every user with a named reason; API keys, `admin_api_key`, and CLI bootstrap unaffected
- [ ] 3.5 Implement the switch

## 4. Contract, clients, UI

- [ ] 4.1 Failing test: OpenAPI document lists `/ui/session/config`, `/ui/session/oidc/start`, `/ui/session/oidc/callback` with schemas, marked unauthenticated
- [ ] 4.2 Implement `GET /ui/session/config` (`password_enabled`, `oidc.name`); add all three to `paths(...)`; regenerate `api/signaldb-api.json`, the Rust SDK, and the UI TypeScript client
- [ ] 4.3 Failing UI tests: login panel renders from the probe via the generated client — both doors, SSO-only, password-only; SSO button navigates (no XHR)
- [ ] 4.4 Implement the login-panel changes
- [ ] 4.5 tests-integration: full flow against a Dex (or Keycloak) testcontainer — SSO login, whoami, JIT user, mapped membership, MCP OAuth consent over an SSO session (Docker-gated)

## 5. Docs and skills

- [ ] 5.1 `docs/users/authentication.md`: SSO login section (flow, JIT, linking, password switch, break-glass)
- [ ] 5.2 `docs/operations/`: IdP setup guide (redirect URL/reverse proxy first, Authentik + Keycloak examples, allowlist, group mapping, rollback); update `signaldb.dist.toml`
- [ ] 5.3 Update `multi-tenancy` and `configuration` skills; run the docs-freshness gate after committing
- [ ] 5.4 Verify Definition of Done: probe/endpoints in OpenAPI with both clients regenerated and consumed; surface parity reviewed (SSO is inherently UI+HTTP; CLI/MCP scoped out — MCP benefits via OAuth consent, CLI has no browser)
