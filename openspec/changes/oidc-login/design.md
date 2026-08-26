## Context

See proposal.md — Why. What shapes the approach:

- The users-tenant-membership ADR already landed users, Argon2id passwords,
  server-side sessions (`user_sessions`, SHA-256 token hash, 12h TTL),
  `tenant_memberships` with roles, and the instance-admin flag. OIDC is
  specified there as "an alternative credential on the same `users` row" —
  this design adds a credential, not an identity model.
- SignalDB is already an OAuth 2.1 _authorization server_ for MCP clients
  (`mcp-oauth` spec). That flow authenticates the human via the browser
  session, so upgrading session login upgrades MCP OAuth for free. The AS and
  RP roles must not be conflated: `[mcp.oauth]` (we issue tokens) vs
  `[auth.oidc]` (we consume an IdP).
- Fronting-proxy SSO was rejected: hive's Pangolin experience shows a proxy
  in front breaks the M2M OAuth paths, and header-trust auth would need its
  own spec to be safe.
- No FDAP surface is touched: no Arrow/Parquet/Flight schema or WAL/Iceberg
  layout change; catalog migration is additive columns only.

## Goals / Non-Goals

**Goals:**

- OIDC RP with the smallest possible new surface: two endpoints, one config
  section, two nullable catalog columns.
- Reuse the session, membership, and role machinery unchanged.
- Deterministic, config-declared group mapping that cannot fight local admin
  actions.

**Non-Goals:**

- Multiple providers, SAML, LDAP.
- SCIM or out-of-band deprovisioning (disabled-user semantics plus IdP-side
  revocation cover it; a lost group also revokes mapped memberships at next
  login).
- Mapping to the instance-admin flag.
- Refresh of IdP sessions / front-channel or back-channel logout — SignalDB
  sessions keep their own lifetime.
- Machine-to-machine JWT auth (API keys stay the machine plane).

## Decisions

1. **`openidconnect` crate for the RP flow.** Mature, does discovery, JWKS
   caching, ID-token validation, PKCE. _Alternative:_ hand-rolled via
   `jsonwebtoken` + manual discovery — more code in the most
   security-sensitive path for no gain.
2. **Endpoints live on the router beside the session endpoints:**
   `GET /ui/session/oidc/start` (302 to IdP; sets a short-lived, HttpOnly
   pending-login cookie holding state/nonce/PKCE-verifier, server-side
   stateless) and `GET /ui/session/oidc/callback`. A
   `GET /ui/session/config` probe reports `{password_enabled, oidc: {name}}`.
   All three unauthenticated, all in OpenAPI. _Alternative:_ persisting
   pending logins in the catalog — needless writes and cleanup for a 5-minute
   artifact; the signed cookie carries the same guarantees.
3. **Identity resolution order:** `(issuer, subject)` unique pair → verified
   email link → JIT create (allowlist permitting). Email linking requires
   `email_verified: true` from the IdP; otherwise treat as no match to avoid
   account takeover via unverified email claims.
4. **Catalog change:** `users.oidc_issuer`, `users.oidc_subject` (nullable,
   unique together), `password_hash` nullable. `tenant_memberships` gains a
   `granted_by` discriminator (`local` | `oidc_mapping`) so mapping sync can
   add/update/remove only its own rows. Additive migration; rollback = older
   binary ignores the columns.
5. **Mapping sync is transactional per login:** compute desired mapped set
   from token groups × config rules, diff against existing
   `granted_by = 'oidc_mapping'` rows, apply. Local rows are never read by
   the sync. Conflict rule: if a user holds both a local and a mapped
   membership for the same tenant, the higher role wins at resolution time
   and the rows stay independent.
6. **`disable_password_login` lives in `[auth.oidc]`** and is only honoured
   when OIDC is configured — a config with the flag but no provider is a
   startup error, so an operator cannot lock every human out by typo.
7. **UI:** login panel consumes `/ui/session/config` through the generated
   client; SSO button does a full-page navigation to the start endpoint (no
   XHR — the flow is redirect-based).
8. **Testing:** unit-level RP tests against a wiremock IdP (discovery, JWKS,
   token endpoint; tampered nonce/signature/state cases). One
   tests-integration case against a Dex or Keycloak testcontainer for the
   full browser flow (Docker-gated, like other testcontainer suites).

## Risks / Trade-offs

- [IdP redirects depend on correct external URL config] → the start endpoint
  derives the callback from the request's origin unless
  `[auth.oidc].redirect_url` overrides it; the setup doc leads with the
  reverse-proxy case.
- [Clock skew breaks exp/iat validation] → accept the crate's default leeway
  (5 min) and document it.
- [JWKS rotation mid-flight] → `openidconnect` refetches on unknown `kid`;
  covered by a unit test.
- [Email-link takeover if IdP asserts unverified emails] → hard
  `email_verified` requirement (Decision 3); allowlist evaluated before any
  DB write.
- [Two pending auth changes touching the router] → no shared spec requirement
  is modified by both; implementation ordering is free, but rebasing whichever
  lands second over the other's middleware changes is expected.

## Migration Plan

1. Additive catalog migration ships first; runs on both SQLite and Postgres.
2. Deploy with `[auth.oidc]` unset — zero behaviour change; then configure
   the IdP and roll forward.
3. Rollback: unset `[auth.oidc]` (SSO-only users fall back to password reset
   by an admin) or roll back the binary — the extra columns are inert.

## Open Questions

- Display name for the SSO button: from config (`[auth.oidc].display_name`)
  with a default of the issuer host — cosmetic, decide at implementation.
