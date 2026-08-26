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
  layout change; the catalog migration adds columns to `users` and re-keys
  `tenant_memberships` (Decision 5).

## Goals / Non-Goals

**Goals:**

- OIDC RP with the smallest possible new surface: three endpoints, one
  config section, one small catalog migration.
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
   `GET /ui/session/oidc/start` (302 to IdP; sets a short-lived pending-login
   cookie holding state/nonce/PKCE-verifier, server-side stateless) and
   `GET /ui/session/oidc/callback`. A `GET /ui/session/config` probe reports
   `{password_enabled: bool, oidc: {name: string} | null}` — `oidc` is a
   nullable object, `null` whenever OIDC is not configured or discovery has
   not succeeded yet, so the generated clients get one schema for both
   states. All three unauthenticated, all in OpenAPI. _Alternative:_
   persisting pending logins in the catalog — needless writes and cleanup for
   a 5-minute artifact; the signed cookie carries the same guarantees.
   **Pending-login cookie policy:** `signaldb_oidc_pending`, HMAC-signed
   (key derived from the session-token secret), `HttpOnly; Secure;
   SameSite=Lax; Path=/ui/session/oidc; Max-Age=300`, cleared by the
   callback. It MUST be `Lax`, not `Strict`: the callback is a cross-site
   top-level GET navigation initiated by the IdP's redirect, and a
   `SameSite=Strict` cookie is not sent on that request — the callback would
   never see the state or PKCE verifier. `signaldb_session` keeps
   `SameSite=Strict`; only the pending-login cookie relaxes to `Lax`, and its
   narrow path plus HMAC and five-minute TTL bound what `Lax` exposes.
3. **Identity resolution order:** `(issuer, subject)` unique pair → verified
   email link → JIT create (allowlist permitting). Email linking requires
   `email_verified: true` from the IdP; otherwise treat as no match to avoid
   account takeover via unverified email claims.
4. **Catalog change — users:** `users.oidc_issuer`, `users.oidc_subject`
   (nullable, unique together), `password_hash` nullable. Additive; rollback
   = older binary ignores the columns.
5. **Catalog change — memberships are keyed by source.** `tenant_memberships`
   gains `granted_by TEXT NOT NULL DEFAULT 'local' CHECK (granted_by IN
   ('local', 'oidc_mapping'))` and the primary key becomes
   `(user_id, tenant_id, granted_by)`, so a local row and a mapped row for
   the same user/tenant coexist as independent rows. Today's key is
   `(user_id, tenant_id)`, so this is not a pure column add:
   - _Migration_: Postgres — `ALTER TABLE ... ADD COLUMN granted_by ...`,
     drop the old PK constraint, add the new one. SQLite cannot alter a
     primary key, so the migration rebuilds the table (create
     `tenant_memberships_new`, `INSERT ... SELECT ..., 'local'`, drop, rename)
     inside one transaction. Every pre-existing row becomes `local`. Rollback:
     an older binary reads the table fine (it never filters on `granted_by`)
     but would see duplicate `(user_id, tenant_id)` pairs if mapped rows
     exist — the rollback step in the Migration Plan therefore clears mapped
     rows first.
   - _Catalog operations become source-aware_: the existing
     `upsert_tenant_membership` / `remove_tenant_membership` (admin API, CLI,
     MCP) are pinned to `granted_by = 'local'` — insert conflicts on the full
     key, delete filters on it — so local admin actions can never create or
     remove a mapped row. The sync path gets its own
     `sync_oidc_memberships(user_id, desired: [(tenant_id, role)])` which,
     in one transaction, upserts the desired rows with
     `granted_by = 'oidc_mapping'` and deletes `oidc_mapping` rows not in the
     desired set; it never reads or writes `local` rows.
   - _Resolution_: `get_tenant_membership(user_id, tenant_id)` returns the
     effective membership — the higher role across whichever rows exist
     (`admin > member > viewer`), with `granted_by` reported as the source
     that supplied it. `list_user_memberships` / `list_tenant_memberships`
     return every row with its `granted_by`, so admins can see that a
     membership is mapping-managed before trying to edit it; the UI/CLI
     surfaces render the source.
6. **Mapping sync is transactional per login:** compute the desired mapped
   set from token groups × config rules and hand it to
   `sync_oidc_memberships`. Local rows are never read by the sync. Conflict
   rule: if a user holds both a local and a mapped membership for the same
   tenant, the higher role wins at resolution time and the rows stay
   independent.
7. **`disable_password_login` lives in `[auth.oidc]`** and is only honoured
   when OIDC is configured — a config with the flag but no provider is a
   startup error, so an operator cannot lock every human out by typo.
8. **UI:** login panel consumes `/ui/session/config` through the generated
   client; SSO button does a full-page navigation to the start endpoint (no
   XHR — the flow is redirect-based).
9. **Testing:** unit-level RP tests against a wiremock IdP (discovery, JWKS,
   token endpoint; tampered nonce/signature/state cases). One
   tests-integration case against a Dex or Keycloak testcontainer for the
   full browser flow (Docker-gated, like other testcontainer suites).
10. **Startup: fail hard on bad config, degrade on an unreachable issuer.**
    Configuration errors (`disable_password_login` without a provider,
    malformed `issuer_url`, unusable mapping rules) fail startup — they are
    operator typos and the fix is local. A discovery fetch that fails at
    startup (issuer down, DNS, TLS) does _not_ stop the instance: OIDC enters
    an `unavailable` state, the probe reports `oidc: null`, the start endpoint
    answers 503 with a message naming the issuer, and a background task
    retries discovery with exponential backoff (cap 5 min) until it succeeds,
    at which point SSO is offered without a restart. Metadata that fetches but
    fails validation is logged at error level and treated the same as
    unreachable — it is more likely a transient IdP deploy than a SignalDB
    misconfiguration, and stopping the instance cannot fix it. This keeps the
    break-glass promise honest: a restart during an IdP outage still brings
    up API keys, `admin_api_key`, and the CLI bootstrap path. _Alternative:_
    fail hard on any discovery failure — simpler, but contradicts the
    break-glass scenario, which is the whole point of keeping those paths.

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

1. Catalog migration ships first; runs on both SQLite and Postgres. The
   `users` part is additive; the `tenant_memberships` part re-keys the table
   (Decision 5) and is idempotent, so a half-applied SQLite rebuild is safe
   to re-run.
2. Deploy with `[auth.oidc]` unset — zero behaviour change; then configure
   the IdP and roll forward.
3. Rollback: unset `[auth.oidc]` (SSO-only users fall back to password reset
   by an admin; mapped memberships stay until an admin removes them). To roll
   back the binary, first delete `granted_by = 'oidc_mapping'` rows (a
   documented one-liner in the setup guide) — the older binary keys on
   `(user_id, tenant_id)` and must not meet duplicates; the `users` columns
   are inert.

## Open Questions

- Display name for the SSO button: from config (`[auth.oidc].display_name`)
  with a default of the issuer host — cosmetic, decide at implementation.
