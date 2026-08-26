## Why

Human login is email + password only. Self-hosted operators run an identity
provider (Authentik, Keycloak, Pocket ID, Dex, or a hosted IdP) and expect
every service to defer to it — per-service passwords are the thing homelab and
team deployments are trying to eliminate. The users-tenant-membership ADR
planned OIDC as phase 3: "an alternative credential on the same `users` row."
The session, membership, role, and `TenantContext` machinery it needs all
exist; only the credential is missing.

## What Changes

- SignalDB acts as an OIDC relying party against a single configured provider
  (`[auth.oidc]`): authorization-code flow with PKCE, discovery via
  `.well-known/openid-configuration`, ID-token validation (signature, issuer,
  audience, expiry, nonce).
- A successful callback resolves or creates a user and issues the standard
  session cookie — no new session mechanism, and the MCP OAuth
  authorize/consent flow therefore works with SSO unchanged.
- Just-in-time user provisioning, gated by an optional email-domain/claim
  allowlist. Users gain a nullable `(oidc_issuer, oidc_subject)` identity;
  `password_hash` becomes nullable for SSO-only users.
- Memberships stay locally managed; optional config rules map an IdP group
  claim to tenant memberships and roles, re-applied at each login without
  touching locally granted memberships. `tenant_memberships` is re-keyed by
  `(user_id, tenant_id, granted_by)` so local and mapped grants are separate
  rows; the effective role is the higher of the two.
- Password login remains available and can be disabled by config once OIDC is
  configured; the CLI/config bootstrap path and `admin_api_key` remain as
  break-glass regardless. An unreachable IdP at startup degrades SSO (with
  background retry) rather than stopping the instance, so break-glass holds
  across restarts too.
- The login page advertises and offers SSO when the server has OIDC
  configured; the OIDC endpoints and the login-configuration probe are part of
  the published API contract.

## Capabilities

### New Capabilities

- `oidc-login`: authenticating humans against an external OIDC provider —
  configuration and discovery, the login flow, token validation, JIT
  provisioning and identity linking, group-claim mapping, coexistence with
  password login, and the contract surface (endpoints, login-config probe).

### Modified Capabilities

<!-- none: password login, sessions, memberships, and the MCP OAuth flow keep
     their existing requirements; oidc-login layers a new credential onto them.
     The pending auth-surface-consistency change already owns the
     admin-management-api-contract security-scheme delta; oidc-login's contract
     requirements live in its own spec to avoid conflicting deltas. -->

## Impact

- Crates: `common` (config `[auth.oidc]`, catalog columns
  `users.oidc_issuer`/`oidc_subject` + nullable `password_hash`,
  `tenant_memberships.granted_by` + re-keyed primary key with source-aware
  membership operations, authenticator user-resolution), `router` (OIDC
  start/callback endpoints, login-config probe, OpenAPI additions, startup
  discovery with degraded mode), `signaldb-sdk` and the UI TypeScript client
  (regenerated), `signaldb-cli` (unchanged behaviour; `user create` gains
  nothing), `tests-integration` (IdP testcontainer).
- New dependency: the `openidconnect` crate (RP-side flow, JWKS validation).
- UI: `src/ui` login panel (SSO button, hidden password form when disabled).
- Config: new `[auth.oidc]` section; BREAKING nothing — purely additive, and
  no OTLP ingest, query-compat, Flight wire, or WAL/Iceberg surface changes.
- Docs: `docs/users/authentication.md`, `docs/operations/` (IdP setup guide);
  `multi-tenancy` and `configuration` skills.
- Relationship: independent of the pending `auth-surface-consistency` change;
  both touch router auth but no shared requirement is modified by both.
