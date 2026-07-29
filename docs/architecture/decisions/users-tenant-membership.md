---
audience: contributor
type: decision-record
status: record
---

# Users and Tenant Membership

## Status

Accepted — implementation phased; this record describes the target model and the
rationale for introducing human users alongside the existing API-key model.

## Context

SignalDB authentication today has no notion of a person. The only principals
are:

- **Tenant API keys** — machine credentials bound to exactly one tenant,
  validated by `Authenticator` (config-defined keys first, then the catalog's
  `api_keys` table). The result of authentication is a `TenantContext`
  (tenant, dataset, slugs, key name) consumed by every downstream layer:
  HTTP middleware, the acceptor's gRPC interceptor, WAL pathing, Iceberg
  namespacing, and rate limiting.
- **A single admin API key** — one static shared secret guarding the entire
  admin API, with no identity behind it.

This creates four concrete problems:

1. **No human identity or audit.** The closest thing to "who did this" is the
   optional API key name. Admin actions are anonymous.
2. **Credential-hostile UI login.** The embedded explore UI asks the user to
   paste a raw tenant API key, which is then stored (base64, unencrypted) in
   the session cookie and replayed on every request. Logout invalidates
   nothing; a leaked cookie is a leaked long-lived credential.
3. **One person, one tenant.** A key binds to a single tenant, so an operator
   who works across tenants juggles multiple keys manually. `whoami` can only
   ever answer with the key's one tenant.
4. **No roles.** Within a tenant every key is equally powerful; there is no
   viewer/member/admin distinction.

## Decision

Introduce **users** as a second kind of principal, connected to tenants
through an explicit **membership** relation, while keeping API keys as the
machine credential for ingestion.

### Key architectural constraint: `TenantContext` is the narrow waist

All request handling downstream of authentication consumes `TenantContext`
and does not care how it was produced. Users are therefore introduced as a
*second way to produce a `TenantContext`* — the entire data plane (acceptor,
writer, querier, WAL, Iceberg layout, rate limiting) is untouched.

### Data model (service catalog)

```text
users              (id, email UNIQUE, display_name, password_hash,
                    is_instance_admin, created_at, updated_at, disabled_at)
tenant_memberships (user_id, tenant_id, role IN ('admin','member','viewer'),
                    created_at, PRIMARY KEY (user_id, tenant_id))
user_sessions      (id, token_hash UNIQUE, user_id, created_at,
                    expires_at, revoked_at)
```

Both catalog backends (SQLite and PostgreSQL) carry the same tables,
following the existing inline-DDL pattern.

Email is the login identity and is **case-insensitive**: values are
canonicalized (trimmed, lowercased) at account creation and at login, and
the unique constraint applies to that canonical form. Normalizing in the
application keeps identity semantics identical on both backends instead of
depending on backend-specific collation behavior.

### Authentication model

- **API keys stay** as the machine credential for OTLP ingestion. The gRPC
  path does not change. API keys may later gain a `created_by` user column
  for provenance.
- **Users are for humans**: the embedded UI, tenant self-service API, and
  admin API. Login is email + password; passwords are hashed with
  **Argon2id** (memory-hard KDF for low-entropy secrets). The existing
  unsalted SHA-256 helper remains correct for high-entropy random API keys
  and session tokens, and wrong for passwords.
- **Server-side sessions replace the raw-key cookie.** The session cookie
  becomes an opaque random token whose SHA-256 hash is stored in
  `user_sessions`, so logout and revocation actually work and no long-lived
  credential lives in the browser. The token is still a bearer credential,
  so the session contract bounds its blast radius: sessions carry a bounded
  absolute lifetime (`expires_at`) plus an idle timeout; the cookie is set
  `HttpOnly; Secure; SameSite=Strict` (the current UI cookie already ships
  `HttpOnly` and `SameSite=Strict`); a fresh token is issued on every login
  rather than reusing an existing one; and CSRF is mitigated by
  `SameSite=Strict` combined with origin checks on state-changing requests.
  The improvement over the raw-key cookie is bounded lifetime and real
  server-side revocation — not immunity to cookie theft.
- **Disabled users are cut off immediately.** A non-null `disabled_at`
  fails both password login and session validation: session lookup joins
  against `users`, so disabling a user invalidates their existing sessions
  at the next request without requiring per-session revocation.
- **Tenant resolution via membership.** For a user request, the requested
  tenant is validated against `tenant_memberships` instead of key ownership.
  This is what makes one-person-many-tenants work; `whoami` naturally
  returns all memberships. `TenantContext` gains optional `user_id`/`role`
  fields so handlers can enforce roles and audit logs name a person.
- **Instance admin** becomes a flag on users, giving admin-API actions a real
  identity. The static `admin_api_key` remains as a break-glass and
  automation credential.
- **Bootstrap** via CLI (`signaldb-cli user create ... --instance-admin`)
  and/or a config-declared initial user, mirroring tenant bootstrap.

### Roles

Per-tenant roles start minimal:

| Role     | Intent                                            |
| -------- | ------------------------------------------------- |
| `admin`  | Manage the tenant: datasets, API keys, members    |
| `member` | Read and write data, use self-service API         |
| `viewer` | Read-only queries                                 |

## Phasing

1. **Foundation**: catalog tables and CRUD; Argon2id password hashing and
   session-token utilities; password login issuing session tokens; extended
   `whoami`. No role enforcement yet (every member acts as today's
   key-holder).
2. **Roles**: enforcement on the tenant self-service and admin APIs;
   membership management endpoints (instance-admin and tenant-admin).
3. **Later**: OIDC/SSO as an alternative credential on the same `users` row;
   possibly unifying API keys as service accounts.

## Consequences

- The ingestion path keeps its exact current behavior and performance; no
  agent configuration changes.
- Session validation adds a catalog lookup per UI request; a small in-memory
  cache with TTL bounds the cost (config-defined API keys are already served
  from memory today).
- The previous cookie format (base64 of the raw API key) is replaced;
  existing UI sessions simply re-login once.
- The `_system` self-monitoring tenant suppression in the auth middleware
  must keep applying to user-originated requests that resolve to the
  `_system` tenant.
- Two credential planes (keys for machines, passwords/sessions for humans)
  are deliberate: neither can be removed without breaking the other's use
  case, and each uses hashing appropriate to its entropy.
