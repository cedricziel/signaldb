---
name: multi-tenancy
description: SignalDB multi-tenancy and authentication - tenant model, auth flow, isolation layers, slug-based naming, API keys, admin API, and CLI. Use when working with tenant isolation, authentication, API keys, or dataset management.
user-invocable: false
sources:
  - src/common/src/auth/**
  - src/common/src/config/mod.rs
  - src/common/src/ratelimit.rs
  - src/router/src/endpoints/admin.rs
  - src/router/src/endpoints/tenant.rs
  - src/router/src/endpoints/session.rs
---

# SignalDB Multi-Tenancy & Authentication

## Tenant Model

```
Tenant (e.g., "acme", slug: "acme")
  +-- API Keys (SHA-256 hashed, revocation support)
  +-- Datasets
  |   +-- "production" (slug: "prod", default)
  |   +-- "staging" (slug: "staging")
  +-- Schema Config (optional per-tenant overrides)
```

## Authentication Flow

1. Client sends `Authorization: Bearer <api-key>` + optional `X-Tenant-ID` / `X-Dataset-ID`
2. `Authenticator` hashes key (SHA-256) and checks:
   - Config-based keys first (from `signaldb.toml`)
   - Database-backed keys second (from service catalog)
3. Validates tenant_id matches key's tenant (403 on mismatch)
4. Resolves dataset: explicit header -> tenant default_dataset -> first `is_default` -> 400 error
5. Returns `TenantContext { tenant_id, dataset_id, tenant_slug, dataset_slug }`

### Session-Cookie Fallback (Embedded UI)

When a router HTTP request has no `Authorization` header, `auth_middleware`
falls back to the `signaldb_session` cookie. The cookie value has **two
formats**, dispatched on prefix (`session_cookie_value` in
`src/common/src/auth/session.rs` returns the raw value):

| Cookie value        | Credential   | Middleware path                                   |
| ------------------- | ------------ | ------------------------------------------------- |
| base64 JSON         | API key      | `decode_session` -> `Authenticator::authenticate` |
| starts with `sdbs_` | user session | `Authenticator::authenticate_user_session`        |

For the **API-key format** the cookie supplies the API key; explicit
`X-Tenant-ID`/`X-Dataset-ID` headers still win over cookie values. The
cookie is set by `POST /ui/session` and cleared by `DELETE /ui/session`
(`src/router/src/endpoints/session.rs`), both public routes on the router.

### User-Session Auth (Phase 1 of users-tenant-membership ADR)

`POST /ui/session` also accepts `{"email", "password"}` (untagged serde
enum alongside the API-key shape). The flow:

1. `catalog.get_user_by_email` (email canonicalized); disabled users
   rejected.
2. `verify_password` (Argon2id) on a `spawn_blocking` thread.
3. `generate_session_token()` -> `create_user_session(user_id,
   hash_session_token(token), now + 24h)`; the cookie holds the **raw**
   token, the catalog only its SHA-256 hash. 24h absolute lifetime is the
   Phase 1 default; idle timeout is deferred.
4. Failures answer a uniform 401 (`Invalid email or password`) for
   unknown email / wrong password / disabled account alike.

`Authenticator::authenticate_user_session(token, tenant?, dataset?)`
validates the token via `get_valid_session` (which already excludes
revoked, expired, and disabled-user sessions) and resolves the tenant
from `tenant_memberships`:

- explicit `X-Tenant-ID` must match a membership -> else 403
- no header + exactly one membership -> that tenant
- no header + several memberships -> 400 asking for `X-Tenant-ID`
- no memberships -> 403

Dataset/slug resolution then reuses the shared `resolve_tenant_context`
helper (config tenants first, then DB tenants), so it is identical to the
API-key path. The resulting `TenantContext` carries
`user: Option<UserIdentity { user_id, email, role }>`;
`TenantContext::new` stays API-key shaped and `with_user` attaches the
identity. The `_system` suppression in `auth_middleware` keys off the
resolved `tenant_id`, so it covers user-session requests too.

`DELETE /ui/session` revokes the session row (`get_valid_session` ->
`revoke_session`) when the cookie holds an `sdbs_` token, and clears the
cookie unconditionally. `GET /api/v1/whoami` adds
`user {id, email, display_name}` and `memberships [{tenant_id, role}]`
for user sessions (both `skip_serializing_if = "Option::is_none"`, so
API-key responses are byte-identical to before). Roles are surfaced but
**not enforced** yet — that is Phase 2.

### Error Codes

- **400**: Malformed auth headers (wrong scheme, invalid tenant/dataset ID)
- **401**: Missing credentials (no auth header/cookie or tenant ID) or invalid API key
- **403**: Key valid but wrong tenant/dataset

## Isolation Layers

| Layer                 | Mechanism                                           |
| --------------------- | --------------------------------------------------- |
| **WAL**               | `{wal_dir}/{tenant_id}/{dataset_id}/{signal_type}/` |
| **Iceberg Namespace** | `[tenant_slug, dataset_slug]`                       |
| **Object Store**      | `{base}/{tenant_slug}/{dataset_slug}/{table}/`      |
| **DataFusion**        | Per-tenant catalog in SessionContext                |
| **Storage Backend**   | Per-dataset storage override                        |

## Slug-Based Naming

All storage paths and Iceberg identifiers use **slugs** (URL-friendly), not raw IDs.

- `CatalogManager::get_tenant_slug(tenant_id)` -> slug from config, or tenant_id if not found
- `CatalogManager::get_dataset_slug(tenant_id, dataset_id)` -> slug from config, or dataset_id if not found

**Security**: Slugs validated against alphanumeric, hyphen, and underscore pattern. Path traversal (`../`) is checked.

## Configuration

Tenant auth is always enforced on the tenant-facing APIs; there is no
`enabled` flag (removed in #601).

```toml
[auth]
admin_api_key = "sk-admin-key"

[[auth.tenants]]
id = "acme"
slug = "acme"
name = "Acme Corporation"
default_dataset = "production"

[[auth.tenants.datasets]]
id = "production"
slug = "prod"
is_default = true

[[auth.tenants.datasets]]
id = "archive"
slug = "archive"
[auth.tenants.datasets.storage]
dsn = "s3://acme-archive/signals"    # Per-dataset override

[[auth.tenants.api_keys]]
key = "sk-acme-prod-key-123"
name = "Production Key"
```

## Rate Limits & Quotas

`TenantLimits` (`[auth.default_limits]`, overridable per tenant via
`[[auth.tenants]].limits`; resolved by `AuthConfig::limits_for`). Unset
fields mean unlimited; DB-provisioned tenants get the defaults.

| Limit                                                      | Enforced at                                                           | On exceed                                 |
| ---------------------------------------------------------- | --------------------------------------------------------------------- | ----------------------------------------- |
| `max_ingest_requests_per_sec` / `max_ingest_bytes_per_sec` | Acceptor (OTLP gRPC incl. profiles, OTLP/HTTP profiles, remote_write) | 429 / RESOURCE_EXHAUSTED                  |
| `max_query_requests_per_sec`                               | Router HTTP query API (`/tempo`, `/api/v1`)                           | 429                                       |
| `max_api_keys` (active keys only)                          | Admin API key creation                                                | 429 `quota_exceeded`                      |
| `max_datasets`                                             | Admin API dataset creation                                            | 429 `quota_exceeded`                      |
| `max_storage_bytes`                                        | Acceptor (OTLP gRPC incl. profiles, OTLP/HTTP profiles, remote_write) | 429 / RESOURCE_EXHAUSTED `quota_exceeded` |
| `[querier] max_concurrent_queries_per_tenant`              | Querier                                                               | query rejected                            |

Token buckets per dimension (`common::ratelimit::TenantRateLimiter`);
ingest and query budgets are independent. Storage quotas
(`common::storage_usage::StorageUsageTracker`) compare cached per-tenant
usage — refreshed from Iceberg manifests every
`[auth].storage_usage_refresh_interval` (default 60s) — against
`max_storage_bytes`; enforcement is eventually consistent by design and
usage is exported as the `signaldb.tenant.storage_usage` gauge.

## Admin API (Router)

Mounted at `/api/v1/admin`, requires `admin_api_key` (`src/router/src/lib.rs`):

| Endpoint                                           | Methods          | Description          |
| -------------------------------------------------- | ---------------- | -------------------- |
| `/api/v1/admin/tenants`                            | GET, POST        | List/create tenants  |
| `/api/v1/admin/tenants/{id}`                       | GET, PUT, DELETE | Manage a tenant      |
| `/api/v1/admin/tenants/{id}/api-keys`              | GET, POST        | List/create API keys |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}`     | DELETE           | Revoke API key       |
| `/api/v1/admin/tenants/{id}/datasets`              | GET, POST        | List/create datasets |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE           | Delete dataset       |

## Tenant Self-Service API (Router)

Mounted at `/api/v1` with tenant auth (`src/router/src/endpoints/tenant.rs`):

| Endpoint                             | Methods | Description                                                                                 |
| ------------------------------------ | ------- | ------------------------------------------------------------------------------------------- |
| `/api/v1/whoami`                     | GET     | Authenticated tenant (id, slug, name) + datasets + default dataset (`endpoints/session.rs`); user sessions also get `user` + `memberships` |
| `/api/v1/tenants`                    | GET     | List tenants visible to the caller                                                          |
| `/api/v1/tenants/{id}`               | GET     | Tenant details                                                                              |
| `/api/v1/tenants/{id}/tables`        | GET     | List tenant tables                                                                          |
| `/api/v1/tenants/{id}/tables/create` | POST    | Create tenant tables                                                                        |
| `/api/v1/tenants/{id}/schemas`       | GET     | List tenant schemas                                                                         |
| `/api/v1/schemas/available`          | GET     | List available schema definitions                                                           |

## CLI Tool

Subcommands: `tenant`, `api-key`, `dataset`, `query` (SQL), `tui`.

```bash
signaldb-cli tenant list
signaldb-cli tenant create acme --name "Acme Corp" [--default-dataset production]
signaldb-cli api-key create acme --name "Production Key"
signaldb-cli dataset create acme --name production
signaldb-cli query ...          # SQL queries against SignalDB
signaldb-cli tui                # Interactive terminal UI
```

### User credential primitives

`src/common/src/auth/password.rs` provides the hashing primitives for the
user/tenant-membership model (users-tenant-membership ADR): Argon2id
`hash_password`/`verify_password` (PHC strings) for low-entropy user
passwords, and `generate_session_token` (`sdbs_` prefix, 32 OS-random
bytes) + SHA-256 `hash_session_token` for opaque browser-session tokens.
API keys keep the existing fast SHA-256 path — the split is entropy-based.
These are wired up by the user-session auth flow described above.

## Key Implementation Files

| File                                  | Purpose                                              |
| ------------------------------------- | ---------------------------------------------------- |
| `src/common/src/config/mod.rs`        | Tenant/dataset config structs                        |
| `src/common/src/auth/`                | Authenticator, TenantContext, middleware, validation |
| `src/common/src/auth/password.rs`     | Argon2id password hashing + opaque session tokens    |
| `src/common/src/catalog_manager.rs`   | Slug resolution                                      |
| `src/router/src/endpoints/admin.rs`   | Admin API endpoints (incl. quota checks)             |
| `src/router/src/endpoints/tenant.rs`  | Tenant self-service API endpoints                    |
| `src/router/src/endpoints/session.rs` | UI session login/logout + whoami endpoints           |
| `src/common/src/auth/session.rs`      | Session cookie codec (`signaldb_session`)            |
| `src/common/src/ratelimit.rs`         | Per-tenant token-bucket rate limiter                 |
| `src/signaldb-cli/`                   | CLI for tenant management                            |

Under `[compactor.attr_promotion]` (auto-promotion decision pass), a tenant's resolved materialized-label allowlist is the _pinned_ set: those keys are never demotion candidates.
