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
  - src/router/src/endpoints/oauth.rs
  - src/router/src/read_scope.rs
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
5. For a database tenant, the resolved dataset must have a `datasets` row — `resolve_database_tenant` fails closed with `403 Dataset '<name>' not found for tenant '<id>'`. Tenant creation and update therefore **materialize the `default_dataset` as a real row**, via `Catalog::upsert_tenant_with_default_dataset` — one transaction, because a tenant row that commits without its dataset row cannot be repaired by a retry (creation 409s on an existing id). Config sync uses the idempotent `Catalog::ensure_dataset`, and `Catalog::backfill_default_datasets` converges tenants created before this at router/monolith boot (#1066). Never use `create_dataset` on a path that may run twice — it is a bare INSERT and errors on a duplicate
6. Returns `TenantContext { tenant_id, dataset_id, tenant_slug, dataset_slug }`

### Session-Cookie Fallback (Embedded UI)

When a router HTTP request has no `Authorization` header, `auth_middleware`
falls back to the `signaldb_session` cookie (an opaque random token whose
hash indexes a server-side session; helpers in
`src/common/src/auth/session.rs`). The session identifies the user; the
request's `X-Tenant-ID`/`X-Dataset-ID` headers pick the tenant, validated
against the user's memberships on every request. The cookie is set by
`POST /ui/session` and cleared by `DELETE /ui/session`
(`src/router/src/endpoints/session.rs`), both public routes on the router.
Login `tenant` is optional: the response always lists the user's
memberships (`SessionMembership { tenant_id, name, role }`); a sole
membership is auto-selected, several leave `tenant` null so the UI shows a
picker, none is a 403.

### OAuth 2.1 access tokens (MCP connectors)

A third credential type, for Claude.ai / ChatGPT connectors (change:
mcp-oauth-dcr; router serves the authorization server, see `docs/users/mcp.md`).
An `Authorization: Bearer` whose value starts with `sdb_at_` is an **opaque
OAuth access token**: `auth_middleware` routes it to
`Authenticator::authenticate_oauth_token`, which looks the token up in the
catalog and resolves `(user, tenant, scopes)` **from the token record** — not
from `X-Tenant-ID`, which is ignored for this credential (an OAuth session
cannot be pointed at a tenant it wasn't granted). Tokens are audience-bound to
the configured `mcp.oauth.resource_url` (a token for another resource is
rejected). Tenant is fixed at consent time; one connector per tenant.

**Read scopes.** OAuth scopes populate `TenantContext.api_key_scopes` and are
enforced like API-key write scopes. `can_read(<signal>)` requires the matching
`traces:read` / `logs:read` / `metrics:read` scope; a legacy unscoped key or a
human session is unrestricted. A per-signal guard on the Tempo / Loki /
Prometheus query routers rejects a scoped caller lacking the scope with `403`.
The mirror of the acceptor's `<signal>:write` ingest scopes.

**Schema scopes** (change `schema-registry`). `schema:read` (in `READ_SCOPES`, so
part of the OAuth default grant) gates `/api/v1/schema/*` reads via
`TenantContext::can_read_schema()`; `schema:write` gates custom-registry
create/replace/validate/delete via `can_write_schema()` (sessions additionally
need tenant Admin / instance admin; not OAuth-grantable). `API_KEY_SCOPES` is
the single vocabulary (`validate_scopes()`), used by key creation on every
surface. Bundled registries answer `409` on mutation regardless of scope.

### Error Codes

- **400**: Malformed auth headers (wrong scheme, invalid tenant/dataset ID)
- **401**: Missing credentials (no auth header/cookie or tenant ID), invalid API key, or expired/revoked/wrong-audience OAuth token
- **403**: Key valid but wrong tenant/dataset, or a scoped credential lacking the required `<signal>:read`/`:write` scope

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

| Endpoint                                           | Methods          | Description                                                                          |
| -------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------ |
| `/api/v1/admin/tenants`                            | GET, POST        | List/create tenants                                                                  |
| `/api/v1/admin/tenants/{id}`                       | GET, PUT, DELETE | Manage a tenant                                                                      |
| `/api/v1/admin/tenants/{id}/api-keys`              | GET, POST        | List/create API keys                                                                 |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}`     | DELETE, PATCH    | Revoke API key / update its scopes and dataset restriction                          |
| `/api/v1/admin/tenants/{id}/datasets`              | GET, POST        | List/create datasets                                                                 |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE           | Delete dataset                                                                       |
| `/api/v1/admin/users`                              | POST             | Create a human user + initial tenant membership (used by `signaldb-cli user create`) |

## Tenant Self-Service API (Router)

Mounted at `/api/v1` with tenant auth (`src/router/src/endpoints/tenant.rs`):

| Endpoint                             | Methods | Description                                                                                                                                                   |
| ------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/api/v1/whoami`                     | GET     | Authenticated tenant (id, slug, name) + datasets + default dataset (`endpoints/session.rs`)                                                                   |
| `/api/v1/tenants`                    | GET     | List tenants visible to the caller                                                                                                                            |
| `/api/v1/tenants/{id}`               | GET     | Tenant details                                                                                                                                                |
| `/api/v1/tenants/{id}/tables`        | GET     | List tenant tables                                                                                                                                            |
| `/api/v1/tenants/{id}/tables/create` | POST    | Provision the tenant's enabled signal tables across its datasets, before returning `201`. Manual trigger for what the writer's reconciler does on an interval |
| `/api/v1/tenants/{id}/schemas`       | GET     | List tenant schemas                                                                                                                                           |
| `/api/v1/schemas/available`          | GET     | List available schema definitions                                                                                                                             |

## CLI Tool

Subcommands: `query` (one required language flag —
`--sql`/`--promql`/`--logql`/`--traceql`/`--ir`), `admin`
(`tenant`/`api-key`/`dataset`), `user`, `tui`, `completions` (static shell
scripts; dynamic tenant-ID completion for tenant-taking args via
`COMPLETE=<shell> signaldb-cli` — queries the admin API like
`admin tenant list`, silently empty when the backend is unreachable).

```bash
signaldb-cli admin tenant list
signaldb-cli admin tenant create acme --name "Acme Corp" [--default-dataset production]
signaldb-cli admin api-key create acme --name "Production Key" --scope traces:write --scope schema:read
signaldb-cli admin api-key update acme <key-id> --scope traces:write --scope schema:write
signaldb-cli admin dataset create acme --name production
signaldb-cli query --sql "SELECT ..."   # also --promql/--logql/--traceql/--ir
signaldb-cli tui                         # Interactive terminal UI
```

### User credential primitives (groundwork)

`src/common/src/auth/password.rs` provides the hashing primitives for the
planned user/tenant-membership model (users-tenant-membership ADR):
Argon2id `hash_password`/`verify_password` (PHC strings) for low-entropy
user passwords, and `generate_session_token` (`sdbs_` prefix, 32 OS-random
bytes) + SHA-256 `hash_session_token` for opaque browser-session tokens.
API keys keep the existing fast SHA-256 path — the split is entropy-based.

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
