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

### Error Codes
- **400**: Missing/malformed auth headers
- **401**: Invalid API key
- **403**: Key valid but wrong tenant/dataset

## Isolation Layers

| Layer | Mechanism |
|-------|-----------|
| **WAL** | `{wal_dir}/{tenant_id}/{dataset_id}/{signal_type}/` |
| **Iceberg Namespace** | `[tenant_slug, dataset_slug]` |
| **Object Store** | `{base}/{tenant_slug}/{dataset_slug}/{table}/` |
| **DataFusion** | Per-tenant catalog in SessionContext |
| **Storage Backend** | Per-dataset storage override |

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

| Limit | Enforced at | On exceed |
|-------|-------------|-----------|
| `max_ingest_requests_per_sec` / `max_ingest_bytes_per_sec` | Acceptor (OTLP gRPC, remote_write) | 429 / RESOURCE_EXHAUSTED |
| `max_query_requests_per_sec` | Router HTTP query API (`/tempo`, `/api/v1`) | 429 |
| `max_api_keys` (active keys only) | Admin API key creation | 429 `quota_exceeded` |
| `max_datasets` | Admin API dataset creation | 429 `quota_exceeded` |
| `[querier] max_concurrent_queries_per_tenant` | Querier | query rejected |

Token buckets per dimension (`common::ratelimit::TenantRateLimiter`);
ingest and query budgets are independent. Storage quotas: not yet
implemented (#610).

## Admin API (Router)

Mounted at `/api/v1/admin`, requires `admin_api_key` (`src/router/src/lib.rs`):

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/api/v1/admin/tenants` | GET, POST | List/create tenants |
| `/api/v1/admin/tenants/{id}` | GET, PUT, DELETE | Manage a tenant |
| `/api/v1/admin/tenants/{id}/api-keys` | GET, POST | List/create API keys |
| `/api/v1/admin/tenants/{id}/api-keys/{key_id}` | DELETE | Revoke API key |
| `/api/v1/admin/tenants/{id}/datasets` | GET, POST | List/create datasets |
| `/api/v1/admin/tenants/{id}/datasets/{dataset_id}` | DELETE | Delete dataset |

## Tenant Self-Service API (Router)

Mounted at `/api/v1` with tenant auth (`src/router/src/endpoints/tenant.rs`):

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/api/v1/tenants` | GET | List tenants visible to the caller |
| `/api/v1/tenants/{id}` | GET | Tenant details |
| `/api/v1/tenants/{id}/tables` | GET | List tenant tables |
| `/api/v1/tenants/{id}/tables/create` | POST | Create tenant tables |
| `/api/v1/tenants/{id}/schemas` | GET | List tenant schemas |
| `/api/v1/schemas/available` | GET | List available schema definitions |

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

## Key Implementation Files

| File | Purpose |
|------|---------|
| `src/common/src/config/mod.rs` | Tenant/dataset config structs |
| `src/common/src/auth/` | Authenticator, TenantContext, middleware, validation |
| `src/common/src/catalog_manager.rs` | Slug resolution |
| `src/router/src/endpoints/admin.rs` | Admin API endpoints (incl. quota checks) |
| `src/router/src/endpoints/tenant.rs` | Tenant self-service API endpoints |
| `src/common/src/ratelimit.rs` | Per-tenant token-bucket rate limiter |
| `src/signaldb-cli/` | CLI for tenant management |
