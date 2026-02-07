---
name: multi-tenancy
description: SignalDB multi-tenancy and authentication - tenant model, auth flow, isolation layers, slug-based naming, API keys, admin API, and CLI. Use when working with tenant isolation, authentication, API keys, or dataset management.
user-invocable: false
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

**Security**: Slugs validated against alphanumeric + hyphen pattern. Path traversal (`../`) is checked.

## Configuration

```toml
[auth]
enabled = true
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

## Admin API (Router)

Requires `admin_api_key`:

| Endpoint | Description |
|----------|-------------|
| `/api/v1/admin/tenants` | CRUD for tenants |
| `/api/v1/admin/tenants/{id}/api-keys` | Manage API keys |
| `/api/v1/admin/tenants/{id}/datasets` | Manage datasets |

## CLI Tool

```bash
signaldb-cli tenant list
signaldb-cli tenant create --name "Acme Corp" --slug acme
signaldb-cli api-key create --tenant acme --name "Production Key"
signaldb-cli dataset create --tenant acme --name production --slug prod
```

## Key Implementation Files

| File | Purpose |
|------|---------|
| `src/common/src/config/mod.rs` | Tenant/dataset config structs |
| `src/common/src/auth.rs` | Authenticator, TenantContext |
| `src/common/src/catalog_manager.rs` | Slug resolution |
| `src/router/src/admin.rs` | Admin API endpoints |
| `src/signaldb-cli/` | CLI for tenant management |
