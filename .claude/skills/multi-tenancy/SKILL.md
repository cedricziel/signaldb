---
name: multi-tenancy
description: SignalDB multi-tenancy and authentication - tenant model, auth flow, isolation layers, slug-based naming, API keys, admin API, and CLI. Use when working with tenant isolation, authentication, API keys, or dataset management.
user-invocable: false
sources:
  - src/common/src/auth/**
  - src/common/src/config/mod.rs
  - src/common/src/catalog.rs
  - src/acceptor/src/lib.rs
  - src/common/src/schema/type_authority.rs
  - src/common/src/schema/type_authority/**
  - src/common/src/schema_registry/type_hints.rs
  - src/router/src/endpoints/schema.rs
  - src/common/src/ratelimit.rs
  - src/router/src/endpoints/tenants.rs
  - src/router/src/endpoints/management.rs
  - src/router/src/endpoints/tenant.rs
  - src/router/src/endpoints/session.rs
  - src/router/src/endpoints/oauth.rs
  - src/router/src/endpoints/oidc.rs
  - src/router/src/oidc.rs
  - src/router/src/github.rs
  - src/router/src/endpoints/github.rs
  - src/router/src/source_context.rs
  - src/router/src/endpoints/source_context.rs
  - src/router/src/read_scope.rs
  - src/signaldb-cli/src/commands/tenant_self.rs
  - src/mcp-server/src/server.rs
---

# SignalDB Multi-Tenancy & Authentication

Read `docs/users/authentication.md` for the credential model (API keys,
session cookies, OAuth tokens with single- vs multi-tenant grants), headers,
error codes, API-key scopes (incl. `tenant:manage`, which a legacy unscoped
key never gains), dataset and origin restrictions, rate limits/quotas, and the
admin, self-service, and management HTTP APIs (all under `/api/v1`; the
break-glass `admin_api_key` reaches `/api/v1/tenants[/{id}]`, `/api/v1/users`,
and a tenant's `api-keys`/`datasets`/`memberships`).

Related homes:

- `docs/operations/oidc-sso.md` — SSO login, JIT provisioning, source-keyed
  memberships (`granted_by`), rollback; `docs/operations/github-app.md` —
  GitHub App linking, `attach` (instance-admin only), source context.
- `docs/users/schema-registry.md#canonical-types` — the attribute type
  authority (per tenant + dataset + signal + level + key, first write wins,
  pins, propagation delay).
- `docs/users/processors.md` and `docs/users/eval-sets.md` — the
  `processors:*`/`evals:*` scopes and their tenant/dataset scoping.
- `docs/users/sending-otlp.md#browser-cors-ingestion` — how an origin
  restriction is enforced on the OTLP/HTTP path.
- `docs/users/mcp.md` — the OAuth connector, introspection, and how MCP tools
  take a tenant for multi-tenant grants.
- `docs/users/client-retry.md` — the 429 retry-after contract (headers, body
  shape, `signaldb_rate_limit_rejections_total`) and how the SDK/CLI/MCP/UI
  clients retry it.
- `docs/operations/table-provisioning.md` — how a tenant's Iceberg tables come
  into existence.
- `docs/architecture/decisions/users-tenant-membership.md` — the human-user/
  role model (Argon2id passwords, `tenant_memberships`, sessions).

Isolation-layer paths (WAL, Iceberg namespace, object store) and slug
resolution (`get_tenant_slug`/`get_dataset_slug`) are the `storage-layout`
skill's domain, not this one. The `[auth]` TOML surface lives in
`signaldb.dist.toml` (`configuration` skill).

## Gotchas not in the docs above

- Tenant/dataset creation must stay one transaction: for a database tenant,
  `resolve_database_tenant` fails closed (`403`) if the resolved dataset has
  no `datasets` row. `Catalog::upsert_tenant_with_default_dataset` therefore
  materializes the tenant row and its `default_dataset` row together — a
  tenant that commits without its dataset can't be repaired by retrying
  (create 409s on an existing id). Config sync uses idempotent
  `Catalog::ensure_dataset`; `backfill_default_datasets` converges pre-#1066
  tenants at boot. Never call `create_dataset` on a path that may run twice —
  it's a bare INSERT, errors on a duplicate.
- `tenant:manage` is checked by `TenantContext::can_manage_via_key()` against
  the key's **explicit** scopes, deliberately not
  `has_scope_or_unrestricted`. Human sessions never satisfy it; they go
  through membership roles.
- `github-installations` handlers take a `TenantContextExtractor`, so they are
  excluded from the admin-key bypass (`is_admin_key_bypass_path` in
  `src/router/src/lib.rs`); routing the admin key to them 500s.
- OIDC membership sync only touches `granted_by = 'oidc_mapping'` rows;
  admin/CLI/MCP membership writes are pinned to `local`. Keep it that way.
