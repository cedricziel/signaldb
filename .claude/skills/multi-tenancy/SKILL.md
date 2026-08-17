---
name: multi-tenancy
description: SignalDB multi-tenancy and authentication - tenant model, auth flow, isolation layers, slug-based naming, API keys, admin API, and CLI. Use when working with tenant isolation, authentication, API keys, or dataset management.
user-invocable: false
---

# SignalDB Multi-Tenancy & Authentication

Read `docs/users/authentication.md` for the credential model (API keys,
session cookies, OAuth tokens), headers, error codes, rate limits/quotas, and
the admin/self-service HTTP APIs. Read `docs/operations/table-provisioning.md`
for how a tenant's Iceberg tables come into existence. Read
`docs/architecture/decisions/users-tenant-membership.md` for the phased
human-user/role model (Argon2id passwords, `tenant_memberships`, sessions) —
implementation is foundation-only today, no role enforcement yet.

Isolation-layer paths (WAL, Iceberg namespace, object store) and slug
resolution (`get_tenant_slug`/`get_dataset_slug`) are the `storage-layout`
skill's domain, not this one. Full `[auth]` TOML example lives in the
`configuration` skill.

## Gotcha not in the docs above

Tenant/dataset creation must stay one transaction: for a database tenant,
`resolve_database_tenant` fails closed (`403`) if the resolved dataset has no
`datasets` row. `Catalog::upsert_tenant_with_default_dataset` therefore
materializes the tenant row and its `default_dataset` row together — a
tenant that commits without its dataset can't be repaired by retrying
(create 409s on an existing id). Config sync uses idempotent
`Catalog::ensure_dataset`; `backfill_default_datasets` converges pre-#1066
tenants at boot. Never call `create_dataset` on a path that may run twice —
it's a bare INSERT, errors on a duplicate.
