## Why

Today a tenant created through the admin API (`source = "database"`) can
authenticate and **ingest** successfully, but **every query against it fails**
with `failed to resolve catalog: <tenant>`. The ingest/auth layer resolves
tenants from a merged view (config maps _and_ the database), while the querier
and compactor enumerate active tenants by reading `config.auth.tenants`
directly — so database-created tenants get no DataFusion catalog, no object
store, and no compaction/retention coverage. This is a live production defect
on the hive deployment (tenant `matter-survey`): data is silently written and
is never queryable.

The root cause is architectural: there is no single, source-agnostic answer to
"what tenants/datasets are active right now?" Config is only a **bootstrap**
source; everything beyond bootstrap already lives in the database (the
`tenants` table with its `source` discriminator, populated for config tenants
by `sync_config_tenants` at boot and for admin tenants by `create_tenant`).
The bug is that only _some_ consumers read that merged registry.

## What Changes

- Introduce a single **tenant registry** as the authoritative, source-agnostic
  enumeration of active tenants and datasets. Config-file tenants remain the
  **bootstrap** seed; the registry is the union of all registered sources
  (today: config + database) and is designed to admit further sources later
  (e.g. an external directory/IdP) without changing consumers.
- **Route every tenant-enumerating consumer through the registry instead of
  reading `config.auth.tenants` directly**:
  - **Querier**: startup DataFusion catalog + object-store registration
    (`new_with_catalog_manager`) registers a catalog/object store for **every**
    registry tenant/dataset, regardless of source. Fixes the 500s.
  - **Compactor**: planning, retention enforcement, and orphan cleanup operate
    over registry tenants/datasets, so database tenants get compaction and
    retention like config tenants.
- Ensure the registry yields everything a consumer needs to register a catalog
  uniformly across sources — tenant **slug**, dataset **slug**, default
  dataset, and effective **storage DSN** — deriving deterministic slugs and
  applying storage/schema/limit fallbacks for database-sourced records that
  carry no explicit override (config-sourced records keep their explicit
  values). No dataset's on-disk namespace changes.
- Because the querier registers catalogs at **startup**, a newly created
  database tenant is not queryable until the registry is (re)read. This change
  specifies that the registry is consulted such that a tenant becomes queryable
  without hand-editing `signaldb.toml`; whether that is achieved by
  re-resolution on demand or by a defined refresh point is a design decision
  (see design.md). At minimum, a process restart MUST make any registry tenant
  queryable — which is not true today.

This is **not** a wire-contract change: OTLP ingest, Tempo/LogQL/PromQL query
surfaces, Flight schemas, and the on-disk Iceberg/WAL layout are unchanged. It
changes _which tenants are served_, additively — no currently-working tenant
regresses.

## Capabilities

### New Capabilities

- `tenant-catalog-registry`: The source-agnostic registry of active tenants and
  datasets — config as bootstrap seed, unioned with database (and future)
  sources — and the requirement that catalog/object-store registration and
  lifecycle subsystems (querier, compactor) resolve tenants exclusively through
  it. Covers slug/storage derivation and cross-source uniformity, and the
  guarantee that any registry tenant is queryable and lifecycle-managed
  regardless of its origin.

### Modified Capabilities

<!-- None. ingest-auth-tenancy already specifies the merged auth resolver and
     is unchanged by this proposal; this change adds the query/lifecycle side
     of source-agnostic tenant resolution as a new capability. -->

## Impact

- **querier** (`src/querier/src/flight.rs`): `new_with_catalog_manager` catalog
  - object-store registration now enumerates registry tenants; requires the
    querier bootstrap to have access to the tenant registry / DB `Catalog`.
- **common** (`src/common/src/catalog_manager.rs`, `catalog.rs`, `config/`):
  `CatalogManager` (currently holds only `Configuration`) gains access to the
  merged registry; `get_enabled_tenants()` — the shared config-only enumeration
  — is replaced/superseded by a registry query returning source-agnostic
  tenant/dataset descriptors with resolved slugs and storage DSNs.
- **compactor** (`src/compactor/src/planner.rs`, `src/compactor/src/main.rs`):
  planning, retention, and orphan cleanup enumerate registry tenants.
- **router / admin API** (`src/router/src/endpoints/admin.rs`): unchanged in
  behavior; its existing `source`-stamped list already reflects the registry
  and serves as the reference merge.
- **signaldb-bin** (`src/signaldb-bin/src/main.rs`): boot wiring so querier and
  compactor receive the registry-backed catalog manager.
- No dependency, migration, or on-disk layout changes. No Flight/WAL/Iceberg
  schema changes.
