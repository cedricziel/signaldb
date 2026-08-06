## Context

See proposal.md — Why. The relevant current-state facts (verified against the
tree):

- The database already holds a **merged** tenant table. `Catalog` owns
  `tenants` (with a `source` column: `'config' | 'database'`), `datasets`, and
  `api_keys` (`src/common/src/catalog.rs`). At boot,
  `Catalog::sync_config_tenants(&auth)` (catalog.rs:2072) upserts every
  config tenant/dataset/api-key into these tables with `source="config"`;
  `admin::create_tenant` upserts admin tenants with `source="database"`. So
  after startup the DB is the union of both sources.
- The **auth path already reads that union**. `Authenticator`
  (`src/common/src/auth/authenticator.rs`) checks config maps first, then falls
  back to `Catalog::validate_api_key` + `get_tenant` + `get_datasets`. This is
  why admin-API tenants ingest and pass `whoami`.
- The **query/lifecycle path does not**. `CatalogManager`
  (`src/common/src/catalog_manager.rs`) holds only `Configuration` (no
  `Arc<Catalog>`); its `get_enabled_tenants()` (catalog_manager.rs:159)
  iterates `config.auth.tenants`. The querier's startup registration
  (`QuerierFlightService::new_with_catalog_manager`, flight.rs:401→448) and the
  compactor's planner/retention/orphan loops (planner.rs:109; main.rs:468/514/527)
  all consume that config-only list. Database tenants therefore get no
  DataFusion catalog, no object store, and no lifecycle coverage.
- Slug/namespace resolution already goes through `CatalogManager::get_tenant_slug`
  / `get_dataset_slug` (catalog_manager.rs:145-146), used by both write and
  read paths. The DB `tenants`/`datasets` tables store no slug and no storage
  override; those live in config (`TenantConfig.slug`, `DatasetConfig.storage`).

## Goals / Non-Goals

**Goals:**

- One source-agnostic enumeration of active tenants/datasets that querier and
  compactor consume in place of `get_enabled_tenants()`.
- The registry is the DB union overlaid with config-owned detail, so admin-API
  tenants become queryable and lifecycle-managed on restart with no
  `signaldb.toml` edit.
- Read and write namespaces stay identical (reuse the existing slug functions).

**Non-Goals:**

- Moving storage DSNs into the database. DSNs reference env/secret
  interpolation and stay config-owned.
- Any change to OTLP ingest, query-API surfaces, Flight schemas, or on-disk
  layout.

## Decisions

### D1 — The registry is the DB union overlaid with config, not a new store

The union already exists in the DB after `sync_config_tenants`. Rather than
build a parallel store, the registry enumerates from the DB
(`Catalog::list_tenants` + `get_datasets`) and **overlays** config-owned detail
(explicit slug, per-dataset `storage`, `schema_config`/materialized labels,
`limits`) for tenants that config defines; for records with no config entry
(database-sourced) it derives defaults.

- **Membership** is DB-authoritative → admin tenants are included.
- **Detail** falls back global → tenant → dataset exactly as
  `get_dataset_storage_config` / `get_tenant_schema_config` already do.

_Alternative rejected:_ add `slug`/`storage` columns to the DB and read the DB
alone. Bigger migration, and it would duplicate/persist storage DSNs (with
embedded secrets) into the catalog DB — a worse security and config story.

### D2 — Give the enumeration access to the DB `Catalog`

`CatalogManager` gains an `Option<Arc<Catalog>>` (or a dedicated
`TenantRegistry { catalog, config }` in `common` that both the querier
registration and the compactor consume). `get_enabled_tenants()` is superseded
by a registry query returning **source-agnostic descriptors** —
`{ tenant_id, tenant_slug, datasets: [{ id, slug, storage_dsn, is_default }],
default_dataset, enabled }` — with slugs produced by the **existing**
`get_tenant_slug`/`get_dataset_slug` so read/write namespaces cannot diverge.
When no `Catalog` is wired (pure in-memory/unit contexts), the registry falls
back to config-only, preserving today's behavior for those paths.

_Alternative rejected:_ keep `CatalogManager` config-only and pass a second
tenant list alongside it. That leaves two competing notions of "the tenants,"
which is the exact bug.

### D3 — Enumerate at startup AND resolve lazily on demand (no restart)

Two registration points, because a tenant must be usable the moment it is
created:

1. **Startup enumeration.** `new_with_catalog_manager` keeps its structure
   (object-store dedup via `registered_urls`, one catalog per tenant slug) but
   iterates the registry instead of `get_enabled_tenants()`, so every existing
   registry tenant (any source) is registered at boot. The compactor loops
   likewise.
2. **Lazy on-demand registration.** The querier already authenticates every
   request through `Authenticator`, which resolves DB tenants. When an
   authenticated query targets a tenant/dataset whose DataFusion catalog is not
   yet registered in the running `SessionContext`, the querier resolves it from
   the registry and registers the object store + catalog **before executing the
   query**. `SessionContext::register_catalog` / `register_object_store` are
   runtime-safe; registration is made idempotent (check-then-insert, guarded so
   concurrent first-queries for the same tenant register once). This is what
   delivers "queryable the moment created" across both monolithic and
   microservice deployments — the querier is a separate process from the router
   that created the tenant, so a pull-on-demand model is the robust choice
   rather than a cross-service push.

Writes already resolve new tenants immediately (the acceptor's `Authenticator`
reads the DB), so no acceptor change is needed for the write side; the
compactor picks up new tenants on its next scheduled pass via the registry.

_Alternative rejected:_ push catalog registration from the admin `create_tenant`
handler. It only works in-process (monolithic) and races the querier's own
state; lazy pull is deployment-agnostic and self-heals.

### D4 — Preserve the `enabled` filter and config overrides

A config tenant with `schema_config.enabled = false` stays excluded; DB tenants
default enabled. Config-defined explicit slug/storage overrides win over
derivation (the write path used them, so the read path must too).

## Risks / Trade-offs

- **Slug divergence between read and write** → catalogs would point at an empty
  namespace. _Mitigation:_ the registry MUST derive slugs via the same
  `get_tenant_slug`/`get_dataset_slug` the acceptor/writer use; add a test that
  ingests for a DB tenant and asserts the querier resolves the same namespace.
- **Startup ordering** — the querier/compactor must enumerate _after_
  `sync_config_tenants` has run and the DB is reachable. _Mitigation:_ in
  monolithic mode sync already precedes service start (main.rs:107); for
  standalone binaries, wire the same DB `Catalog` used for auth, and fail fast
  with context if it is unavailable.
- **Lazy registration on the query path** adds a first-query cost (a registry
  lookup + object-store/catalog registration) for each not-yet-seen tenant, and
  must be concurrency-safe. _Mitigation:_ idempotent check-then-register guarded
  so concurrent first-queries for the same tenant register exactly once;
  subsequent queries hit the already-registered catalog with no extra cost.
- **In-memory/test contexts without a DB** → registry must degrade to
  config-only so existing unit tests are unaffected.

## Migration Plan

- No data migration, no schema change, no on-disk layout change. Deploy is a
  normal rolling restart.
- **Live hive incident**: after deploying this change, tenant `matter-survey`
  becomes queryable with **no** `signaldb.toml` edit and **no** restart required
  for future tenants; on this deploy's restart its already-ingested data
  (written to the global-default namespace under the `matter-survey`/`production`
  slugs) resolves, and any subsequently-created tenant is queryable on its first
  query.
- **Rollback**: revert the change; config tenants are unaffected (they are
  still enumerated from config via the fallback), and the pre-existing manual
  workaround (config block + restart) still works.
