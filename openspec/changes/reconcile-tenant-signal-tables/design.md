## Context

See proposal.md — Why. The state that shapes the approach, all verified against
the code during review:

- Signal tables are created exactly one way today: `IcebergTableWriter::new`
  calls `CatalogManager::ensure_table` on the write path
  (`src/writer/src/storage/iceberg.rs:69-79`). No tenant-creation path touches
  the Iceberg catalog (`src/router/src/endpoints/admin.rs:63-150`,
  `src/common/src/catalog.rs:1774-1817`).
- `IcebergTableManager::ensure_table` (`src/common/src/iceberg/table_manager.rs:94-230`)
  is load-or-create and idempotent: one `load_tabular` round-trip, create on
  NotFound, tolerate `AlreadyExists` and the SQLite unique-constraint variant
  from a concurrent caller. It also backfills the #895/#959 metadata-pruning
  properties on pre-existing tables, committing at most once per table. It
  rejects any table name outside the eight known ones.
- `TableSchema::all_from_config` (`src/common/src/iceberg/schemas.rs:597-626`)
  gates the eight tables on per-signal flags **and appends a `Custom` entry per
  `custom_schemas` key**, which `ensure_table` cannot create.
- The enabled set is per-tenant: `Configuration::get_tenant_schema_config`
  returns the tenant's whole `SchemaConfig` when present
  (`src/common/src/config/mod.rs:1477-1486`), and `ensure_table` already resolves
  per-tenant for materialized labels (`src/common/src/catalog_manager.rs:200-206`).
- `list_active_tenants` returns config tenants only when no tenant source is
  attached (`src/common/src/catalog_manager.rs:329-331`). The writer is the one
  service that does not attach one (`src/writer/src/main.rs:138`; compare
  `src/querier/src/main.rs:141`, `src/compactor/src/main.rs:133`,
  `src/signaldb-bin/src/main.rs:169`).
- Admin tenant creation stores `default_dataset` as a **column on the tenant
  row**, not a dataset row, so `db_tenant_descriptor` resolves such a tenant with
  `datasets: []` (`src/common/src/catalog_manager.rs:257-280`).
- The querier's `TenantCatalog`/`LiveIcebergSchema` (`src/querier/src/flight.rs:41-151`)
  hit the live catalog per request rather than `datafusion_iceberg`'s snapshot
  `Mirror`, so a table created by another process is visible to a running querier
  with no re-registration. `LiveIcebergSchema::table` maps `CatalogNotFound` to
  `Ok(None)`; `LiveIcebergSchema::table_exist` is a hardcoded `true`
  (`flight.rs:98-100`) because the sync trait method cannot do the async catalog
  round-trip a real check needs.
- `POST /tenants/{tenant_id}/tables/create` is routed
  (`src/router/src/endpoints/tenant.rs:20`), handled (`:130`), tested (`:353`),
  and documented (`docs/users/authentication.md:143`,
  `.claude/skills/multi-tenancy/SKILL.md:177`). Its handler reaches
  `SchemaRegistry::create_default_tables_for_tenant`
  (`src/common/src/schema/mod.rs:394-420`), which logs `"Would create table …"`
  and creates nothing.
- FDAP constraint: schemas and partition specs come from
  `common::iceberg::schemas`, which builds on the Arrow/Parquet types re-exported
  through DataFusion. Reconciliation reuses those constructors unchanged — it
  must not grow a second definition of a table's schema.
- The v1 wire → v2 storage transform in the writer
  (`apply_schema_transformation_if_needed`) is unaffected: reconciliation creates
  storage-format (v2) tables, exactly what the transform already targets.

## Goals / Non-Goals

**Goals:**

- One reconcile entry point, built on `ensure_table`, safe to call repeatedly and
  concurrently with ingest.
- Convergence for datasets that already exist, including ones created before this
  change, without operator action.
- Steady-state cost near zero: a converged deployment issues no catalog traffic
  for reconciliation.
- Read paths that treat a missing table as "no data" independently of whether
  reconciliation has run.

**Non-Goals:**

- Schema evolution of existing tables. `ensure_table` backfills properties, never
  columns; a materialized-label config change still does not reach tables that
  already exist. Unchanged by this design (see Risks).
- Dropping tables for signal types an operator later disables. Reconciliation is
  create-only.
- Custom-schema tables. `all_from_config` emits them, `ensure_table` cannot
  create them; they stay a config-only concept.
- Writing dataset rows for tenants created with only a `default_dataset`. The
  reconciler provisions that dataset's namespace; bringing it under
  compaction/retention enumeration is a separate pre-existing gap, filed
  separately.

## Decisions

### Decision 1: Reconcile per dataset via `CatalogManager`, reusing `ensure_table`

Add `CatalogManager::ensure_dataset_tables(tenant_id, dataset_id)` that resolves
the enabled set from **the tenant's** schema config, skips `TableSchema::Custom`,
and calls the existing `ensure_table` per remaining table, returning a report of
created / already-present / failed tables.

_Why:_ `CatalogManager` already owns the catalog handle, slug resolution, and the
per-tenant config lookup. Building on `ensure_table` guarantees provisioned
tables are what the write path would have produced — same schema, partition spec,
bloom-filter, compression, and metadata-pruning properties — which is the spec's
"indistinguishable from the ingest path's table" requirement.

_Per-tenant, not global:_ the config already supports a per-tenant
`SchemaConfig`; resolving globally would provision five metrics tables for a
tenant that explicitly disabled metrics.

_Alternative rejected:_ a standalone provisioning module with its own
`CreateTableBuilder` — it would duplicate the property/schema assembly
`ensure_table` performs and drift from it the first time either side changes.

### Decision 2: The writer owns the loop; startup pass plus periodic re-run

The writer runs a reconcile pass at startup over the tenant registry, then
re-runs on an interval from a new `[writer]` config key (default 5m; zero
disables). Failures are logged with tenant/dataset/table and retried next pass;
nothing fails startup.

Two pieces of wiring this requires, neither of which exists today:

- **Attach a tenant source to the writer's `CatalogManager`.** Without it
  `list_active_tenants` yields config tenants only and the reconciler would skip
  every admin-API tenant — the exact population this change targets. The writer's
  `ServiceBootstrap` is moved into `InMemoryFlightTransport`
  (`src/writer/src/main.rs:118`) before the `CatalogManager` is built, so
  `bootstrap.catalog()` must be cloned out beforehand.
- **Expose the loop as `start_table_reconciler()` on the ingest service**,
  mirroring the existing `start_background_processing()`. `src/writer/src/main.rs`
  and `src/signaldb-bin/src/main.rs` are independent wirings with no shared
  startup path, so monolithic mode does _not_ inherit this for free; both mains
  call the same start method with their own `CatalogManager`.

_Why the writer:_ it is the only service that creates these tables today, and it
runs continuously.

_Alternative rejected — the compactor:_ right shape
(`active_tenants_or_empty` × datasets in `lifecycle.rs`) but operators disable
compaction, and provisioning must not be collateral damage of that choice.

_Alternative rejected — the querier on lazy registration:_ ties table creation to
read traffic, giving a read path a write side effect, and leaves never-queried
datasets unprovisioned.

### Decision 3: Convergence is the mechanism; the existing endpoint becomes the manual trigger

Table creation stays out of the `POST /api/v1/admin/tenants` transaction — a new
tenant's tables appear within one reconcile interval, and the read guard makes
the interim window return empty results either way.

Separately, `POST /tenants/{tenant_id}/tables/create` is rewired to
`ensure_dataset_tables`. It keeps its route, request shape, and `201` response;
only its effect becomes real. That gives operators an immediate trigger without
coupling tenant creation to catalog availability, and removes an endpoint that
currently reports success for work it never did.

### Decision 4: Read paths guard on the async table lookup, not on `table_exist` and not on error strings

Metadata/label/search paths resolve their table through
`state.schema_for_ref(table_ref)?.table(name).await` and treat `Ok(None)` as
absence, yielding an empty result. Errors from a table that does exist propagate
unchanged.

_Why not `SessionContext::table_exist`:_ it is sync and delegates to the schema
provider's sync `table_exist`, which for `LiveIcebergSchema` is a hardcoded
`true` (`src/querier/src/flight.rs:98-100`) — the guard would never fire. It also
returns `Result` and errors rather than returning `false` when the catalog or
schema cannot be resolved, blurring "absent" against "unknown tenant".

_Why not matching `"No table named"`:_ string matching on DataFusion error text
is brittle across upgrades and would swallow genuine planning failures that
happen to mention a table.

The async lookup is exactly the signal DataFusion itself turns into
`No table named 'logs'`, so keying on `Ok(None)` catches the real condition at
its source while keeping "absent" and "broken" distinguishable.

All four signal families are in scope, traces included — contrary to issue #972's
description, no read path has such a guard today
(`src/querier/src/query/trace.rs:84-96`). The Query-IR surface
(`src/querier/src/query/ir_planner.rs:305`) is in scope too.

Separately, `querier_error_to_status` (`src/querier/src/flight.rs:2252-2261`) is
a single mapper that labels every signal's failure `"Profile query failed"`; each
signal's path gets its own wrapper.

### Decision 5: In-memory "already ensured" set to keep steady state free

The reconciler keeps a process-local set of `(tenant, dataset, table)` triples it
has confirmed. A converged deployment issues no catalog calls per pass; only
newly seen datasets are checked. The set is rebuilt on restart, so the startup
pass is always a real check.

_Why:_ without it each pass costs up to eight `load_tabular` round-trips per
dataset against the SQL catalog — negligible for a handful of datasets, wasteful
at hundreds, pure overhead once converged. Staleness is benign: the worst case is
skipping a table deleted out-of-band, which the write path recreates and a
restart re-checks.

Multi-writer deployments need no lease: concurrent `ensure_table` is already safe
(`table_manager.rs:141-153, 208-230`). The set is per process, so N writers each
pay one confirming pass.

### Decision 6: Provision a tenant's default dataset even without a dataset row

The reconciler iterates each tenant's resolved datasets **plus** its
`default_dataset` when that name has no dataset row
(`ResolvedTenant.default_dataset`, `src/common/src/catalog_manager.rs:44-45`).

_Why:_ admin-API tenant creation stores `default_dataset` as a tenant column and
writes no dataset row, so `tenants × datasets` alone would provision nothing for
a freshly created tenant — the headline scenario. Materializing the row instead
was rejected here: it changes admin API write behavior and would start
lifecycle-managing data that currently is not. That gap (such datasets are also
invisible to compaction and retention enumeration) is pre-existing and filed
separately.

## Risks / Trade-offs

- **Materialized labels freeze at provisioning time instead of first-write time.**
  `ensure_table` never evolves an existing table's schema, so a later
  `materialized_labels` change still does not reach existing tables. This change
  makes that bite immediately for every dataset rather than only for ones that
  ingest → Documented limitation; schema evolution is tracked separately.
- **Empty tables warn in the compactor planner.** `group_files_by_partition`
  errors `"Table has no current snapshot"` when `current_snapshot_id` is `None`
  (`src/compactor/src/planner.rs:306-310`), caught per table as a `warn!`
  (`:217-226`) — roughly eight warnings per dataset per cycle in steady state
  → Fixed in this change: no snapshot means zero candidates, not a warning.
- **Empty tables multiply catalog objects.** Up to eight rows and eight
  `metadata.json` files per dataset → Bounded and small (no snapshots, no data
  files). Verified safe for orphan cleanup (data scan is
  `.../data/**.parquet`, `src/compactor/src/orphan/detector.rs:264-313`),
  storage-usage accounting (`src/common/src/storage_usage.rs:221-228`), and
  retention partition drop (`src/compactor/src/retention/enforcer.rs:494-499`).
- **"Repeat passes change nothing" is not literally true on the first pass over
  pre-#895 tables**, which get the pruning-property backfill commit
  (`table_manager.rs:113`) → The idempotency test must be scoped to
  already-backfilled tables or it goes flaky on realistic fixtures.
- **Tightening the metrics paths is a behavior change, not a no-op.** They
  currently swallow _all_ errors as empty (`src/querier/src/query/metrics.rs:1072`,
  `:1231`, `:1438`), which already violates the new "actionable errors are still
  reported" requirement → Called out as its own task so the change is deliberate.
- **A wedged catalog makes every pass log failures** → warn-level with
  tenant/dataset/table fields, never fatal; degrades exactly to today's
  create-on-first-write.
- **Read guard could mask a real regression** (a table that should exist silently
  reads empty) → The guard fires only on genuine absence, and reconciler metrics
  make an unexpectedly absent table visible in logs.

## Migration Plan

No data migration, no on-disk layout change, no wire-format change.

- Deploy order is unconstrained: the read guard (querier) and the reconciler
  (writer) are independent, and either is useful alone.
- First writer start after deploy performs a full pass over existing tenants,
  creating missing tables for datasets with partial coverage. This is additive —
  existing tables are loaded, not recreated — and may commit the #895/#959
  property backfill on tables that still lack it, which is `ensure_table`'s
  intended behavior today.
- The rewired admin endpoint changes observable behavior (it now creates tables)
  while keeping its route and response contract, so no client changes are needed.
- Rollback: revert the writer to stop reconciling; tables already created stay
  and are used by the ingest path as if a write had created them. Reverting the
  querier guard restores the previous error behavior. Neither direction requires
  touching stored data.
