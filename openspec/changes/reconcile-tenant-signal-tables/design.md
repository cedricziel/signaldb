## Context

See proposal.md — Why. The state that shapes the approach:

- Signal tables are created exactly one way today: `IcebergTableWriter::new`
  calls `CatalogManager::ensure_table` on the write path
  (`src/writer/src/storage/iceberg.rs:78`). Tenant and dataset creation through
  the admin API never touches the Iceberg catalog.
- `IcebergTableManager::ensure_table` (`src/common/src/iceberg/table_manager.rs`)
  is already load-or-create and idempotent: one `load_tabular` round-trip, create
  on NotFound, tolerate `AlreadyExists` from a concurrent caller. It also
  backfills the #895/#959 metadata-pruning properties on pre-existing tables,
  committing at most once per table.
- The enabled table set is already derivable:
  `TableSchema::all_from_config(&config.schema.default_schemas)` returns traces,
  logs, the five `metrics_*` tables, and profiles, gated by per-signal flags.
- The querier's `TenantCatalog` (`src/querier/src/flight.rs`) wraps the live
  Iceberg catalog rather than a snapshot of it, so a table created by another
  process becomes visible to a running querier with no re-registration.
- A dead placeholder already occupies this slot:
  `SchemaRegistry::create_default_tables_for_tenant`
  (`src/common/src/schema/mod.rs:394`) logs `"Would create table …"` and returns
  `Ok(())`, reachable only through `TenantApi::create_default_tables`.
- FDAP constraint: schemas and partition specs come from
  `common::iceberg::schemas`, which builds on the Arrow/Parquet types re-exported
  through DataFusion. Reconciliation reuses those constructors unchanged — it
  must not grow a second definition of a table's schema.
- The v1 wire → v2 storage transform in the writer
  (`apply_schema_transformation_if_needed`) is unaffected: reconciliation creates
  storage-format (v2) tables, exactly what the transform already targets.

## Goals / Non-Goals

**Goals:**

- One reconcile entry point, built on `ensure_table`, that is safe to call
  repeatedly and concurrently with ingest.
- Convergence for datasets that already exist, including ones created before this
  change, without operator action.
- Steady-state cost near zero: a converged deployment should issue no catalog
  traffic for reconciliation.
- Read paths that treat a missing table as "no data" independently of whether
  reconciliation has run.

**Non-Goals:**

- Schema evolution of existing tables. `ensure_table` backfills properties, never
  columns; a materialized-label config change still does not reach tables that
  already exist. Unchanged by this design (see Risks).
- Dropping tables for signal types an operator later disables. Reconciliation is
  create-only.
- Creating tables for tenants that are not in the registry, or pre-creating
  datasets that do not exist yet.
- Synchronous provisioning inside the admin API request path (see Decision 3).

## Decisions

### Decision 1: Reconcile per dataset via `CatalogManager`, reusing `ensure_table`

Add `CatalogManager::ensure_dataset_tables(tenant_id, dataset_id)` that iterates
`TableSchema::all_from_config` and calls the existing `ensure_table` per table,
returning a small report (created / already-present / failed counts) for logging
and metrics.

_Why:_ `CatalogManager` already owns the catalog handle, the slug resolution, and
the per-tenant materialized-label lookup that `ensure_table` needs. Building on
`ensure_table` guarantees provisioned tables are byte-for-byte what the write
path would have produced — same schema, partition spec, bloom-filter and
compression properties, metadata-pruning properties — which is the spec's
"indistinguishable from the ingest path's table" requirement.

_Alternative rejected:_ a standalone provisioning module with its own
`CreateTableBuilder`. It would duplicate the property/schema assembly that
`ensure_table` performs and drift from it the first time either side changes.

### Decision 2: The writer owns the loop; startup pass plus periodic re-run

The writer runs a reconcile pass at startup over `list_active_tenants()` ×
datasets, then re-runs on an interval from a new `[schema]` config key
(default 5m; zero disables). Failures are logged with tenant/dataset/table and
retried next pass; nothing fails startup.

_Why the writer:_ it already constructs the `CatalogManager`
(`src/writer/src/main.rs:138`), it is the only service that creates these tables
today, and it runs continuously. Monolithic mode gets it for free.

_Alternative rejected — the compactor:_ it has the right shape
(`active_tenants_or_empty` × datasets in `lifecycle.rs`) but operators disable
compaction, and provisioning must not be collateral damage of that choice.

_Alternative rejected — the router at creation time:_ the router has no
`CatalogManager` wired into its state at all, so this means new plumbing, and it
still would not cover pre-existing or previously-failed datasets. Covered by
Decision 3.

_Alternative rejected — the querier on lazy registration:_ it would tie table
creation to read traffic, giving a read path a write side effect, and would leave
never-queried datasets unprovisioned.

### Decision 3: Convergence only — no synchronous hook in the admin API

Table creation is not part of the `POST /api/v1/admin/tenants` transaction. A
newly created tenant's tables appear within one reconcile interval.

_Why:_ the read-path guard (Decision 4) makes the interim window harmless — the
tenant answers queries with empty results either way — so a synchronous hook buys
only cosmetic immediacy at the cost of coupling tenant creation to catalog
availability and making the admin API fail on catalog trouble. If the create →
first-query gap ever matters, a best-effort call can be added later without
changing this design; the reconciler remains the correctness mechanism.

### Decision 4: Read paths guard on table existence, not on error strings

In the querier, table access on metadata/label/search paths goes through a helper
that checks existence (`SessionContext::table_exist` against the resolved table
reference) and returns `None` when absent, with callers yielding an empty result.
Data-plane errors from a table that does exist keep propagating unchanged.

_Why existence rather than matching `"No table named"`:_ string matching on
DataFusion error text is brittle across DataFusion upgrades and would swallow
genuine planning failures that happen to mention a table. An explicit existence
check keeps "absent" and "broken" distinguishable, which the spec requires.

Separately, the Flight status mapping in `src/querier/src/flight.rs` labels logs
label-query failures `"Profile query failed"`; each signal's error path gets its
own wrapper so a failure names the signal it came from.

### Decision 5: In-memory "already ensured" set to keep steady state free

The reconciler keeps a process-local set of `(tenant, dataset, table)` triples it
has confirmed. A converged deployment therefore issues no catalog calls per pass;
only newly seen datasets are checked. The set is rebuilt on restart, so the
startup pass is always a real check.

_Why:_ without it, each pass costs eight `load_tabular` round-trips per dataset
against the SQL catalog — negligible for a handful of datasets, wasteful at
hundreds, and pure overhead once converged. Cache staleness is benign: the worst
case is skipping a table that was deleted out-of-band, which the write path
recreates and a restart re-checks.

## Risks / Trade-offs

- **Materialized labels freeze at provisioning time instead of first-write time.**
  `ensure_table` never evolves an existing table's schema, so a later
  `materialized_labels` config change still does not reach existing tables. This
  change makes that bite immediately for every dataset rather than only for ones
  that ingest → Documented as a known limitation; schema evolution is tracked
  separately and unchanged in scope here.
- **Empty tables multiply catalog objects.** Up to eight rows and eight
  `metadata.json` files per dataset, including for signals a tenant never sends
  → Bounded and small (one metadata file each, no snapshots, no data files); the
  compactor's `list_signal_tables` enumeration already skips empty tables as
  cheap no-ops. Operators who do not want tables for a signal disable it in
  `[schema.default_schemas]`, which reconciliation honors.
- **Reconcile races a concurrent first write for the same table.**
  `ensure_table` already handles `AlreadyExists` and the SQLite unique-constraint
  variant → No new risk; covered by an explicit concurrency test.
- **A wedged catalog makes every pass log failures.** → Failures are warn-level
  with tenant/dataset/table fields and never fatal; behavior degrades exactly to
  today's create-on-first-write.
- **Read guard could mask a real regression** (a table that should exist silently
  reads empty) → The guard fires only on genuine absence, and the reconciler plus
  its metrics make an unexpectedly absent table visible in logs rather than only
  through query results.

## Migration Plan

No data migration, no on-disk layout change, no wire-format change.

- Deploy order is unconstrained: the read guard (querier) and the reconciler
  (writer) are independent, and either is useful alone.
- First writer start after deploy performs a full pass over existing tenants,
  creating missing tables for datasets that had partial coverage. This is
  additive — existing tables are loaded, not recreated — and may commit the
  #895/#959 property backfill on tables that still lack it, which is the intended
  behavior of `ensure_table` today.
- Rollback: revert the writer to stop reconciling; tables already created stay
  and continue to be used by the ingest path exactly as if a write had created
  them. Reverting the querier guard restores the previous error behavior. Neither
  direction requires touching stored data.
