## Why

A tenant or dataset created through the admin API gets no Iceberg tables: tables
are only created as a side effect of the first write. Until telemetry lands, the
dataset is registered and resolvable but has no `logs`/`traces`/`metrics_*`/
`profiles` tables, so metadata and label queries fail with
`Internal: "No table named 'logs'"` instead of returning an empty result
(issue #972). This contradicts the `tenant-catalog-registry` promise that a
newly created tenant is usable — including queryable — the moment it exists,
and it makes a brand-new tenant look broken to Grafana and the Explore UI before
its first data point arrives.

## What Changes

- Add an idempotent **table reconciler** that ensures every registered
  tenant/dataset has the full set of enabled signal tables, driven by the
  `[schema.default_schemas]` flags: `traces`, `logs`, the five `metrics_*`
  tables, and `profiles`.
- Run the reconciler in the writer: a startup pass over the tenant registry plus
  a periodic re-run, so tenants created while the writer was down, tenants
  predating this change, and datasets added at runtime all converge without a
  restart.
- Add a **read-path guard**: a missing signal table on a metadata/label/search
  query returns an empty result instead of an `Internal` error. This is required
  independently of provisioning, because a disabled signal type legitimately has
  no table forever, and because a query can always race a not-yet-reconciled
  dataset.
- Fix the mislabeled `"Profile query failed"` wrapper that the logs label path
  returns in the querier's Flight status mapping.
- Remove the dead placeholder `SchemaRegistry::create_default_tables_for_tenant`
  (logs `"Would create table …"` and returns `Ok(())`) and its
  `TenantApi::create_default_tables` wrapper, superseded by the real reconciler.
- New configuration key controlling the reconcile interval, with a documented
  default and an opt-out.

Not breaking: no OTLP ingest, Tempo/LogQL/PromQL surface, Flight wire schema, or
on-disk Iceberg/WAL layout changes. Reconciliation creates tables that the write
path would have created anyway, with the same schemas, partition specs, and
properties.

## Capabilities

### New Capabilities

- `dataset-table-provisioning`: every registered tenant/dataset converges on the
  full set of enabled signal tables without waiting for a first write —
  idempotent, restart-safe, and driven by the tenant registry rather than by
  ingest traffic.

### Modified Capabilities

- `tenant-catalog-registry`: strengthens "New tenants are usable the moment they
  are created" — a registered dataset that has never been written to SHALL
  answer metadata, label, and search queries with empty results rather than an
  error, closing the gap between "catalog resolves" and "queries succeed".

## Impact

- **common**: `CatalogManager` gains a per-dataset table-reconcile entry point
  built on the existing idempotent `ensure_table`; `schema::SchemaRegistry` and
  `tenant_api` lose the dead table-creation stubs; new config key under
  `[schema]`.
- **writer**: owns the reconcile loop (it already holds the `CatalogManager` and
  is the only service that creates these tables today); startup pass plus
  periodic re-run, failures logged and never fatal.
- **querier**: empty/missing-table guards on the logs, metrics, and profiles
  metadata paths (the trace path already has one); corrected error wrapper in
  the Flight status mapping.
- **tests-integration** and `querier/tests/lazy_tenant_registration.rs`: the
  existing KNOWN-ISSUE pin on #972 flips to asserting empty results.
- **docs**: configuration reference gains the reconcile-interval key.
- No new dependencies. Operational note: each reconciled dataset gains up to
  eight empty Iceberg tables (one catalog row and one `metadata.json` each),
  which the compactor's existing `list_signal_tables` enumeration handles as
  cheap no-ops.
