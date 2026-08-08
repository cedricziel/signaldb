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

`POST /tenants/{tenant_id}/tables/create` already exists for exactly this job and
does not do it: it returns `201 {"message": "Default tables created"}` while
creating nothing.

## What Changes

- Add an idempotent **table reconciler** that ensures every registered
  tenant/dataset has the full set of signal tables enabled **for that tenant**:
  `traces`, `logs`, the five `metrics_*` tables, and `profiles`, resolved through
  the tenant's schema config rather than the global default, so a per-tenant
  override that disables a signal is honored.
- Run the reconciler in the writer: a startup pass over the tenant registry plus
  a periodic re-run, so tenants created while the writer was down, tenants
  predating this change, and datasets added at runtime all converge without a
  restart. This requires attaching a tenant source to the writer's
  `CatalogManager`, which it does not have today — without it the reconciler
  would see config-defined tenants only, i.e. exactly not the ones this change
  is for.
- Provision a tenant's **default dataset even when no dataset row exists**, which
  is the state admin-API tenant creation leaves behind today.
- Add a **read-path guard** across all four signal families — traces included —
  so a missing signal table on a metadata/label/search query returns an empty
  result instead of an `Internal` error. Required independently of provisioning:
  a disabled signal type legitimately has no table forever, and a query can
  always race a not-yet-reconciled dataset.
- Make the querier's Flight error mapping name the signal it came from. The
  shared mapper hardcodes `"Profile query failed"` for every signal.
- **Rewire** `POST /tenants/{tenant_id}/tables/create` to the real reconciler so
  the endpoint stops lying. Its route, request shape, and success response are
  unchanged; only its effect becomes real.
- Fix the compactor planner to treat a snapshot-less table as zero compaction
  candidates, so newly provisioned empty tables do not warn on every cycle.
- New configuration key controlling the reconcile interval, with a documented
  default and an opt-out.

Not breaking: no OTLP ingest, Tempo/LogQL/PromQL surface, Flight wire schema, or
on-disk Iceberg/WAL layout changes. Reconciliation creates tables that the write
path would have created anyway, with the same schemas, partition specs, and
properties. The admin endpoint keeps its contract and gains its intended effect.

## Capabilities

### New Capabilities

- `dataset-table-provisioning`: every registered tenant/dataset converges on the
  set of signal tables enabled for its tenant without waiting for a first write —
  idempotent, restart-safe, and driven by the tenant registry rather than by
  ingest traffic.

### Modified Capabilities

- `tenant-catalog-registry`: strengthens "New tenants are usable the moment they
  are created" — a registered dataset that has never been written to SHALL
  answer metadata, label, and search queries with empty results rather than an
  error, closing the gap between "catalog resolves" and "queries succeed".

## Impact

- **common**: `CatalogManager` gains a per-dataset table-reconcile entry point
  built on the existing idempotent `ensure_table`, resolving the enabled set
  per tenant and skipping `TableSchema::Custom` (which `ensure_table` rejects by
  name); `SchemaRegistry::create_default_tables_for_tenant` and
  `TenantApi::create_default_tables` are reimplemented on top of it instead of
  logging `"Would create table …"`.
- **writer**: owns the reconcile loop; its `CatalogManager` gains the tenant
  source every other service already attaches. The loop is exposed as a start
  method on the ingest service so the standalone binary and `signaldb-bin` share
  one wiring rather than duplicating it. Failures logged, never fatal. New
  `[writer]` config key for the interval.
- **querier**: missing-table guards on the traces, logs, metrics, profiles, and
  Query-IR read paths, keyed on the async table lookup that already reports
  absence as `Ok(None)`; per-signal error wrappers in the Flight status mapping.
  The metrics paths currently swallow _all_ errors as empty and are tightened to
  distinguish absence from failure.
- **router**: `POST /tenants/{tenant_id}/tables/create` delegates to the
  reconciler; route and response contract unchanged.
- **compactor**: planner treats a table with no current snapshot as zero
  candidates instead of warning per table per cycle.
- **tests-integration** and `querier/tests/lazy_tenant_registration.rs`: the
  existing KNOWN-ISSUE pin on #972 flips to asserting empty results.
- **docs**: configuration reference gains the interval key;
  `docs/users/authentication.md` and the `multi-tenancy` skill are corrected
  where they describe the table-create endpoint.
- No new dependencies. Operational note: each reconciled dataset gains up to
  eight empty Iceberg tables (one catalog row and one `metadata.json` each),
  which the compactor's `list_signal_tables` enumeration handles as no-ops once
  the planner fix above is in.
