---
audience: operator
type: reference
status: living
sources:
  - src/writer/src/reconcile.rs
  - src/common/src/catalog_manager.rs
  - src/common/src/iceberg/table_manager.rs
  - src/router/src/endpoints/tenant.rs
---

# Signal Table Provisioning

Every tenant/dataset SignalDB knows about holds an Iceberg table for each
signal type enabled for it, so a dataset is complete and queryable from the
moment it exists rather than only after telemetry happens to arrive for each
signal.

The writer converges datasets onto that set continuously. Nothing needs to be
run by hand; this page describes the knob, what a pass does, and how to tell
whether it is working.

## What gets provisioned

Up to eight tables per dataset, gated on the signal types enabled **for that
tenant** — a tenant that carries its own `[schema]` block narrows the set, so
one that disabled metrics gets no `metrics_*` tables:

| Signal   | Tables                                                                                                  | Gate                               |
| -------- | ------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| Traces   | `traces`                                                                                                | `default_schemas.traces_enabled`   |
| Logs     | `logs`                                                                                                  | `default_schemas.logs_enabled`     |
| Metrics  | `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exponential_histogram`, `metrics_summary` | `default_schemas.metrics_enabled`  |
| Profiles | `profiles`                                                                                              | `default_schemas.profiles_enabled` |

Custom tables declared in `default_schemas.custom_schemas` are a config-only
concept and are never provisioned.

Provisioned tables are what the ingest path would have created: the same
schemas, partition specs, bloom-filter and compression settings, and
metadata-pruning properties. A later write lands in them — it does not create a
second table and cannot conflict.

## Configuration

```toml
[writer]
# How often the writer re-runs the reconciler over the tenant registry.
# A pass always runs at startup; this governs the periodic re-run.
# "0s" disables periodic passes (tables then appear on first write).
table_reconcile_interval = "5m"
```

Environment override: `SIGNALDB__WRITER__TABLE_RECONCILE_INTERVAL=90s`.

Set it shorter if you create tenants frequently and want their tables to appear
sooner; longer if your catalog is remote and you want less background traffic.
A converged deployment issues no catalog calls per pass either way (see
[Steady-state cost](#steady-state-cost)), so the interval mostly bounds how
long a _newly created_ dataset waits.

## What a pass does

1. Enumerate the tenant registry — config-defined tenants **and** tenants
   created through the admin API.
2. For each tenant, take its datasets plus its `default_dataset` when that
   name has no separate dataset record. Admin-API tenant creation stores
   `default_dataset` on the tenant row and writes no dataset row, so this
   fallback is the common case for a freshly created tenant.
3. For each dataset, load-or-create every enabled table.

Datasets created while the writer was down, datasets predating this behavior,
and datasets added at runtime all converge on a later pass — no restart, no
configuration-file edit.

### Steady-state cost

The writer remembers, per process, which `(tenant, dataset, table)` triples it
has confirmed, and skips those datasets entirely on later passes. A fully
converged deployment therefore issues no catalog traffic per pass. The memory
is rebuilt on restart, so the startup pass is always a real check.

### Failures

A provisioning failure is never fatal and never blocks ingest or queries. It is
logged at `warn` with the tenant, dataset, and table it concerns, and retried on
the next pass. One table's failure does not abort its siblings, and one
dataset's failure does not abort the rest of the pass.

Because the ingest path independently creates any table it needs, a
persistently failing reconciler degrades to create-on-first-write — the prior
behavior — rather than to data loss.

## Provisioning a tenant on demand

To create a tenant's tables immediately instead of waiting for the next pass:

```bash
curl -X POST http://localhost:3000/api/v1/tenants/acme/tables/create \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: acme"
```

The call provisions the tenant's datasets before returning `201`, and returns
`500` if any table could not be created. Tenant-administrator privileges are
required.

## Verifying

Metrics, exported over OTLP by [self-monitoring](self-monitoring-traces.md) and
attributed by `tenant` and `dataset`:

| Instrument                                    | Meaning                       |
| --------------------------------------------- | ----------------------------- |
| `signaldb.writer.tables_provisioned`          | Tables the reconciler created |
| `signaldb.writer.table_provisioning_failures` | Tables it could not create    |

A rising failure count means the deployment has degraded to
create-on-first-write; check the writer's logs for the tenant, dataset, and
table, and confirm the Iceberg catalog is reachable.

Logs: a pass that created or failed anything logs at `info` with
`datasets_checked`, `datasets_skipped`, `tables_created`, and `tables_failed`.
A converged pass logs at `debug`.

## Consequences to expect

- **Empty tables multiply catalog objects.** Each provisioned dataset gains up
  to eight catalog rows and eight `metadata.json` files. They carry no
  snapshots and no data files, so retention, orphan cleanup, storage
  accounting, and the compactor all treat them as no-ops.
- **Materialized labels are fixed at provisioning time.** `[schema]
materialized_labels` is applied when a table is created and never evolves an
  existing table's schema. Provisioning means that now happens for every
  dataset, not only for ones that ingest — so change `materialized_labels`
  before a dataset is provisioned, not after.

## Related

- [Compactor configuration](compactor/configuration.md) — retention and
  lifecycle for the data these tables hold
- [Authentication](../users/authentication.md) — the tenant self-service API
