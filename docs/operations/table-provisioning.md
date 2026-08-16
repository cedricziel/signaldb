---
audience: operator
type: reference
status: living
sources:
  - src/writer/src/reconcile.rs
  - src/common/src/catalog_manager.rs
  - src/common/src/iceberg/table_manager.rs
  - src/common/src/schema/mod.rs
  - src/common/src/tenant_api.rs
  - src/router/src/endpoints/tenant.rs
---

# Signal Table Provisioning

Every tenant/dataset SignalDB knows about converges on an Iceberg table for
each signal type enabled for it, so a dataset becomes complete without waiting
for telemetry to arrive for each signal.

Convergence is eventual, not instantaneous: a dataset created while the writer
is running is provisioned by the next reconcile pass (within
`table_reconcile_interval`), by an on-demand request, or by its first write,
whichever comes first. Queries do not wait for any of that — a signal with no
table yet reads as an empty result, never an error.

The writer runs this continuously; nothing needs to be run by hand. This page
describes the knob, what a pass does, and how to tell whether it is working.

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
# A pass always runs at startup; this governs only the periodic re-run.
# "0s" keeps the startup pass and disables the periodic one, so datasets
# created after startup wait for a first write, an on-demand request, or a
# writer restart.
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
2. For each tenant, take its datasets. The registry guarantees a tenant's
   `default_dataset` is among them even when no dataset row names it, so a
   tenant whose default exists only as a column on its tenant row is
   provisioned like any other.
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

To create a tenant's tables immediately instead of waiting for the next pass,
and to list what is already provisioned, `POST /api/v1/tenants/{id}/tables/create`
and `GET /api/v1/tenants/{id}/tables` are in the OpenAPI contract, so every
client surface reaches them through the generated SDK — raw HTTP, the CLI,
an MCP tool, and the UI's management area:

```bash
# raw HTTP
curl -X POST http://localhost:3000/api/v1/tenants/acme/tables/create \
  -H "Authorization: Bearer $SIGNALDB_API_KEY" \
  -H "X-Tenant-ID: acme"

# CLI (signaldb-cli tenant table)
signaldb-cli tenant table list --api-key "$SIGNALDB_API_KEY" --tenant-id acme
signaldb-cli tenant table provision --api-key "$SIGNALDB_API_KEY" --tenant-id acme
```

Through an MCP agent session: the `tenant_list_tables` and
`tenant_create_tables` tools (see [the MCP server doc](../users/mcp.md)).
Through the web UI: the management area's **Tables** section for a
dataset, with a **Provision tables** action.

The call provisions the tenant's datasets before returning `201`, and returns
`500` if any table could not be created. Tenant-administrator privileges are
required — in practice any valid tenant API key, since a tenant-scoped key is
already trusted to shape its own tenant's infrastructure (see
`TenantContext::can_manage_tenant`); this is a lower bar than the
[management API](../users/authentication.md)'s dataset/API-key/membership
endpoints, which require a human-authenticated session.

`GET /api/v1/tenants/{id}/tables` (`tenant table list` / `tenant_list_tables`)
lists what is really in the Iceberg catalog — the same place provisioning
writes to — grouped by dataset. Each table carries the dataset it belongs to
(`dataset` on every entry); the response also groups them under `datasets`
(one entry per dataset, each with its own `tables`), while the flat `tables`
list is kept for existing consumers. A dataset with nothing provisioned yet
lists as empty, not as an error, and a listing right after
`POST …/tables/create` reflects exactly what was just created.

```bash
$ signaldb-cli tenant table list --api-key "$SIGNALDB_API_KEY" --tenant-id acme
DATASET     TABLE    TYPE
production  logs     logs
production  traces   traces
```

Pass `--json` for the raw response instead of the formatted table.

## Verifying

Metrics, exported over OTLP by [self-monitoring](self-monitoring-traces.md) and
attributed by `signaldb.tenant.id` and `signaldb.dataset.id`:

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
- **Materialized labels are fixed at creation time.** `[schema]
materialized_labels` is applied when a table is created; `ensure_table`'s
  own catch-up path (below) only brings a **traces or logs** table's
  `schemas.toml`-declared columns forward, it does not retrofit a changed
  label configuration onto an existing table. Provisioning means the initial
  labels-at-creation behavior now happens for every dataset, not only for
  ones that ingest, so prefer setting `materialized_labels` before a dataset
  is provisioned. Existing tables are not stuck: the compactor's
  attribute-promotion pass can add `label_<key>` columns to them — see
  [label columns can be added to existing tables](../architecture/storage-layout.md#label-columns-can-be-added-to-existing-tables).
- **`ensure_table` does evolve an existing traces or logs table's schema**
  (not metrics/profiles yet — those are hand-written, not `schemas.toml`-sourced).
  Every load, not just creation, brings the table's schema forward to the
  current `schemas.toml` version if it's behind, additively — new nullable
  columns only, never a rewrite of existing data. See
  [schema evolution](../architecture/storage-layout.md#an-existing-tables-schema-tracks-and-catches-up-to-schematomls-version).
- **Not every table property is set at creation.** Provisioning applies the
  bloom-filter, column-statistics, compression and metadata-pruning
  properties, but deliberately not `write.target-file-size-bytes`: compaction
  reconciles that one against `[compactor].target_file_size_mb` before each
  rewrite, so it cannot drift when an operator retunes the target. A
  provisioned table therefore shows no such property until its first
  compaction — see
  [output file size](../architecture/storage-layout.md#output-file-size).

## Related

- [Compactor configuration](compactor/configuration.md) — retention and
  lifecycle for the data these tables hold
- [Authentication](../users/authentication.md) — the tenant self-service API
