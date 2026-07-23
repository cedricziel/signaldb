---
audience: user
type: tutorial
status: living
sources:
  - src/signaldb-cli/src/commands/query.rs
  - src/querier/src/flight.rs
---

# Query SignalDB with SQL

In this tutorial you run SQL queries against your stored traces, logs, and
metrics using the `signaldb-cli` command-line tool. Queries travel over
Arrow Flight to the router (port 50053 by default), which forwards them to
a querier where DataFusion executes them against the Iceberg tables.

## Prerequisites

- A running SignalDB deployment (`./scripts/run-dev.sh` is enough locally).
- The `signaldb-cli` binary (`cargo build -p signaldb-cli` in the repo).
- An API key and tenant ID, and some ingested data — see
  [Sending OTLP data](sending-otlp.md) and
  [Authentication](authentication.md).

## 1. Run your first query

```bash
signaldb-cli query sql "SELECT trace_id, span_name, service_name FROM traces LIMIT 10" \
  --api-key sk-acme-prod-key-123 \
  --tenant-id acme
```

You get a pretty-printed table and a `10 row(s) returned.` summary on
stderr. All flags can also come from the environment:
`SIGNALDB_FLIGHT_URL` (default `http://localhost:50053`),
`SIGNALDB_API_KEY`, `SIGNALDB_TENANT_ID`, `SIGNALDB_DATASET_ID`.

## 2. Understand table naming

Your data is organized as `catalog.schema.table`:

- **catalog** = your tenant slug
- **schema** = your dataset slug
- **tables** = `traces`, `logs`, `metrics_gauge`, `metrics_sum`,
  `metrics_histogram`

When you authenticate, the session's default catalog and schema are pinned
to your tenant and dataset, so unqualified names work:

```sql
SELECT count(*) FROM traces
```

Fully qualified names work too (quote slugs that contain hyphens):

```sql
SELECT count(*) FROM "acme"."production"."traces"
```

Authenticated sessions default to your own tenant's catalog and dataset
schema, and trace-lookup and trace-search requests that name another
tenant are rejected. Always qualify (or leave unqualified) names within
your own tenant's catalog.

## 3. Try some useful queries

Services that reported spans:

```bash
signaldb-cli query sql "SELECT DISTINCT service_name FROM traces"
```

Slowest spans:

```bash
signaldb-cli query sql "SELECT trace_id, span_name, duration_nano FROM traces ORDER BY duration_nano DESC LIMIT 20"
```

Recent log records:

```bash
signaldb-cli query sql "SELECT * FROM logs LIMIT 20"
```

## 4. Change the output format

`--format` selects `table` (default), `json` (newline-delimited, one
object per row, pipe-friendly), or `csv` (with header row):

```bash
signaldb-cli query sql "SELECT DISTINCT service_name FROM traces" --format json | jq -r .service_name
```

## Limits to know

- **Row cap**: the querier truncates raw SQL results at the server-side
  `max_sql_rows` limit (default 1,000,000). Use `LIMIT`/aggregation rather
  than relying on unbounded selects.
- **Concurrency**: operators can cap concurrent queries per tenant; excess
  queries fail with a resource-exhausted error.

## Where to go next

- `signaldb-cli tui` starts an interactive terminal UI over the same
  endpoints.
- The Tempo-compatible HTTP API serves Grafana trace views — see the
  [Tempo API reference](tempo-api-reference.md).
- [Grafana datasource options](grafana-datasource.md) for dashboards.
