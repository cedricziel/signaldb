---
name: tempo-api
description: SignalDB Tempo API compatibility - implemented/stub endpoints, query flow, admin API, Grafana native plugin, and built-in Tempo datasource support. Use when working with HTTP API, Grafana integration, or query endpoints.
user-invocable: false
---

# SignalDB Tempo API Compatibility

Read `docs/users/tempo-api-reference.md` for the endpoint list, status
(implemented/partial/501), tag-value time-window semantics, span-field
extras, error mapping, and the standalone Tempo gRPC querier protocol
(`tempopb.Querier` on the Flight port). Read `docs/users/grafana-datasource.md`
for wiring Grafana's built-in Tempo/Loki datasources and building/installing
the native SignalDB plugin (pnpm, `pnpm run build:backend`) — including its
current limitation (fixed Flight tickets, router answers with empty
placeholder results).

Admin API and tenant self-service API endpoints are the `multi-tenancy`
skill's domain. Flight ticket grammar (`find_trace:...`, `search_traces:...`)
is documented in `docs/architecture/flight-communication.md`.

SignalDB's native, non-dialect query surface is the Query IR
(`POST /api/v1/query`, `docs/users/querying-ir.md`) — Tempo/LogQL/Prometheus
are compatibility dialects that sit alongside it, not superseded by it.

## Gotcha not in the docs above

Tag-value lookups (`distinct_values_sql` in `endpoints/tempo.rs`) apply the
`start`/`end` window twice: once as a precise `start_time_unix_nano` row
bound, and again as an `Hour(timestamp)` bound on the partition column —
mirroring the querier's trace-lookup path so Iceberg partitions actually
prune instead of every Parquet file being scanned per tag dropdown.
