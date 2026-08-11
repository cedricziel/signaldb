---
audience: operator
type: reference
status: living
sources:
  - src/common/src/self_monitoring/**
  - otel/registry/**
---

# Self-Monitoring Trace Model

The spans SignalDB emits about its own operation follow the OpenTelemetry
semantic conventions, pinned at **semconv v1.43.0** (the `schema_url` on
every exported resource and instrumentation scope). This page is the
operator-facing reference: what spans exist, what they're named, and what
changed if you had dashboards on the old names.

This page covers spans only. Counter/histogram/gauge instruments (e.g.
`signaldb.wal.entries_written`, `signaldb.wal.corrupt_entries`) are defined
in `src/common/src/self_monitoring/app_metrics.rs`; WAL-specific ones are
documented alongside their recovery behavior in
[WAL Persistence](wal-persistence.md#monitoring-and-alerting).

## Resource identity

Every service exports with:

| Attribute                     | Value                                                                                                                                  |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `service.namespace`           | `signaldb`                                                                                                                             |
| `service.name`                | `signaldb-acceptor`, `signaldb-router`, `signaldb-writer`, `signaldb-querier`, `signaldb-compactor` (or `signaldb` in monolithic mode) |
| `service.version`             | crate version                                                                                                                          |
| `service.instance.id`         | per-process UUID                                                                                                                       |
| `deployment.environment.name` | `[self_monitoring] environment` config key (default `production`)                                                                      |

The deprecated `deployment.environment` attribute is no longer emitted.

## Span model

```mermaid
flowchart LR
    C[Client trace] -->|traceparent| A["SERVER<br/>POST /v1/traces"]
    A --> W1["CLIENT<br/>…FlightService/DoPut"]
    W1 --> W2["SERVER<br/>…FlightService/DoPut"]
    W2 -.->|span link| B["INTERNAL batch<br/>(writer WAL processor)"]
    C2[Query client] -->|traceparent| R["SERVER<br/>GET /api/traces/{id}"]
    R --> Q1["CLIENT<br/>…FlightService/DoGet find_trace"]
    Q1 --> Q2["SERVER<br/>…FlightService/DoGet find_trace"]
    Q2 --> P["INTERNAL<br/>signaldb.query.plan / execute"]
```

- **HTTP boundaries** (router APIs, acceptor OTLP/HTTP + remote-write,
  health): SERVER spans named `{method} {route}` with the stable `http.*`
  attributes. 4xx leaves span status unset (caller fault); 5xx sets Error +
  `error.type`.
- **gRPC/Flight boundaries**: SERVER and CLIENT spans named by the
  fully-qualified method plus a low-cardinality detail
  (`arrow.flight.protocol.FlightService/DoGet query_ir`,
  `…/DoAction compact_dry_run`, OTLP
  `opentelemetry.proto.collector.trace.v1.TraceService/Export`), carrying
  `rpc.system.name=grpc`, `rpc.method`, string `rpc.response.status_code`.
  Server spans fail only on server-fault codes; client spans on any non-OK.
  CLIENT spans also carry `server.address`/`server.port` (the resolved
  target, from service discovery); SERVER spans carry
  `network.peer.address`/`network.peer.port` (the connecting socket, from
  `tonic::Request::remote_addr()`) when available.
- **SQL catalog**: CLIENT spans `{verb} signaldb-catalog` with
  `db.system.name` / `db.operation.name` / `db.namespace`.
- **Query stages**: `signaldb.query.plan` / `signaldb.query.execute`
  INTERNAL spans with `signaldb.query.rows`/`batches`; recorded query text
  is always literal-sanitized (`… WHERE name = ?`).
- **Background jobs**: `compaction`, `retention_enforcement`,
  `orphan_cleanup` root INTERNAL spans with `signaldb.tenant.id` /
  `signaldb.dataset.id` / `signaldb.table` and affected-object counts
  (`signaldb.job.partitions_dropped`, `…snapshots_expired`,
  `…files_deleted`, `…bytes_reclaimed`).

  Orphan cleanup carries enough to reconstruct a pass without reading the
  source: why it declined a table (`signaldb.job.skip_reason`,
  `…estimated_live_files` against `…live_files_threshold`), what it saw
  (`…live_files`, `…total_files`, `…scanned_metadata_files`,
  `…candidates`, `…grace_period_hours`), and what it did
  (`…files_deleted`, `…bytes_reclaimed`, `…deletion_failures`,
  `…dry_run`). Counts are emitted as `i64` — the registry types them as
  `int`, and `usize`/`u64` would otherwise bridge to OpenTelemetry as
  strings and break numeric queries. Per-file deletion detail stays in
  plain logs: a path per file is unbounded cardinality and does not belong
  in span attributes.

- **WAL fan-in**: the writer's batch span **links** to every distinct
  source ingest trace (one link per origin, never a parent).

Trace continuity: a caller-supplied W3C `traceparent` is honored at every
boundary, and the sampler is parent-based by default (an unrecognized
`OTEL_TRACES_SAMPLER` value also falls back to parent-based).

The HTTP middleware also returns the server span's context to the caller on
every response (`Server-Timing: traceparent;desc="..."` + `traceresponse`,
plus `dur` stage timings), with the trace flags reflecting the sampling
decision. Headers are omitted when self-monitoring is disabled and on
`_system` tenant requests. Caller-facing reference:
[Trace Context on HTTP Responses](../users/response-trace-context.md).

## Renames (breaking for dashboards)

| Old                                                       | New                                                   |
| --------------------------------------------------------- | ----------------------------------------------------- |
| span `flight_do_get`                                      | `arrow.flight.protocol.FlightService/DoGet <verb>`    |
| span `flight_do_put`                                      | `arrow.flight.protocol.FlightService/DoPut`           |
| span `compaction_job`                                     | `compaction`                                          |
| field `tenant_id`                                         | `signaldb.tenant.id`                                  |
| field `dataset_id`                                        | `signaldb.dataset.id`                                 |
| field `table` / `table_name`                              | `signaldb.table`                                      |
| field `entry_count`                                       | `signaldb.wal.entry_count`                            |
| field `operation` / `data_size` / `entry_id` (WAL spans)  | `signaldb.wal.operation` / `…data_size` / `…entry_id` |
| resource `deployment.environment` (= `"self-monitoring"`) | `deployment.environment.name` (config-sourced)        |

## Conventions registry and enforcement

The `signaldb.*` attributes are declared in `otel/registry/` (an OTel
Weaver registry layered on upstream semconv v1.43.0). CI validates the
registry (`weaver registry check`), pins code↔registry drift
(`registry_pins` test), enforces span-construction rules (no bare
`#[tracing::instrument]`; `otel.kind` only in the span factories), and an
advisory **Weaver Live Check** workflow boots the monolithic binary
against a `weaver registry live-check` listener and reports findings plus
`registry_coverage` per PR (hardening to a blocking check is tracked in
#912; further follow-ups: #913, #914, #915, #916).

Spans carry only registry-declared attributes. The tracing→OTel bridge's
convenience attributes (`busy_ns`, `idle_ns`, `target`, `code.*`) are
disabled at construction (`common::self_monitoring::otel_span_layer`,
pinned by the `otel_bridge_attrs` test) — don't build dashboards on them.

Three bridge-emitted attribute families cannot be disabled and are instead
whitelisted for live-check via finding filters in the repo-root
`.weaver.toml`: `thread.id`/`thread.name` on spans (upstream semconv at
`development` stability — kept deliberately), `level`/`target` on span
events (stamped unconditionally by tracing-opentelemetry's event bridge),
and the `not_stable` advice for our own `signaldb.*` attributes (the
SignalDB registry is `development` by design). `info!`/`warn!` events
inside instrumented spans become span events, so their fields must be
declared in the resolved registry — `signaldb.*` for SignalDB-specific
fields, or an upstream semconv attribute (e.g. `file.path`) where one
fits. Per-item developer detail belongs at `debug!`, which the default
`info` level keeps out of telemetry.
