## Why

SignalDB already emits proper OTel database CLIENT spans (`db.system.name`,
`db.operation.name`, `db.namespace`) for its SQL catalog boundary, but its
actual query engine — DataFusion, which serves every SQL, PromQL, LogQL,
TraceQL, and query-IR request in the querier — has no `db.*` spans at all.
Two of the five DataFusion execution paths carry informal, kind-less
`signaldb.query.plan`/`signaldb.query.execute` spans; the other three
(PromQL, LogQL, TraceQL) have no spans around query execution whatsoever.
This means operators cannot use SignalDB's own self-monitoring pipeline to
answer the basic "how is my database performing" question — slow-query
identification, per-query-type latency, or query text for debugging — for
the component that does the actual data access. This change closes that
gap by extending the existing DB CLIENT span pattern from the catalog
boundary to DataFusion query execution, consistently across all five
execution paths.

## What Changes

- Add a DataFusion query CLIENT span factory in
  `common::self_monitoring::spans`, following the same shape as the
  existing `db_client_span` catalog factory: `otel.kind="client"`,
  `db.system.name` (custom value identifying DataFusion), `db.operation.name`
  (one fixed literal per query surface — never parsed from the submitted
  query text), `db.namespace` (tenant/dataset), and `db.query.text`
  (sanitized per-surface — reusing the existing `sanitize_query_text`
  helper for SQL, new equivalents for PromQL/LogQL/TraceQL, omitted where
  safe sanitization isn't available).
- Wire this span into all five DataFusion execution call sites in the
  querier: raw SQL (`execute_query`), query-IR (`IrService::query`),
  PromQL (`MetricsService::query_metric`), LogQL (`LogsService::query_logs`),
  and TraceQL (`trace.rs` find/search) — the latter three currently have no
  query-execution span coverage at all.
- Keep the existing `signaldb.query.plan`/`signaldb.query.execute`
  two-stage timing spans, renested as INTERNAL children under the new
  CLIENT span rather than replaced, preserving today's plan-vs-execute
  latency breakdown.
- Extend `otel/registry/signaldb.yaml` with the new span/attribute
  definitions and add corresponding weaver live-check coverage.
- Tag the existing `signaldb.query.duration`, `signaldb.query.errors`, and
  `signaldb.query.rows_returned` metrics with a fixed low-cardinality
  subset of the new span's attributes — `db.system.name` and
  `db.operation.name` only, never `db.namespace` or `db.query.text` — so
  traces and metrics correlate on the query type without growing metric
  cardinality by tenant count or copying free text into a metric label.
- Add pin/regression tests for the new factory (semconv field names,
  literal attribute values) following the existing
  `db_catalog_span_semconv.rs` pattern.
- Out of scope (explicitly deferred): per-operator instrumentation via
  DataFusion's `ExecutionPlan::metrics()` (spill counts, per-node row
  counts). This change only adds one CLIENT span per query at the
  DataFusion-entry boundary, not per-physical-plan-node breakdown.

## Capabilities

### New Capabilities

(none — this change extends spans already governed by the
`self-monitoring-traces` capability)

### Modified Capabilities

- `self-monitoring-traces`: adds two new requirements ("Database client
  spans for query execution" and "Query-execution metric attributes are a
  fixed low-cardinality allowlist") alongside the existing catalog-spans
  requirement, which is left unchanged. The existing "Query execution
  stage spans" requirement is modified so its per-stage INTERNAL spans nest
  under the new DB CLIENT span rather than directly under the Flight
  SERVER span, and its wording is narrowed from four described stages to
  the two spans the implementation actually produces (planning,
  execution — see design.md for why).

## Impact

- **querier**: `src/querier/src/flight.rs` (`execute_query`),
  `src/querier/src/query/ir_planner.rs` (`IrService::query`),
  `src/querier/src/query/metrics.rs` (`MetricsService::query_metric`),
  `src/querier/src/query/logs.rs` (`LogsService::query_logs`),
  `src/querier/src/query/trace.rs` (find/search) all gain span
  instrumentation at their DataFusion execution boundary.
- **common**: `src/common/src/self_monitoring/spans.rs` gains a new span
  factory; `src/common/src/self_monitoring/app_metrics.rs` metric-recording
  call sites gain new attributes.
- **otel registry**: `otel/registry/signaldb.yaml` gains a new span group;
  weaver live-check (`.github/workflows/weaver-live-check.yml`) picks it up
  automatically.
- **tests**: new pin tests in `src/common/tests/`, analogous to
  `db_catalog_span_semconv.rs`.
- Not breaking: this is additive telemetry only — no OTLP ingest, query
  API, Flight wire schema, or on-disk format changes.
