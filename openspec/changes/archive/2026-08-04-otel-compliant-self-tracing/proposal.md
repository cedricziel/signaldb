# otel-compliant-self-tracing

## Why

SignalDB positions itself as OpenTelemetry-native, but the telemetry it emits
about itself does not follow the OTel trace semantic conventions it expects
from its own users. A survey of the codebase found exactly one compliant
surface (the router's HTTP server span middleware) against a field of gaps:
OTLP ingest on the acceptor produces no server span and silently drops
inbound `traceparent` (a client propagating context into `/v1/traces` gets a
detached ingest trace); the two Flight RPC boundaries export as kind-less
INTERNAL spans named `flight_do_get`/`flight_do_put` with zero `rpc.*`
attributes and no CLIENT spans on the calling side; DataFusion execution,
Iceberg/object-store I/O, and the SQL catalog are unspanned black boxes;
compactor retention/snapshot/orphan jobs — the operations most in need of
forensics — emit nothing; the resource carries the deprecated
`deployment.environment` attribute with a hardcoded wrong value and no
`service.instance.id`; and ~13 bare `#[tracing::instrument]` sites
auto-record handler arguments (cardinality and PII risk). Self-monitoring is
a first-class deployment mode (`_system`/`_monitoring` tenant) and the
product's own credibility story — its traces should be the reference example
of the conventions, and compliance should be enforced so it stays true.

## What Changes

- **Pin semconv v1.43.0** as the version SignalDB's own telemetry targets;
  emit the matching `schema_url` on resource and instrumentation scope.
- **Acceptor becomes a proper trace boundary**: SERVER spans on OTLP gRPC
  (:4317) and OTLP/HTTP + remote-write (:4318) with inbound W3C context
  extraction, so client ingest traces join instead of detaching.
- **Flight hops get RPC semconv**: SERVER spans on querier `do_get` /
  writer `do_put` and CLIENT spans on all router/acceptor Flight call sites,
  using the post-1.39 attribute names (`rpc.system.name`, `rpc.method`,
  `rpc.response.status_code`) with SignalDB-namespaced extras (ticket verb,
  batch/row counts). Replaces the ad-hoc `flight_do_get`/`flight_do_put`
  spans — **BREAKING** for self-monitoring dashboards/alerts keyed on the old
  span names (accepted per post-1.0 policy; no aliases).
- **SQL catalog calls get `db.*` CLIENT spans** (`db.system.name`,
  `db.operation.name`, `db.namespace`) for SQLite/Postgres catalog and
  discovery queries.
- **Query execution becomes observable**: stage-level INTERNAL spans in the
  querier (plan, Iceberg scan, execute, encode) with row/byte counts and
  sanitized query text following the stable DB-semconv sanitization rules.
- **Compactor lifecycle jobs get root spans**: retention enforcement,
  snapshot expiration, orphan cleanup, alongside the existing compaction-job
  span, all with consistent tenant/dataset attributes.
- **Resource attributes fixed**: `deployment.environment.name` (real value,
  stable name), `service.instance.id` (UUID per process),
  `service.namespace = signaldb`.
- **Span hygiene**: every span at a remote boundary carries the correct
  SpanKind and `error.type`; 4xx/client-fault gRPC codes do not mark server
  spans as errors; bare `#[tracing::instrument]` sites move to
  `skip_all` + explicit fields; SignalDB-local fields consolidate under a
  `signaldb.*` attribute namespace (e.g. `signaldb.tenant.id`).
- **Compliance is enforced structurally**: semconv span factories in
  `common` become the only sanctioned way to open boundary spans (lint/CI
  guard against raw span macros at boundaries); per-surface
  `InMemorySpanExporter` conformance tests extend the existing
  `http_span_semconv.rs` pattern; a SignalDB Weaver registry (depending on
  upstream semconv v1.43.0) defines the `signaldb.*` conventions with
  `weaver registry check`/`diff` gates; and a `weaver registry live-check`
  CI job validates actually-emitted telemetry by pointing the
  self-monitoring OTLP exporter at Weaver's listener under generated load.
- The WAL boundary's existing link-based fan-in (writer batch links to each
  source ingest trace) is affirmed as the correct model and pinned by test;
  WAL attributes move to the `signaldb.wal.*` namespace rather than the
  unstable messaging semconv.

## Capabilities

### New Capabilities

- `self-monitoring-traces`: the contract for spans SignalDB emits about its
  own operation — semconv-conformant span names, kinds, attributes, and
  status mapping at every HTTP/RPC/DB boundary; end-to-end trace continuity
  (client → acceptor → WAL-link → writer, client → router → querier);
  resource identity; and the `signaldb.*` attribute namespace, validated
  against a versioned convention registry.

### Modified Capabilities

None — OTLP ingestion, query-API, and tenancy behavior for user data are
unchanged; this change governs only the telemetry SignalDB emits about
itself.

## Impact

- **Crates**: `common` (span factories, telemetry init, resource,
  `flight/trace_context`, self-monitoring middleware), `acceptor` (OTLP
  gRPC/HTTP server spans + extraction), `router` (Flight CLIENT spans,
  instrument hygiene), `querier` (Flight SERVER span, query stage spans),
  `writer` (Flight SERVER span, WAL/Iceberg spans), `compactor` (lifecycle
  job spans), `mcp-server` (mount existing HTTP trace middleware only).
- **Dependencies**: add `opentelemetry-semantic-conventions` (with
  `semconv_experimental` for `rpc.*`); Weaver is a CI-only tool (pinned
  0.x release, not a crate dependency).
- **CI**: new conformance jobs — registry check/diff, generated-constants
  drift gate, live-check harness reusing the e2e stack and
  `signal-producer`.
- **Operators**: self-monitoring span names and attributes change
  (**BREAKING** for dashboards keyed on `flight_do_get`/`flight_do_put` or
  bare `tenant_id` span fields); docs get a reference page describing the
  emitted trace model.
- **Not affected**: OTLP ingest semantics, Tempo/LogQL/PromQL surfaces,
  Flight wire schemas, on-disk layout — no data-plane changes.
