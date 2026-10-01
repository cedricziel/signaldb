## Why

SignalDB's self-emitted **spans** are semantic-convention-conformant by
construction: factories own the attribute literals, the literals are pinned
against the `opentelemetry-semantic-conventions` crate, CI forbids
`otel.kind` outside the factory module, and `registry_pins` fails the build
if a span attribute is missing from `otel/registry/signaldb.yaml`. Its
self-emitted **metrics** have none of that, and have drifted accordingly:
the same tenant is labelled `tenant`, `tenant_id`, and `signaldb.tenant.id`
depending on which recording site you look at, so an operator cannot pivot
from a metric series to the trace that explains it. Unnamespaced labels
(`kind`, `record`, `surface`, `signal`, `query_type`) sit in a global
attribute namespace where they collide with anything a tenant's own
telemetry uses, `service.name` is re-recorded per data point in
contradiction of the module's own documentation, and roughly thirty
`signaldb.*` instrument names are undeclared in the convention registry —
`registry_pins` skips instrument names deliberately (`registry_pins.rs:27`).
Every metric added from here — HTTP, RPC, DB — inherits that drift unless
the gate exists first.

## What Changes

- **BREAKING** — one tenancy vocabulary across signals: metric attributes
  `tenant` and `tenant_id` are replaced by `signaldb.tenant.id`, the key the
  spans and the registry already use. Prometheus label names on the
  self-monitoring surface change accordingly (`tenant` → `signaldb_tenant_id`).
- **BREAKING** — unnamespaced metric attributes are namespaced:
  `record` → `signaldb.wal.record_type`, `signal` → `signaldb.signal`,
  `query_type` → `signaldb.query.type`, and the rate-limit pair
  `surface`/`kind` → `signaldb.ratelimit.surface` /
  `signaldb.ratelimit.dimension`.
- **BREAKING** — `service.name` is removed from every metric data point; it
  is a resource attribute and is already carried by each service's meter
  provider resource.
- Every instrument SignalDB emits is declared in `otel/registry/signaldb.yaml`
  as a `type: metric` group with its instrument kind, unit, and attribute
  set — extending the three compactor entries that already exist to the full
  inventory.
- `registry_pins` stops skipping instrument names: a new instrument or a new
  metric attribute that is not declared in the registry fails the test.
- Metric names defined by OpenTelemetry are pinned against
  `opentelemetry_semantic_conventions::metric::*` constants, mirroring the
  attribute pins in `common::self_monitoring::spans`.
- Instruments may only be constructed inside `common::self_monitoring`
  (the `AppMetrics` holder), enforced by a CI guard, with `signal-producer`
  exempt — it fabricates third-party telemetry rather than emitting
  SignalDB's own.
- A written cardinality rule: metrics defined by OpenTelemetry carry only the
  attributes the convention defines (no tenant); tenant identity appears only
  on `signaldb.*` instruments where per-tenant accounting is the point.

## Capabilities

### New Capabilities

- `self-monitoring-metrics` — the contract for the metric telemetry SignalDB
  emits about its own operation: naming, units, attribute vocabulary,
  registry declaration, and cardinality bounds. The sibling of the existing
  `self-monitoring-traces` capability, which covers spans only.

### Modified Capabilities

None. `self-monitoring-traces` is untouched: this change alters no span.

## Impact

- `common` — `self_monitoring::app_metrics` (instrument definitions, the
  rate-limit recording helper), `self_monitoring::metrics` (per-point
  `service.name` removal), `wal` (record/signal attributes),
  `storage_usage` (tenant attribute), and the `registry_pins` test.
- `writer` — `processor` tenant/signal attributes.
- `querier` — `flight` query-type attribute.
- `acceptor` — the four OTLP service tenant attributes.
- `compactor`, `mcp-server` — instrument declarations in the registry
  (recording sites already use the namespaced vocabulary).
- `otel/registry/signaldb.yaml` — the full metric inventory.
- Operator-facing: any dashboard or alert built on the renamed labels needs
  updating. There is no dual-emit period; per project policy post-1.0,
  breaking changes ship without aliases.
- Follow-on changes (`http-semconv-coverage`, and the planned Flight→RPC,
  DB-client, and WAL-age work) land inside this gate and are cheaper for it.
