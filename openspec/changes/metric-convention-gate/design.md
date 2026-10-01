## Context

See proposal.md — Why. Two mechanisms already keep spans conformant and are
the templates for everything here: the literal-vs-constant pins in
`common::self_monitoring::spans` (unit tests asserting
`"db.operation.name" == attribute::DB_OPERATION_NAME`) and the registry
drift gate in `src/common/tests/registry_pins.rs`, which walks every `.rs`
file, extracts `signaldb.*` span-field usages, and fails when one is absent
from `otel/registry/signaldb.yaml`.

Constraints that shape the approach:

- `registry_pins` extracts span fields syntactically — a macro field
  assignment or the first argument of `record("…")` — and explicitly skips
  instrument names. Extending it means a second extractor, not a tweak.
- Instruments bind once per process from the global meter provider
  (`AppMetrics::from_global_meter`, `OnceLock`), so tests asserting on
  emitted metrics need one test binary each; several such single-test files
  already exist (`common/tests/wal_pending_gauge.rs`,
  `common/tests/mcp_tool_metrics.rs`).
- `signal-producer` legitimately constructs its own instruments: it
  fabricates third-party fleet telemetry (`http.server.request.duration`
  and friends for imaginary services) and is not SignalDB self-monitoring.
- The compactor exposes a second, hand-rendered Prometheus surface
  (`compactor/src/http.rs:69`) whose label names are the Prometheus
  rendering of registry attributes; it is already registry-declared and
  stays as it is.

## Goals / Non-Goals

**Goals:**

- One attribute vocabulary shared by spans and metrics, so a metric series
  and the spans explaining it join on identical keys.
- A gate that fails the build when a new instrument or metric attribute is
  undeclared, so later changes cannot re-drift.
- Metric names that OpenTelemetry defines are pinned to the semconv crate,
  not typed as isolated string literals.

**Non-Goals:**

- Renaming `signaldb.*` instrument _names_ (e.g. `signaldb.flight.*` →
  `rpc.server.*`). That is the follow-on Flight→RPC change; this change
  fixes attribute vocabulary and installs the gate.
- Adding any new instrument. Coverage work lands in follow-on changes.
- Changing the metric export pipeline, temporality, or exporter config.

## Decisions

### D1: The span vocabulary wins, not the metric one

`signaldb.tenant.id` / `signaldb.dataset.id` / `signaldb.table` are already
declared in the registry, already emitted by every span factory, and already
enforced. Metrics adopt them; the shorter `tenant` / `tenant_id` forms are
deleted.

_Alternative considered:_ keep the short forms on metrics because Prometheus
label ergonomics favour brevity (`tenant` reads better than
`signaldb_tenant_id`). Rejected: the whole point is the join, and having the
Prometheus rendering of one concept differ from the trace attribute for the
same concept is precisely the defect being fixed.

### D2: Namespace every SignalDB-specific metric attribute under `signaldb.*`

`record` → `signaldb.wal.record_type`, `signal` → `signaldb.signal`,
`query_type` → `signaldb.query.type`, `surface` → `signaldb.ratelimit.surface`,
`kind` → `signaldb.ratelimit.dimension`. Unnamespaced single words are
unregisterable (the registry namespace is `signaldb.*`), unjoinable, and
collide with attributes a tenant's own applications emit into the same
backend.

`signaldb.signal` is deliberately distinct from the existing
`signaldb.table`: a signal is `traces | logs | metrics | profiles`, a table
is a physical table name such as `metrics_gauge`.

### D3: `service.name` never appears on a data point

`metrics.rs` records `service.name` on five observable-instrument callbacks
even though `app_metrics.rs:8` documents that the resource already carries
it. Those callbacks observe per-service process/system values from a single
process, so the resource is the correct carrier; the per-point copy both
duplicates and invites a second, divergent value.

### D4: Cardinality rule — convention metrics carry only convention attributes

An instrument whose name OpenTelemetry defines carries the attribute set the
convention defines, and nothing else; in particular no tenant. Tenant
identity belongs on `signaldb.*` instruments where per-tenant accounting is
the purpose (storage usage, ingest counts, rate-limit rejections).

_Alternative considered:_ tenant on everything, for per-tenant RED. Rejected:
it multiplies every histogram by the tenant count, on a backend that ingests
its own output, and semconv defines no tenant attribute for these metrics —
so a dashboard built on it would not port to any other backend.

### D5: The gate is a second extractor in `registry_pins`, not a new test file

Instrument names and their attribute keys are extracted from the
`AppMetrics::from_global_meter` constructor and from `KeyValue::new` sites,
then checked against `type: metric` groups in the registry. Keeping it in
the same test keeps one failure message and one place to look.

_Alternative considered:_ a Weaver-based check over live-check output.
Rejected as the primary gate — live-check needs a running deployment and
covers what was _exercised_, so an unexercised instrument passes silently. It
remains a valuable second layer.

### D6: Instrument construction is confined to `common::self_monitoring`

A CI grep guard rejects meter/instrument builder calls outside
`src/common/src/self_monitoring/` (with `src/signal-producer/` exempted, per
Context). This mirrors the existing `otel.kind` guard and is what makes the
registry gate complete: an instrument created elsewhere would never be seen
by the extractor.

### D7: Clean break, no dual emit

Per project policy post-1.0, the renamed attributes ship without aliases or a
transition window. The affected consumers are operator dashboards, edited in
one place, not client code.

## Risks / Trade-offs

- **[Operator dashboards break on deploy]** → The rename set is small and
  fully enumerated in the proposal; the change ships with a docs update
  listing old → new label names, so a dashboard edit is mechanical.
- **[The syntactic extractor produces false positives/negatives]** → It is
  the same class of extractor already trusted for span fields, and D6 bounds
  the surface it must cover to a single module. A missed instrument is caught
  by the second layer (Weaver live-check) rather than shipping undetected.
- **[`signal-producer` exemption becomes a hole]** → The exemption is
  path-scoped in the guard and that crate emits no SignalDB self-monitoring
  telemetry; if it ever does, the guard fails loudly rather than silently
  allowing it.
- **[Registry churn slows later changes]** → Intended. Declaring an
  instrument is a five-line YAML block and is the cheapest moment to think
  about its attribute set and cardinality.

## Migration Plan

1. Land the registry declarations first (additive, no behavior change).
2. Rename the attributes at their recording sites, one commit per crate.
3. Extend `registry_pins` and add the CI guard last, so the gate closes on an
   already-clean tree rather than reporting the pre-existing drift.
4. Rollback is a revert: nothing persists, metric state is process-local, and
   no stored data carries these names.
