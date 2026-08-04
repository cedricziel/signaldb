# Design — otel-compliant-self-tracing

## Context

See `proposal.md` for motivation and the survey findings. What shapes the
design:

- The compliant pattern already exists once: `http_trace_context_middleware`
  (`src/common/src/self_monitoring/app_metrics.rs`) plus its regression test
  `src/common/tests/http_span_semconv.rs` (InMemorySpanExporter). The design
  generalizes that pattern rather than inventing a new one.
- W3C propagation plumbing is complete and correct across four carriers
  (`src/common/src/flight/trace_context.rs`: HTTP headers, tonic metadata,
  Flight `app_metadata` JSON, span links), including the
  tracing-opentelemetry 0.33 constraint that a span's parent must be set
  before its first enter. The writer's link-based WAL fan-in
  (`src/writer/src/processor.rs:467`) is already the semconv-correct batch
  model and is preserved, not redesigned.
- `tracing` macros require _literal_ field names — semconv constants cannot
  be interpolated into `info_span!`. The dotted attribute names must be
  written out somewhere; the design ensures that "somewhere" is exactly one
  module in `common`.
- The RPC semantic conventions were rewritten in semconv 1.39–1.41
  (`rpc.service` removed, `rpc.system` → `rpc.system.name`,
  `rpc.grpc.status_code` → string-valued `rpc.response.status_code`); no
  maintained tonic middleware crate emits the new names, and the
  `opentelemetry-semantic-conventions` crate gates all `rpc.*` constants
  behind `semconv_experimental`.
- Self-monitoring exports OTLP/gRPC into SignalDB itself with an anti-loop
  suppression layer (`OtelExportFilter`); any new spans on the ingest path
  must go through the same `_system`-tenant suppression to avoid feedback.
- FDAP constraint: any Arrow/Parquet types touched in instrumentation code
  come from DataFusion's re-exports.

## Goals / Non-Goals

**Goals:**

- One sanctioned construction path per boundary type (HTTP, RPC server, RPC
  client, DB client, background job) in `common`, so compliance is a
  property of construction, not review vigilance.
- Every span-shape rule pinned by a test that reads exported spans, not by
  documentation.
- A machine-readable registry as the single source of truth for
  `signaldb.*` conventions, checked in CI both statically (registry check /
  diff) and dynamically (live-check against emitted telemetry).

**Non-Goals:**

- Instrumenting the ingested-data path's _content_ (that is
  `otel-native-schema`'s territory; this change shares only the pinned
  semconv version).
- Metrics or log semconv compliance (spans only; the existing HTTP metrics
  attributes stay as they are).
- Per-object-store-request spans (request-level object-store visibility is
  a metrics concern; spans stop at query stages).
- Migrating exception span events to the new exception-log-record
  convention (deprecated-but-functional; deferred until the Rust SDK's
  opt-in story stabilizes).
- Browser/UI instrumentation (covered by the frontend-instrumentation
  work).

## Decisions

### D1. Span factories in `common::self_monitoring::spans` (new module)

All boundary spans are opened through typed constructors:

- `http_server_span` — extracted from the existing middleware (behavior
  unchanged, gains `error.type` + `server.port`/`client.address`).
- `rpc_server_span(method, ticket_verb)` / `rpc_client_span(method, addr)`
  — Flight/gRPC, emit `rpc.system.name`, `rpc.method`,
  `rpc.response.status_code`; kind server/client; status mapping per the
  server/client asymmetry in the spec.
- `db_client_span(system, operation, namespace)` — catalog access.
- `job_span(job_kind, tenant, dataset, table)` — compactor lifecycle and
  writer batch processing (the latter with `add_link_from_fields`).

Each factory declares the literal dotted field names exactly once,
pre-declared `Empty` where recorded late (status codes). Rationale: the
literal-field-name constraint of `tracing` macros makes centralization the
only way constants stay consistent; alternatives (per-call-site literals,
proc-macro wrapper) either scatter the names or add build complexity for no
coverage gain.

### D2. New RPC attribute names, hand-rolled tonic/Flight layer

Adopt post-1.39 names now (`semconv_experimental` feature of the semconv
crate). Alternatives rejected: the deprecated names (`rpc.system`,
`rpc.service`, `rpc.grpc.status_code`) are what existing ecosystem crates
emit, but SignalDB consumes its own telemetry, the old names are already
marked deprecated with `renamed_to:` metadata, and hand-rolling is required
either way since no crate emits the new names for tonic. Flight has no
semconv of its own; it is modeled as plain gRPC
(`rpc.method = arrow.flight.protocol.FlightService/DoGet`) with the ticket
verb appended to the span name (`… DoGet query_ir`) — explicitly permitted
by the RPC naming rules — and Flight detail in `signaldb.*` attributes.

Server-side extraction lives in the factory call sites in
querier/writer/compactor Flight impls (replacing `flight_do_get` /
`flight_do_put` spans in place, keeping the existing suppression-scope and
parent-before-enter handling). Client-side, the 8 router injection sites
plus acceptor `do_put` call sites wrap the call in `rpc_client_span` so
injection reads the client span's context instead of the ambient one.

### D3. Acceptor boundary

OTLP/HTTP and remote-write routers mount the same
`http_trace_context_middleware` the router uses (it already handles
`_system` bypass). OTLP gRPC gets a thin tower layer on the tonic server
stack applying `rpc_server_span` + `set_parent_from_request` uniformly to
all four OTLP services — a layer rather than per-service edits so a fifth
signal service cannot forget it. The existing `#[instrument]` handler spans
remain as INTERNAL children.

### D4. SignalDB Weaver registry + generated constants

`otel/registry/` in-repo: `manifest.yaml` (format `manifest/2.0`) depending
on upstream semconv v1.43.0 by `schema_url` + git tag, plus `signaldb.*`
attribute/span groups (tenant/dataset/wal/query/compaction). Rust constants
for `signaldb.*` names are generated into
`common/src/self_monitoring/conventions.rs` via `weaver registry generate`
using the opentelemetry-rust template set, with a checked-in-output +
`git diff --exit-code` drift gate (the opentelemetry-rust approach; no
`build.rs`, Weaver pinned by version in CI, not a crate dependency).
Upstream OTel names come from the `opentelemetry-semantic-conventions`
crate. Rationale: registry-first makes the live-check meaningful
(`registry_coverage`) and gives `registry diff` as an evolution gate;
alternative (constants only, no registry) enforces spelling but not shape.

### D5. Conformance testing, three tiers

1. **Unit pins**: extend the `http_span_semconv.rs` pattern — one
   InMemorySpanExporter test per factory (HTTP, RPC server, RPC client, DB,
   job/link) asserting name, kind, required attributes, status mapping, and
   the WAL link-count behavior.
2. **Static gates**: `weaver registry check` + `registry diff
--baseline-registry` (against the previous release tag) + generated-code
   drift check, all in the lint CI job.
3. **Live-check**: a CI job boots the monolithic binary with
   self-monitoring enabled and `self_monitoring.endpoint` pointed at
   `weaver registry live-check`'s OTLP gRPC listener, drives ingest via
   `signal-producer` and queries via the HTTP API, then stops the listener
   and fails on violation-level findings (`--fail-on violation`), using the
   official `setup-weaver` / `weaver-live-check-*` composite actions
   (pattern proven in opentelemetry-rust-contrib CI). Noise is managed in
   `.weaver.toml` finding filters, not by weakening the registry.

### D6. Keeping raw spans out of boundaries

Boundary-adjacent crates get a CI guard that raw `info_span!` /
`#[tracing::instrument]` may not appear in designated boundary modules
(Flight impls, HTTP router assembly, handler entry points) — implemented as
a spike on clippy `disallowed-macros` (per-crate `clippy.toml`), falling
back to a simple grep-based check in the lint job if clippy's
attribute-macro coverage proves insufficient. INTERNAL spans elsewhere
remain free-form `#[instrument(skip_all, fields(...))]` — the discipline
applies to boundaries, not to every span. Bare `#[tracing::instrument]`
(no `skip_all`) is disallowed workspace-wide.

### D7. Sampler fallback fix

`resolve_trace_sampler`'s unrecognized-name arm changes to
`ParentBased(TraceIdRatioBased)` to match the unset default, closing the
typo-reintroduces-the-sampler-bug hole. (One-line behavioral fix that
belongs with this change's "trace continuity" story.)

## Risks / Trade-offs

- **RPC semconv is Release Candidate; names could still shift** → all
  `rpc.*` emission funnels through two factories; a rename is a one-module
  change plus registry bump, and `registry diff` will surface it.
- **Weaver is 0.x with breaking minor releases** → pin the Weaver version
  everywhere it runs (composite action input + docker tag); live-check job
  is `continue-on-error: false` only after a bake-in period as a
  non-blocking job.
- **Breaking self-mon dashboards** (span names `flight_do_get` →
  `…FlightService/DoGet …`, field `tenant_id` → `signaldb.tenant.id`) →
  accepted per post-1.0 policy, no aliases; the docs task ships a
  before/after rename table so operators can migrate alerts in one pass.
- **Span volume growth** (acceptor server spans + query stage spans on
  every request) → head sampling already defaults to
  ParentBased(ratio 0.1); stage spans are children, so sampling decisions
  scale the whole trace; `_system` suppression prevents self-amplification.
  Batch-size attributes stay on existing spans rather than adding per-batch
  spans in the writer hot path.
- **Live-check flakiness in CI (timing, partial traffic)** → the job
  asserts conformance of what was emitted, not completeness of coverage;
  coverage regression is tracked via the reported `registry_coverage`
  metric as a non-gating signal first.
- **`semconv_experimental` pulls in a large constant surface** → compile
  -time only; unused constants cost nothing at runtime.

## Migration Plan

Single-repo, no data-plane changes; deploys like any release. Operators
with self-mon dashboards consult the rename table (docs task). Rollback =
revert; emitted telemetry shape is not persisted state. Implementation is
sequenced as a stack of small PRs (see `tasks.md` groups): factories+tests
→ resource/sampler → acceptor → Flight server → Flight client → catalog/db
→ query stages → compactor → hygiene sweep → registry+static gates →
live-check CI.

## Open Questions

- Whether clippy `disallowed-macros` catches `#[tracing::instrument]`
  (attribute position) or only bang-macros — resolved by the D6 spike;
  either outcome has a working fallback and changes no spec or task.
- Exact stage boundaries for querier spans (e.g. whether Iceberg metadata
  fetch is its own stage or folded into scan) — settled during
  implementation by what DataFusion's execution structure exposes cheaply.
