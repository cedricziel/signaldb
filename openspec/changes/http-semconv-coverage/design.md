## Context

See proposal.md — Why. This change depends on `metric-convention-gate`: the
registry declarations, name pins, and cardinality rule it installs are what
keep the instruments added here conformant.

Facts that shape the approach:

- `common::self_monitoring::http_metrics_middleware` and
  `http_trace_context_middleware` are attached as a pair at 12 sites across
  acceptor (×4), router, and mcp-server. Both run the `_system` anti-loop
  check, both take an `Instant`, both walk headers; only the span one
  extracts `MatchedPath`, derives `url.scheme` from the URI, parses Host, and
  maps the protocol version.
- `object_store` 0.13.2 exposes the public `client::HttpConnector` /
  `HttpService` traits, wired through `AmazonS3Builder::with_http_connector`.
  Its retry layer (`client/retry.rs`) holds an `HttpClient` and calls it once
  per attempt, so a connector observes individual attempts, not logical
  operations. SignalDB constructs stores in one place
  (`common::storage::create_object_store` / `create_s3_builder_from_dsn`).
- `signaldb-sdk` routes every generated operation through the hand-written
  `retry::execute`, which already receives `OperationInfo` (operation id),
  the request, and an attempt counter, and already injects W3C trace context
  under its default-on `tracing` feature (which pulls `opentelemetry`).
- The `_system` suppression marker is a tokio task-local and filters the
  tracing layers only; metrics have no equivalent filter and use explicit
  tenant checks instead.
- The UI already registers fetch/XHR auto-instrumentation and exports spans
  and logs; it has no metrics SDK at all.

## Goals / Non-Goals

**Goals:**

- HTTP server telemetry that answers "which endpoint" and "which failure
  mode", with metric and span attributes guaranteed identical.
- Structurally complete coverage: no HTTP surface can be served without the
  layer, and no HTTP client call leaves the process unmeasured.
- Object-store latency separated from compute in query analysis.

**Non-Goals:**

- Browser metrics (see D6).
- `http.client.open_connections` and `http.client.connection.duration` —
  connection lifecycle lives below the `HttpService` hook and is not
  observable there (D4).
- Flight/gRPC metrics: that is the follow-on Flight→RPC change.
- Changing routing, retry policy, or storage behavior. This change observes
  only.

## Decisions

### D1: One merged layer, not two middlewares

A single `common::self_monitoring::http` layer derives the attribute set once
and feeds both the SERVER span and the four server metrics. This removes the
duplicated header parsing, the duplicated `_system` check, and — the actual
motivation — the possibility that the metric's `http.route` or `error.type`
differs from the span's, which would break the metric→trace pivot the
`metric-convention-gate` vocabulary work exists to enable.

_Alternative considered:_ add the missing attributes to the existing metrics
middleware in place (a ~40-line diff). Rejected: it leaves two
implementations of the same extraction free to drift again, which is the
defect pattern this whole effort is correcting.

### D2: Instrumentation is bound to serving, not to the router

`common` gains a `serve()` helper that applies the layer and runs
`axum::serve`; a CI guard rejects direct `axum::serve` calls outside
`common`. The compactor's endpoints were missed precisely because attaching
the layer was an opt-in step someone had to remember, and the
`self-monitoring-traces` spec has been silently violated since.

_Alternative considered:_ a guard requiring the layer at every `Router::new`
site. Rejected: routers nest and merge freely, so the count of routers is not
the count of surfaces; serving is the real boundary.

### D3: Scrape and discovery endpoints are measured like anything else

`/metrics`, `/health`, `/status`, and `.well-known/...` go through the layer.
The compactor's `/metrics` scrape therefore reports its own previous
duration on the next scrape — self-referential but honest, and scrape
failures become visible. The cost is three routes' worth of series.

_Alternative considered:_ exclude scrape/health endpoints as noise (the
current treatment of `.well-known`). Rejected: uniformity is what makes D2's
guarantee meaningful, and health-endpoint latency is genuine signal when a
service is degrading.

### D4: Client coverage lands at the lowest hook each client offers

Object storage is instrumented at `HttpConnector`; the SDK at
`retry::execute`. The two sit on opposite sides of their retry loops, which
is a feature: S3 retries appear as additional measured requests carrying
`error.type`, while SDK retries appear as `http.request.resend_count` on one
logical request. Neither hook can observe connection establishment, so
`http.client.open_connections` and `http.client.connection.duration` are out
of scope rather than approximated.

### D5: The SDK stays independent of `common`

`signaldb-sdk` is the standalone client crate; it cannot use the instrument
factory in `common`. It gets its own small instrument holder plus
`opentelemetry-semantic-conventions` as an optional dependency, so both
crates pin identical constants. Instruments bind lazily from the global meter
provider and are a no-op for consumers who never install one.

Metrics live under the existing default-on `tracing` feature rather than a
new `metrics` feature: the feature already pulls `opentelemetry`, so no
consumer gains a dependency, and a published crate does not grow a second
telemetry knob for one behavior.

### D6: The browser stays spans-only

Browser metrics would multiply every series by the session count, because
`session.id` lives in the resource; RUM metrics are properly derived
server-side from spans. The UI already emits `http.client` spans through
fetch auto-instrumentation, so client-side visibility exists — it is the
metric pipeline that is deliberately absent.

### D7: No tenant-aware suppression on the client hooks

At the `HttpService` layer there is no tenant in scope, and `object_store`'s
spawned-connector variant would not carry a task-local marker anyway.
Self-monitoring writes therefore produce object-store client metrics, which
are themselves stored — a bounded constant-factor amplification, not the
re-entrant span loop that motivated the existing suppression guard. Accepted
and documented rather than partially mitigated.

## Risks / Trade-offs

- **[`http.route` raises server-metric cardinality ~40×]** → Bounded by the
  route table (templates, never raw paths), no tenant attribute, and
  measured before/after on a real deployment as part of verification.
- **[The merged layer changes 12 call sites at once]** → Behavior is pinned
  first by the existing span-semconv tests
  (`common/tests/http_span_semconv.rs`, `http_response_trace_context.rs`),
  extended to assert the metric attributes, so the migration is refactoring
  under test rather than rewriting.
- **[A custom `HttpConnector` sits in the storage data path]** → It wraps
  only; it must not retry, buffer, or alter requests. Tested against the
  in-memory and local stores, which bypass it entirely, plus an S3 path
  exercised with a mock/MinIO endpoint.
- **[Presigned URLs leak credentials via `url.full`]** → Sanitization
  applied before recording, sitting next to the existing query-text
  sanitizer, with a test asserting signature and credential parameters are
  redacted.
- **[SDK telemetry surprises embedders]** → It is inert without a global
  meter provider, and the feature that enables it is the one they already
  opt into for trace-context propagation.

## Migration Plan

1. Merged layer plus the `serve()` helper in `common`, migrating the 12
   existing sites; the compactor and `.well-known` gain coverage in the same
   step.
2. Add the CI guard once no direct `axum::serve` remains outside `common`.
3. Object-store connector, wired at the single store-construction site.
4. SDK client telemetry.
5. Rollback is per-step and independent: each step is observation-only, so
   reverting one leaves the others working.
