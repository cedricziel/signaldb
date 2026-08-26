## Why

SignalDB measures HTTP traffic it serves, but not well enough to answer the
first question an operator asks. `http.server.request.duration` is recorded
with three attributes — method, status, and a hardcoded `url.scheme = "http"`
that is simply wrong behind TLS — and no `http.route`, so every endpoint
across the router, the acceptor, and the MCP server collapses into a single
`GET` bucket and "which endpoint got slow" is unanswerable from metrics. The
route is known: the span middleware sitting immediately next to the metrics
middleware extracts it, along with scheme, protocol version, and server
address. Two middlewares independently derive overlapping attributes for the
same request, which is both wasted work and a licence to disagree.

Outbound HTTP is worse: it is not measured at all. Every Iceberg metadata
read and Parquet fetch travels over HTTP to S3/MinIO, and a slow query today
cannot be attributed to object storage rather than compute, because only the
total is recorded. The same is true of the SDK the CLI and MCP server consume.
The compactor, meanwhile, serves three HTTP endpoints with no instrumentation
at all — which already violates the `self-monitoring-traces` requirement that
every HTTP surface produce a server span.

## What Changes

- The HTTP server metrics and server span are produced by **one merged axum
  layer** deriving a single attribute set per request, so the metric and the
  span can no longer disagree about route, scheme, or error type.
- Server metrics gain the attributes the conventions require:
  **`http.route`**, **`error.type`**, `network.protocol.name`/`.version`,
  `server.address`/`server.port`, and a `url.scheme` **derived from the
  request** instead of the hardcoded literal.
- The merged layer is applied to every HTTP surface, closing two gaps: the
  **compactor's `/metrics`, `/status`, `/health`** (previously uninstrumented
  entirely) and the MCP server's `.well-known` discovery document
  (previously excluded by choice). Scrape endpoints are measured like any
  other route.
- A shared `serve()` helper in `common` owns the layer, and a CI guard
  forbids calling `axum::serve` directly outside `common` — so a new HTTP
  surface cannot ship uninstrumented, which is how the compactor's endpoints
  were missed.
- **HTTP client coverage for object storage**: a custom `HttpConnector`
  wraps `object_store`'s HTTP transport, emitting `http.client.*` metrics and
  CLIENT spans for every S3/MinIO request. Because `object_store` retries
  _above_ this hook, each attempt is measured individually with its own
  `error.type`.
- **HTTP client coverage for the Rust SDK** (`signaldb-sdk`), recorded in
  `retry::execute` — the single hook every generated operation already flows
  through. It additionally carries `url.template` (from the operation id) and
  `http.request.resend_count`, which the object-store hook structurally
  cannot provide.
- Client span URLs are sanitized before `url.full` is recorded, so presigned
  S3 credentials never reach telemetry.
- The browser client is explicitly **out of scope for metrics** and stays on
  spans (it already emits `http.client` spans via fetch auto-instrumentation);
  the rationale is recorded in design.md.

## Capabilities

### New Capabilities

None. `self-monitoring-metrics` is introduced by the `metric-convention-gate`
change, which this change depends on.

### Modified Capabilities

- `self-monitoring-metrics` — adds the HTTP server and HTTP client metric
  requirements.
- `self-monitoring-traces` — HTTP server spans on genuinely _every_ HTTP
  surface (the existing requirement names the compactor ops API, which does
  not currently produce spans), plus new HTTP CLIENT span requirements for
  object storage and the SDK, and the URL sanitization rule.

## Impact

- `common` — the merged `self_monitoring::http` layer replacing
  `http_metrics_middleware` + `http_trace_context_middleware`, the shared
  `serve()` helper, the instrumented `HttpConnector` used by
  `storage::create_object_store*`, and URL sanitization alongside the
  existing query-text sanitizer.
- `acceptor`, `router`, `mcp-server` — migrate from two middlewares to the
  merged layer and the shared `serve()` helper (12 attachment sites).
- `compactor` — its observability router becomes instrumented.
- `signaldb-sdk` — client metrics and spans in `retry::execute`, under the
  existing default-on `tracing` feature; adds
  `opentelemetry-semantic-conventions` as an optional dependency so the SDK
  pins the same constants without depending on `common`.
- `writer`, `querier` — no code change; they gain object-store client
  telemetry through the shared store constructor.
- Operator-facing: object-store latency, error rate, and retry volume become
  visible for the first time; per-endpoint HTTP breakdown becomes possible.
- Cardinality: roughly 1200 server series (routes × method × status) and ~30
  client series; no tenant attribute on any of them, per the cardinality
  rule this change inherits.
