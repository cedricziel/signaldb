# Tasks: server-timing-trace-response

## 1. Trace context response formatting (common)

- [x] 1.1 Write failing unit tests in `common/src/flight/trace_context.rs` for a `format_traceparent(&SpanContext) -> Option<String>` helper: valid context renders `00-<32hex>-<16hex>-<2hex>`, trace-flags reflect sampled/unsampled, invalid (all-zero) context returns `None` (`cargo test -p common`)
- [x] 1.2 Implement the formatting helper next to the existing parsing side; tests pass

## 2. Response header emission in shared middleware (common, router, acceptor)

- [x] 2.1 Write failing tests for `http_trace_context_middleware` covering: response carries `Server-Timing: traceparent;desc=...` + matching `traceresponse` when a tracer provider is active; caller-supplied traceparent yields same trace id / different span id; no headers when self-monitoring is disabled; no headers on `_system` tenant requests; `total;dur=` entry present; `Timing-Allow-Origin: *` present (`cargo test -p common`)
- [x] 2.2 Implement emission in the middleware: extract span context via `OpenTelemetrySpanExt::context()`, guard `is_valid()`, measure `total` around `next.run`, append `Server-Timing`, `traceresponse`, and `Timing-Allow-Origin` headers; tests pass
- [x] 2.3 Write failing test for a `ServerTimings` response-extension type: handler inserts named `Duration` pairs, middleware drains them into additional `name;dur=` entries (`cargo test -p common`)
- [x] 2.4 Implement `ServerTimings` extension draining in the middleware; tests pass
- [x] 2.5 Add stage timings to one router query endpoint (e.g. Tempo trace lookup or search) via `ServerTimings` as the reference usage, with a test asserting the named entry appears (`cargo test -p router`)
- [x] 2.6 Add integration coverage in `tests-integration` asserting both router and acceptor HTTP responses carry the headers in monolithic mode, and that a caller-supplied sampled `traceparent` round-trips its trace id (`cargo test -p tests-integration`)

## 3. UI consumption: document-load correlation (src/ui)

- [x] 3.1 Write failing tests for a `serverTiming` traceparent parser: extracts context from a navigation entry, strict validation (length/hex), rejects malformed `desc`, rejects all-zero ids, preserves trace-flags (`npm test` in src/ui)
- [x] 3.2 Implement the parser module in `src/ui/src/telemetry/`; tests pass
- [x] 3.3 Write failing tests for a `ServerCorrelationSpanProcessor`: adds a span link with the server context to the document-load root span on start; no-op when no entry, malformed entry, or flags `00` parenting is avoided; initialization never throws
- [x] 3.4 Implement the processor and register it in `telemetry/index.ts`; verify the pinned OTel JS SDK supports `span.addLink` (bump the SDK if not, attribute fallback only as last resort); tests pass

## 4. Docs and surface hygiene

- [x] 4.1 Document the `Server-Timing`, `traceresponse`, and `Timing-Allow-Origin` response headers in the router's OpenAPI description (`src/router/src/openapi.rs`) — headers only, no endpoint/schema changes, so no SDK/TS client regeneration needed
- [x] 4.2 Add a docs page on response trace context and server timings (what the headers mean, how to correlate from DevTools/RUM tools) — route placement via the docs skill
- [ ] 4.3 Update the `frontend-instrumentation` skill to describe server→client correlation via `serverTiming` and the link-not-parent policy
