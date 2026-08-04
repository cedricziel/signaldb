# Design: server-timing-trace-response

## Context

See `proposal.md` for motivation and `specs/` for the behavior contract.

Current state that shapes the approach:

- `common/src/self_monitoring/app_metrics.rs::http_trace_context_middleware` is the single choke point: it mints the OTel-semconv SERVER span for every HTTP request, adopts the caller's `traceparent` via `flight/trace_context.rs::set_parent_from_http_headers`, and already sees the response on the way out (it records `http.response.status_code` after `next.run(...)`). Both router and acceptor install it; monolithic mode inherits it.
- The `_system` tenant bypass returns early _before_ any span exists — the "no span → no headers" guard falls out naturally.
- With self-monitoring disabled there is no tracer provider; the `tracing` span exists but its OTel context is invalid (all-zero ids). Emitting that would violate the spec, so validity must be checked explicitly.
- `flight/trace_context.rs` owns traceparent formatting/parsing idioms and the `TRACEPARENT` constant — the natural home for a response-side formatting helper.
- The UI (`src/ui/src/telemetry/`) already has a SpanProcessor pipeline (`NavigationSpanProcessor`, `SessionSpanProcessor`) that new processors slot into, and `getWebAutoInstrumentations` provides the `documentLoad` span this change decorates.
- FDAP constraint (stated for completeness): any Arrow/Parquet types must come from DataFusion re-exports. This change touches none of them — no Flight v1/v2 schema transforms, no WAL or Iceberg layout changes, hence no data migration or rollback concerns.

## Goals / Non-Goals

**Goals:**

- One implementation in shared middleware; services inherit it, no per-service opt-in.
- Headers derived from the _actual_ server span (same trace id the trace viewer will show, real sampling flags).
- A mechanism for stage-level `dur` entries that handlers can feed without the middleware knowing endpoint internals.
- UI document-load correlation that degrades to a no-op on old servers, stripped headers, or disabled tracing.

**Non-Goals:**

- Parenting the document-load span under the server span (link only for now; parenting is a follow-up once sampled-flag handling is proven).
- Correlation for fetch/XHR API calls — the request-side `traceparent` already joins those traces; the response header is informational there.
- Rich stage timings on every endpoint — only `total` is required initially; stages land opportunistically on query endpoints.
- Surfacing `Server-Timing` values in the UI query editor (attractive follow-up, separate change).
- gRPC/Flight surfaces (querier, writer): trailer-based response context is a different mechanism, out of scope.

## Decisions

### 1. Emit from `http_trace_context_middleware`, not a new layer

The span whose context we return is created there; adding a layer would need cross-layer smuggling of the span context. The middleware already holds the span across `next.run(...)`, so appending response headers after the handler returns is a natural extension. Alternative (tower `SetResponseHeader`) rejected: it cannot see the OTel span context.

### 2. Both `Server-Timing: traceparent` and `traceresponse`

`Server-Timing` is the only channel browsers expose via the Performance API (works for document/resource requests JS never made) and is the de-facto RUM convention (Splunk, Elastic, Dynatrace). `traceresponse` (W3C Trace Context Level 2) is the standards-track future at the cost of one extra header line. Emitting both is trivial; picking one would either break browser consumption or bet against the standard.

### 3. Span context extraction and validity guard

Use `tracing_opentelemetry::OpenTelemetrySpanExt::context()` on the middleware's span, take its `SpanContext`, and emit headers only when `span_context.is_valid()`. This is the same bridge the rest of self-monitoring uses; when self-monitoring is off the context is invalid and headers are skipped — no config lookup needed. Formatting helper (`00-{trace_id:032x}-{span_id:016x}-{flags:02x}`) lives in `flight/trace_context.rs` next to the parsing side, with unit tests. Trace-flags come from the span context, so the sampler's decision propagates for free.

### 4. Stage timings via response extensions; `total` measured in the middleware

The middleware measures `total` itself (`Instant::now()` around `next.run`). For stages, handlers insert a small `ServerTimings` value (name + `Duration` pairs) into response extensions; the middleware drains it and appends `name;dur=` entries. This keeps the middleware endpoint-agnostic and makes stage timings a pure opt-in for handlers (query endpoints first). Alternative — a task-local/span-field accumulator — rejected as more magical than a typed extension for no gain. Entry names must be low-cardinality tokens (they are header values, not logs).

### 5. `Timing-Allow-Origin: *` by default

The entries carry a trace id and coarse durations — information we deliberately return to the caller anyway — so restricting TAO adds config surface without protecting anything. Default `*`, aligned with the homelab-first posture; if a deployment later needs to hide timings from third-party origins, a config knob can narrow it (the spec's "configured consumers" wording permits `*`).

### 6. UI: parse once, link via an `onStart` SpanProcessor

A small module parses `performance.getEntriesByType("navigation")[0].serverTiming` for a `traceparent` entry, validating the `00-`-versioned format strictly (regex on lengths + hex, reject all-zero ids). A new `ServerCorrelationSpanProcessor` watches `onStart` for the document-load root span and calls `span.addLink()` with the parsed context. Rationale: links can be attached post-creation (`addLink` is in current OTel JS SDKs — verify the pinned version supports it during implementation; if not, bumping the SDK is preferable to forking `instrumentation-document-load`), whereas _parenting_ requires subclassing the instrumentation — exactly the complexity this change defers. Trace-flags `00` from the server means the linked span will never be exported; a link to a dead span is harmless, unlike a dead parent, which is why link-not-parent is the safe first step.

## Risks / Trade-offs

- [Timing details visible to any caller] → Accepted: trace ids are not secrets and durations are coarse; multi-tenant operators can front SignalDB with a proxy that strips headers until a config knob exists.
- [`addLink` unsupported by the pinned OTel JS version] → Check during implementation; mitigation is an SDK bump (routine) or, worst case, recording the server context as span attributes (`signaldb.server.trace_id`) until then.
- [Two traces instead of one for page loads (link, not parent)] → Deliberate: avoids broken parents under sampling asymmetry. The Explore UI trace view's link rendering determines how discoverable the correlation is — if links render poorly, that's UI work, not a reason to parent prematurely.
- [Header bloat] → ~150 bytes per response; negligible.
- [Middleware double-installed (metrics + trace middleware ordering)] → Header emission lives only in the trace-context middleware; the metrics middleware is untouched, so no duplicate headers.

## Migration Plan

Purely additive response headers; deploy in any order (server first — headers are useful in DevTools with zero client changes; UI consumption no-ops against old servers). Rollback = revert; no state, no schema, no config migration. No WAL/Iceberg impact.

## Open Questions

- Which query-path stages get named `dur` entries first (plan/execute/storage split depends on where the querier already measures) — safely decidable during implementation; only `total` is contractual.
- Whether the acceptor should eventually suppress the headers on OTLP responses to non-browser SDK clients (pure noise there) — harmless either way; uniformity wins for now.
