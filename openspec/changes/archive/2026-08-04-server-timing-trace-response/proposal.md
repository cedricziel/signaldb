# Proposal: server-timing-trace-response

## Why

Trace context currently flows only client → server: the browser UI injects `traceparent` into API calls, but the server's SERVER span context dies with the response. This leaves the one request the client can never instrument — the initial document load, where no JS has run yet — as a detached trace, and gives users no way to see a request's trace id or server-side timing without opening a trace viewer. Returning trace context and stage timings on every HTTP response closes the loop: one coherent page-load trace, DevTools-visible server timings, and a correlation fallback when proxies strip the request `traceparent`.

## What Changes

- The shared HTTP middleware in `common` (used by every service with an HTTP surface — router and acceptor today, automatically any future one) emits on each response:
  - `Server-Timing: traceparent;desc="00-<trace-id>-<span-id>-<flags>"` — the de-facto RUM back-channel, readable by browsers via the Performance API even for document/resource requests.
  - `traceresponse: 00-<trace-id>-<span-id>-<flags>` — the W3C Trace Context Level 2 header, for forward compatibility with standard-compliant clients.
  - Additional `Server-Timing` entries with `dur=` values for server-side stage timings (at minimum `total`; query endpoints may add stages such as plan/execute/storage as instrumentation allows).
- Guards: no headers when the span context is invalid (self-monitoring disabled); the existing `_system` tenant bypass is preserved (no span → no headers).
- `Timing-Allow-Origin` support so cross-origin consumers (e.g. the Grafana plugin frontend) can read `serverTiming` performance entries.
- The browser UI (`src/ui`) consumes the navigation performance entry's `serverTiming` to correlate its `documentLoad` trace with the server's document-serving span — initially as a span link (graceful under sampling asymmetry), with parenting as a possible follow-up once sampled-flag handling is settled.

## Capabilities

### New Capabilities

- `http-response-trace-context`: every SignalDB HTTP response carries W3C trace context (`Server-Timing: traceparent` + `traceresponse`) and server-side stage timings (`Server-Timing` `dur=` entries), with guards for invalid contexts, self-monitoring bypass, and cross-origin readability.
- `ui-server-correlation`: the browser UI correlates client-side traces with server-returned trace context, starting with the document-load ↔ document-request join via the navigation entry's `serverTiming`.

### Modified Capabilities

<!-- none — no existing spec covers HTTP response headers or browser telemetry -->

## Impact

- **common**: `self_monitoring/app_metrics.rs` (`http_trace_context_middleware` mints the SERVER span and is where response headers are added); `flight/trace_context.rs` (existing traceparent formatting/injection idioms to reuse).
- **router**, **acceptor**: pick up the new headers automatically via the shared middleware; no per-service code expected. Querier/writer are Flight-only (gRPC) and out of scope.
- **ui** (`src/ui/src/telemetry/`): new consumption of `PerformanceNavigationTiming.serverTiming`; span-link processor.
- **grafana-plugin**: benefits from `Timing-Allow-Origin` (read-only consumer; no code change required in this change).
- Not BREAKING: additive response headers only; no OTLP ingest, query surface, Flight schema, or on-disk layout changes.
