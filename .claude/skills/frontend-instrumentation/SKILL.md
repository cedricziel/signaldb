---
name: frontend-instrumentation
description: Best practices for instrumenting the SignalDB browser UI with OpenTelemetry - end-to-end trace correlation, RUM sessions, context propagation, exporter config, and manual spans. Use when working on src/ui telemetry, browser tracing, session tracking, or frontend-to-backend correlation.
user-invocable: false
sources:
  - src/ui/src/telemetry/**
  - src/ui/vite.config.ts
  - src/ui/.env.example
---

# Frontend Instrumentation

The SignalDB UI (`src/ui`) is instrumented with **vanilla OpenTelemetry** — the
official browser SDK, no vendor RUM SDK. It lives in `src/ui/src/telemetry/`
and is initialised once from `main.tsx`. Two outcomes drive the design:

1. **End-to-end correlation** — the auto fetch/XHR instrumentation injects a
   W3C `traceparent` into every same-origin API call (`/tempo`, `/loki`,
   `/prometheus`, `/api`, `/ui/session`), so a user action and the querier
   spans it triggers share one trace.
2. **Followable sessions** — every span carries `session.id` plus the active
   `tenant.id` / `dataset.id`, so all activity from one browser session can be
   grouped and lined up against the backend's per-tenant traces.
3. **Server → client correlation** — for the one request the client can never
   instrument (the initial HTML document), the server's trace context is read
   back from the response's `Server-Timing: traceparent` entry and linked to
   the `documentLoad` span.

## Module map

| File                                          | Responsibility                                                                                                                                                                |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry/index.ts`                          | `initTelemetry()` — provider, context manager, propagators, auto-instrumentations, exporter selection, error capture; exports `tracer`                                        |
| `telemetry/session.ts`                        | `createSessionManager()` — RUM session id with sliding inactivity window + absolute cap, `localStorage`-backed                                                                |
| `telemetry/sessionSpanProcessor.ts`           | `SpanProcessor` that stamps `session.id` / `tenant.id` / `dataset.id` on every span                                                                                           |
| `telemetry/navigationSpanProcessor.ts`        | `SpanProcessor` that collapses the auto-instrumentation's `Navigation: <url>` span to the static name `Navigation`, moving the URL into `url.full` / `url.path` / `url.query` |
| `telemetry/serverTiming.ts`                   | Strict parser for the `Server-Timing: traceparent` context SignalDB returns on every HTTP response (see `docs/users/response-trace-context.md`)                               |
| `telemetry/serverCorrelationSpanProcessor.ts` | `SpanProcessor` that links the `documentLoad` span to the server span that served the document, via the navigation entry's `serverTiming`                                     |

## Rules

### Initialise before app code

`initTelemetry()` runs at the top of `main.tsx`, before `createRoot`. The
instrumentation monkey-patches `fetch`/`XHR`, so it must be in place before the
app issues its first request or early calls go untraced. Keep it idempotent and
browser-guarded (`typeof window === "undefined"` bails) so importing it can
never break SSR or tests.

### Never broaden `propagateTraceHeaderCorsUrls`

All API calls are **same-origin** (the router serves the UI and proxies the
API; the Vite dev server proxies in dev). Same-origin requests get `traceparent`
**automatically** — no config needed. Do **not** add a broad
`propagateTraceHeaderCorsUrls` regex (e.g. `/.*/`): that would leak trace
headers to third-party origins and can trip their CORS. Only add a specific
origin here if the UI ever calls a genuinely cross-origin SignalDB API.

### Sessions are stamped per-span, not on the Resource

`session.id`, `tenant.id`, and `dataset.id` all change during a page's life —
the session can roll after inactivity, and the user can switch tenant/dataset
without a reload. So they are set at `onStart` by `SessionSpanProcessor`, not
baked into the immutable `Resource`. The Resource holds only stable facts
(`service.name`, `service.version`, `browser.*`).

Session lifetime follows the common RUM convention: a new session starts after
**4h of inactivity** or at a **24h absolute cap**, whichever first. Each span
start counts as activity and slides the inactivity window. The id persists in
`localStorage` so it survives reloads and spans across tabs; it falls back to an
in-memory id when storage is unavailable (private mode, sandboxed iframe).

### Anything the browser exporter carries is world-readable

The OTLP exporter URL and any headers are visible to anyone who can load the UI
— whether baked in at build time or served at runtime. Export is opt-in either
way: with no endpoint the SDK still runs (so `traceparent` propagation works)
and, in dev, prints spans to the console.

Two ways to configure the export target, in precedence order:

1. **Runtime config (preferred)** — `[self_monitoring.frontend]` in the
   SignalDB config. The router serves it to the browser via
   `GET /runtime-config.js` (see `resolveExportConfig` in
   `telemetry/runtimeConfig.ts`), so one image serves every deployment with no
   rebuild. When `api_key` is set it is delivered to the browser and sent as
   `Authorization: Bearer` on cross-origin exports to the acceptor (whose
   `[self_monitoring.frontend].allowed_origins` drives the CORS layer). This
   **deliberately** puts an ingest key in the browser — only acceptable on a
   trusted network, and the key **must be ingest-only**, scoped to
   `tenant_id`, never an admin key.
2. **Build-time `SIGNALDB_OTLP_ENDPOINT`** — baked into the bundle, no headers.
   Used as a fallback for local dev or a collector that needs no auth.

The most defensive option remains an **OTLP collector you control** that adds
auth/tenant headers, scrubs PII, and rate-limits — point either mechanism at
it instead of straight at a public acceptor when the UI is internet-facing.

### `ZoneContextManager` needs `zone.js` as a direct dependency

`ZoneContextManager` keeps the active span alive across async boundaries
(promises, timers, event handlers) so child spans nest under the interaction
that caused them. It relies on `zone.js`, which pnpm's strict layout will not
let you import unless it is a **direct** dependency of `src/ui` — it is listed
in `package.json` for exactly this reason. `zone.js` patches global async
primitives, so `telemetry/index.ts` (which imports it) must only ever be
imported from `main.tsx`, never from test or library code.

### Capture errors explicitly

Raw browser OTel does **not** capture uncaught errors. `initTelemetry()` adds
`window` `error` and `unhandledrejection` listeners that record the exception on
a short span. Extend this rather than reaching for a vendor RUM SDK.

### Manual spans

Import `tracer` from `telemetry` for user-meaningful operations that aren't a
single fetch (multi-step flows, expensive client work). Always `end()` in a
`finally`, `recordException` + set `ERROR` status on failure, and keep span
names **low-cardinality** (no ids/timestamps in the name — put those in
attributes). The web auto-instrumentation's route span otherwise names itself
after the full URL; `navigationSpanProcessor.ts` rewrites it to enforce this.

### Server-returned context: link, never parent

The document request goes out before any JS runs, so it cannot carry
`traceparent`. SignalDB returns its server span's context on every response
(`Server-Timing: traceparent;desc="..."` + `traceresponse`; see
`docs/users/response-trace-context.md`), and
`serverCorrelationSpanProcessor.ts` reads it off the navigation performance
entry to attach it to the `documentLoad` span **as a span link** — never as a
parent. If the server sampled its span out (flags `00`) a parent would point
at a span that is never exported and dangle the client trace; a link to an
unexported span is harmless. Parsing is strict (version `00`, lowercase hex,
exact widths, non-zero ids) and the whole path is best-effort: any failure
degrades to "no link", never an error. Fetch/XHR calls do **not** need this —
they already root the trace via the request-side `traceparent`.

## Backend must continue the trace

Frontend propagation only pays off if the backend **extracts** the incoming
`traceparent` and continues the trace. Injecting the header is necessary but not
sufficient for true end-to-end — confirm the router/querier parse W3C trace
context on inbound HTTP. If they don't yet, browser traces and backend traces
stay disconnected even though the header is present. The reverse direction is
covered too: the shared HTTP middleware returns the server span's context on
every response, which is what the document-load correlation above consumes.

## Testing

Unit-test the pure logic (`session.ts`, `sessionSpanProcessor.ts`,
`navigationSpanProcessor.ts`, `serverTiming.ts`,
`serverCorrelationSpanProcessor.ts`, `runtimeConfig.ts`) with injected
clock/storage/id/entry providers — see the `.test.ts` files.
Do **not** import `telemetry/index.ts` from tests: it pulls in `zone.js` and
patches globals. The SDK wiring is validated by `pnpm --filter signaldb-ui
build`. For the same reason it's excluded from `vite.config.ts`'s coverage
`thresholds` (80% lines/statements/functions/branches, CI-enforced) — don't
remove the exclusion to chase the number; write injectable-provider unit
tests for the underlying logic instead, as above.

## Configuration

### Runtime (preferred): `[self_monitoring.frontend]`

Set in the SignalDB config file; the router serves it to the browser at
`GET /runtime-config.js` (`window.__SIGNALDB_RUNTIME_CONFIG__`), which
`index.html` loads as a blocking classic script before the app boots.

| Key               | Default       | Meaning                                                                   |
| ----------------- | ------------- | ------------------------------------------------------------------------- |
| `enabled`         | `false`       | Export browser spans (propagation works regardless).                      |
| `endpoint`        | _(empty)_     | OTLP/HTTP base URL the browser posts to. `/v1/traces` appended if absent. |
| `api_key`         | _(none)_      | Ingest key → `Authorization: Bearer`. World-readable; ingest-only.        |
| `tenant_id`       | `_system`     | → `X-Tenant-ID` on exports.                                               |
| `dataset_id`      | `_monitoring` | → `X-Dataset-ID` on exports.                                              |
| `service_name`    | `signaldb-ui` | `service.name` on exported spans.                                         |
| `allowed_origins` | _(any)_       | Acceptor CORS allow-list for browser exports; empty allows any origin.    |

### Build-time env (fallback)

Via `vite.config.ts` `define`; `SIGNALDB_`-prefixed. Used only when no runtime
config is present (local dev, or a collector needing no auth).

| Env                               | Default       | Meaning                                                                                                                    |
| --------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `SIGNALDB_OTLP_ENDPOINT`          | _(empty)_     | OTLP/HTTP endpoint for browser spans; empty disables export (propagation still works). `/v1/traces` is appended if absent. |
| `SIGNALDB_TELEMETRY_SERVICE_NAME` | `signaldb-ui` | `service.name` on exported spans (runtime `service_name` wins when set)                                                    |

`service.version` is taken from `package.json` at build time.

## Semantic conventions used

- `service.name`, `service.version` — resource identity (stable conventions).
- `session.id` — RUM session (incubating convention).
- `tenant.id`, `dataset.id` — SignalDB multi-tenant context (custom; mirrors the
  `X-Tenant-ID` / `X-Dataset-ID` request headers). These are not yet stable
  OTel conventions — expect churn as the Client Instrumentation SIG settles RUM
  semantics.
