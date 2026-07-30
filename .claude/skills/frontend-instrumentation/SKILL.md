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

## Module map

| File                                | Responsibility                                                                                                                         |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry/index.ts`                | `initTelemetry()` — provider, context manager, propagators, auto-instrumentations, exporter selection, error capture; exports `tracer` |
| `telemetry/session.ts`              | `createSessionManager()` — RUM session id with sliding inactivity window + absolute cap, `localStorage`-backed                         |
| `telemetry/sessionSpanProcessor.ts` | `SpanProcessor` that stamps `session.id` / `tenant.id` / `dataset.id` on every span                                                    |

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

### Keep secrets out of the browser

The OTLP exporter URL and any headers ship in the bundle and are world-readable.
**Never** put an API key or `X-Tenant-ID` on the browser exporter. To send
browser spans to SignalDB, point `SIGNALDB_OTLP_ENDPOINT` at an **OTLP collector
you control** that adds auth/tenant headers, scrubs PII, and rate-limits —
never straight at a public acceptor. Export is opt-in: with no endpoint the SDK
still runs (so `traceparent` propagation works) and, in dev, prints spans to the
console.

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
attributes).

## Backend must continue the trace

Frontend propagation only pays off if the backend **extracts** the incoming
`traceparent` and continues the trace. Injecting the header is necessary but not
sufficient for true end-to-end — confirm the router/querier parse W3C trace
context on inbound HTTP. If they don't yet, browser traces and backend traces
stay disconnected even though the header is present.

## Testing

Unit-test the pure logic (`session.ts`, `sessionSpanProcessor.ts`) with injected
clock/storage/id — see the `.test.ts` files. Do **not** import
`telemetry/index.ts` from tests: it pulls in `zone.js` and patches globals. The
SDK wiring is validated by `pnpm --filter signaldb-ui build`.

## Configuration

Build-time env (via `vite.config.ts` `define`; `SIGNALDB_`-prefixed like the
rest of the UI):

| Env                               | Default       | Meaning                                                                                                                    |
| --------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `SIGNALDB_OTLP_ENDPOINT`          | _(empty)_     | OTLP/HTTP endpoint for browser spans; empty disables export (propagation still works). `/v1/traces` is appended if absent. |
| `SIGNALDB_TELEMETRY_SERVICE_NAME` | `signaldb-ui` | `service.name` on exported spans                                                                                           |

`service.version` is taken from `package.json` at build time.

## Semantic conventions used

- `service.name`, `service.version` — resource identity (stable conventions).
- `session.id` — RUM session (incubating convention).
- `tenant.id`, `dataset.id` — SignalDB multi-tenant context (custom; mirrors the
  `X-Tenant-ID` / `X-Dataset-ID` request headers). These are not yet stable
  OTel conventions — expect churn as the Client Instrumentation SIG settles RUM
  semantics.
