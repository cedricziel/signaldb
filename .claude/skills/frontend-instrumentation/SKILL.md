---
name: frontend-instrumentation
description: Best practices for instrumenting the SignalDB browser UI with OpenTelemetry - end-to-end trace correlation, RUM sessions, context propagation, exporter config, and manual spans - plus the UI-wide rule that every visualization panel shows its data through the shared VizTooltip. Use when working on src/ui telemetry, browser tracing, session tracking, frontend-to-backend correlation, or adding a chart/panel to the UI.
user-invocable: false
---

# Frontend Instrumentation

The SignalDB UI (`src/ui`) is instrumented with **vanilla OpenTelemetry** — the
official browser SDK, no vendor RUM SDK. It lives in `src/ui/src/telemetry/`
and is initialised once from `main.tsx`. Two signal types, both wired from
`initTelemetry()` in `index.ts`:

- **Spans** (`index.ts` and its span processors) — request/interaction traces,
  same as always.
- **Log records** (`logs.ts`, called at the end of `initTelemetry()`) —
  event-based telemetry from
  [`@opentelemetry/browser-instrumentation`](https://github.com/open-telemetry/opentelemetry-browser)
  (Web Vitals, navigation/resource timing, route changes, console, uncaught
  errors). Complementary, not a replacement SDK — it needs its own
  `LoggerProvider`/`@opentelemetry/sdk-logs` pipeline alongside the trace one.

Design outcomes:

1. **End-to-end correlation** — the auto fetch/XHR instrumentation injects a
   W3C `traceparent` into every same-origin API call (`/tempo`, `/loki`,
   `/prometheus`, `/api`, `/ui/session`), so a user action and the querier
   spans it triggers share one trace.
2. **Followable sessions** — every span _and log record_ carries `session.id`
   plus the active `tenant.id` / `dataset.id`, so all activity from one
   browser session can be grouped and lined up against the backend's
   per-tenant traces.
3. **Server → client correlation** — for the one request the client can never
   instrument (the initial HTML document), the server's trace context is read
   from a `<meta name="traceparent">` tag the router injects into `index.html`
   and used as the **real parent** of the `documentLoad` span; a
   `Server-Timing: traceparent` response-header link is the fallback when the
   tag is absent.
4. **Deployment identity on the Resource** — `service.namespace`,
   `signaldb.server.version` (the _backend's_ build, not the UI bundle's), and
   `deployment.environment.name` all come from the router-injected runtime
   config, mirroring the backend's own `common::self_monitoring::build_resource`
   facts (see `resource.ts`).

## Module map

| File                                          | Responsibility                                                                                                                                                                        |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `telemetry/index.ts`                          | `initTelemetry()` — trace provider, context manager, propagators, span auto-instrumentations, exporter selection; calls `initBrowserLogs()`; exports `tracer`                         |
| `telemetry/logs.ts`                           | `initBrowserLogs()` — `LoggerProvider`, log exporter selection, event-based instrumentations (Web Vitals, timing, navigation, errors, console)                                        |
| `telemetry/resource.ts`                       | Shared `SERVICE_NAME`/`SERVICE_VERSION`/`RUNTIME_CONFIG` and `buildResource()`, used by both providers so traces and logs carry identical resource attributes                         |
| `telemetry/session.ts`                        | `createSessionManager()` — RUM session id with sliding inactivity window + absolute cap, `localStorage`-backed                                                                        |
| `telemetry/sessionSpanProcessor.ts`           | `SpanProcessor` that stamps `session.id` / `tenant.id` / `dataset.id` on every span                                                                                                   |
| `telemetry/sessionLogRecordProcessor.ts`      | `LogRecordProcessor` counterpart — same three attributes, on every log record                                                                                                         |
| `telemetry/navigationSpanProcessor.ts`        | `SpanProcessor` that collapses the auto-instrumentation's `Navigation: <url>` span to the static name `Navigation`, moving the URL into `url.full` / `url.path` / `url.query`         |
| `telemetry/sanitizeNavigationUrl.ts`          | `sanitizeUrl` hook for the log-based `NavigationInstrumentation` — strips userinfo credentials and redacts known-sensitive query params before a URL reaches a log record             |
| `telemetry/serverTiming.ts`                   | Shared `parseTraceparent()` plus a `Server-Timing`-entry-specific wrapper, for the trace context SignalDB returns on every HTTP response (see `docs/users/response-trace-context.md`) |
| `telemetry/documentTraceContext.ts`           | Reads `<meta name="traceparent">` and builds the real parent `Context` for the `documentLoad` span, read _before_ that span is created                                                |
| `telemetry/serverCorrelationSpanProcessor.ts` | `SpanProcessor` that **links** (never parents) `documentLoad` to the server span via the navigation entry's `serverTiming` — the fallback when the meta tag is absent                 |

## Event-based instrumentation (`logs.ts`)

`@opentelemetry/browser-instrumentation` ships several instrumentations under
`./experimental/*` subpaths; not all are enabled, because several duplicate a
span-based signal we already emit:

| Instrumentation                     | Enabled?                                   | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ----------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Web Vitals                          | Yes                                        | No existing equivalent — Core Web Vitals (LCP, CLS, INP, FCP, TTFB) as log records.                                                                                                                                                                                                                                                                                                                                                                        |
| Navigation Timing / Resource Timing | Yes                                        | No existing equivalent at this granularity; `documentLoad`'s span events are coarser. `ResourceTimingInstrumentation` is configured with `ignoreUrls: [/\/v1\/traces$/, /\/v1\/logs$/]` so it doesn't report on its own telemetry export requests.                                                                                                                                                                                                         |
| Navigation                          | Yes, **alongside** the span-based collapse | Emits a dedicated `browser.navigation` log event with `sanitizeUrl` redaction (see `sanitizeNavigationUrl.ts`). Does **not** replace `navigationSpanProcessor.ts`: `instrumentation-user-interaction` renames the _active click span_ to `Navigation: <url>` on any `history.pushState`/`replaceState` call, with no config to disable just that behavior while keeping click spans — so both signals coexist rather than one cleanly replacing the other. |
| Errors                              | Yes, **replaces** `installErrorCapture()`  | Same `window` `error`/`unhandledrejection` listeners, now as log-record `exception` events instead of hand-rolled `browser.error`/`browser.unhandledrejection` spans. **Deliberate signal change** — anything that grouped the Explore UI's Traces view by the `browser.error` span name (e.g. a saved trace-group view) stops seeing new entries; browser errors now show up as logs instead.                                                             |
| Console                             | Yes, **scoped down**                       | `logMethods: ["error", "warn"]` only — not the package's `log`/`warn`/`error`/`info`/`debug` default. Full console capture ships stack dumps, debug output, and potentially accidentally-logged tokens/PII to the backend; scoping to error/warn only keeps the signal (surfaced console errors) while cutting that risk substantially. Widen deliberately, not by accident.                                                                               |
| Fetch                               | **No**                                     | Straight duplicate of the existing span-based `@opentelemetry/instrumentation-fetch` (via `getWebAutoInstrumentations()`) — would double-instrument every request.                                                                                                                                                                                                                                                                                         |
| User Action                         | **No**                                     | Duplicates `instrumentation-user-interaction`'s click spans.                                                                                                                                                                                                                                                                                                                                                                                               |

All instrumentations are registered with `loggerProvider: provider` (the
`LoggerProvider` built in `initBrowserLogs()`, not the global one implicitly)
so they pick up `SessionLogRecordProcessor`'s stamping and the configured
exporter.

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

### Sessions are stamped per-span/per-log-record, not on the Resource

`session.id`, `tenant.id`, and `dataset.id` all change during a page's life —
the session can roll after inactivity, and the user can switch tenant/dataset
without a reload. So they are set at `onStart`/`onEmit` by
`SessionSpanProcessor`/`SessionLogRecordProcessor`, not baked into the
immutable `Resource`. The Resource holds only stable facts (`service.name`,
`service.version`, `service.namespace`, `signaldb.server.version`,
`deployment.environment.name`, `browser.*`) — see `resource.ts`.

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

This same async-boundary propagation is what makes the meta-tag → real-parent
mechanism above work: `context.with(documentTraceParentContext(), ...)` sets
the active context synchronously, and `ZoneContextManager` carries it into the
`window` `load` event handler `DocumentLoadInstrumentation` registers inside
that callback, even though the event fires much later.

### Errors are captured as log records, not spans

Uncaught errors and unhandled rejections are captured by `ErrorsInstrumentation`
in `logs.ts` (see the table above), not hand-rolled `window` listeners — raw
browser OTel does not capture these on its own, so _something_ has to install
the listeners, and this package's instrumentation does it as log-record
`exception` events.

### Manual spans

Import `tracer` from `telemetry` for user-meaningful operations that aren't a
single fetch (multi-step flows, expensive client work). Always `end()` in a
`finally`, `recordException` + set `ERROR` status on failure, and keep span
names **low-cardinality** (no ids/timestamps in the name — put those in
attributes). The web auto-instrumentation's route span otherwise names itself
after the full URL; `navigationSpanProcessor.ts` rewrites it to enforce this.

### Server-returned context: real parent from the meta tag, link as fallback

The document request goes out before any JS runs, so it cannot carry an
outbound `traceparent`. SignalDB closes the loop two ways:

1. **`<meta name="traceparent">` → real parent (primary).** The router
   (`serve_index_html` in `src/router/src/ui.rs`) injects the server span's
   context directly into `index.html`'s `<head>`. This is readable
   _synchronously_, before any span exists, so `initTelemetry()` uses it as
   the actual OTel parent of `documentLoad`: `DocumentLoadInstrumentation` is
   constructed and registered separately from the rest of
   `getWebAutoInstrumentations()` (which has it disabled), wrapped in
   `context.with(documentTraceParentContext(), ...)`. This works _because_
   `DocumentLoadInstrumentation` starts its root span with no explicit
   context — it resolves `context.active()` inside the `window`'s `load`
   event handler it installs during `enable()`, and `ZoneContextManager`
   (see below) carries the context active at registration time into that
   later async callback.

   This is a **deliberate trade-off**, not an oversight: OpenTelemetry JS's
   default sampler is `ParentBasedSampler`, which drops a span outright when
   its parent was sampled out (`traceparent` flags `00`) — so whenever
   SignalDB's self-monitoring sampler ratio drops the server's root span, the
   entire `documentLoad` subtree silently stops being recorded too, not just
   a dangling reference. Parenting was chosen anyway for the structural
   parent-child edge it gives on the common path; see
   `docs/users/response-trace-context.md#trace-context-in-the-document-body`
   for the full rationale.

2. **`Server-Timing: traceparent;desc="..."` header → link (fallback).**
   Every SignalDB HTTP response, including `index.html` when it predates the
   meta tag (older router, or a dev proxy serving the file directly) or when
   self-monitoring's active-span check found nothing to inject, still carries
   this header (see `docs/users/response-trace-context.md`).
   `serverCorrelationSpanProcessor.ts` reads it off the navigation performance
   entry and attaches it to the already-created `documentLoad` span as a
   **link**, never a parent — a `SpanProcessor.onStart` hook fires _after_ the
   span exists and cannot change its parent, and a link to a possibly-
   unexported span is harmless where a parent would not be.

`parseTraceparent()` in `serverTiming.ts` backs both paths (strict: version
`00`, lowercase hex, exact widths, non-zero ids) and both are best-effort:
any failure (missing tag, malformed value, DOM/Performance API oddity)
degrades silently to "no parent" / "no link", never an error. Fetch/XHR calls
do **not** need any of this — they already root the trace via the
request-side `traceparent`.

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
`sessionLogRecordProcessor.ts`, `navigationSpanProcessor.ts`,
`sanitizeNavigationUrl.ts`, `serverTiming.ts`,
`serverCorrelationSpanProcessor.ts`, `documentTraceContext.ts`,
`resource.ts`, `runtimeConfig.ts`) with injected clock/storage/id/entry
providers — see the `.test.ts` files. `buildResource()` in particular takes
its `RuntimeConfig` as an injectable last parameter (defaulting to the module
constant) for exactly this reason.

Do **not** import `telemetry/index.ts` from tests: it pulls in `zone.js` and
patches globals. `logs.ts` is import-safe (no `zone.js`), but is still thin
SDK-wiring glue like `index.ts` — the same "test the pure logic it calls into,
not the wiring itself" split applies. The SDK wiring is validated by
`pnpm --filter signaldb-ui build`. For the same reason `index.ts`/`logs.ts`
are excluded from `vite.config.ts`'s coverage `thresholds` (80%
lines/statements/functions/branches, CI-enforced) — don't remove the
exclusion to chase the number; write injectable-provider unit tests for the
underlying logic instead, as above.

## Configuration

### Runtime (preferred): `[self_monitoring.frontend]` + deployment identity

Set in the SignalDB config file; the router serves it to the browser at
`GET /runtime-config.js` (`window.__SIGNALDB_RUNTIME_CONFIG__`), which
`index.html` loads as a blocking classic script before the app boots.

| Key                     | Source                          | Default       | Meaning                                                                                                                                                      |
| ----------------------- | ------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `enabled`               | `[self_monitoring.frontend]`    | `false`       | Export browser spans/logs (propagation works regardless).                                                                                                    |
| `endpoint`              | `[self_monitoring.frontend]`    | _(empty)_     | OTLP/HTTP base URL the browser posts to. `/v1/traces` or `/v1/logs` appended if absent.                                                                      |
| `api_key`               | `[self_monitoring.frontend]`    | _(none)_      | Ingest key → `Authorization: Bearer`. World-readable; ingest-only.                                                                                           |
| `tenant_id`             | `[self_monitoring.frontend]`    | `_system`     | → `X-Tenant-ID` on exports.                                                                                                                                  |
| `dataset_id`            | `[self_monitoring.frontend]`    | `_monitoring` | → `X-Dataset-ID` on exports.                                                                                                                                 |
| `service_name`          | `[self_monitoring.frontend]`    | `signaldb-ui` | `service.name` on exported spans/logs.                                                                                                                       |
| `allowed_origins`       | `[self_monitoring.frontend]`    | _(any)_       | Acceptor CORS allow-list for browser exports; empty allows any origin.                                                                                       |
| `namespace`             | hardcoded `"signaldb"`          | —             | → `service.namespace`. Always present regardless of `enabled`.                                                                                               |
| `version`               | `env!("CARGO_PKG_VERSION")`     | —             | The **router's own build version** → `signaldb.server.version` (not `service.version`, which stays the UI bundle's own — see `resource.ts`). Always present. |
| `deploymentEnvironment` | `[self_monitoring].environment` | `production`  | → `deployment.environment.name`. Always present.                                                                                                             |

The last three are deployment-identity facts, not export credentials, so —
unlike `endpoint`/`apiKey`/etc. — they're emitted in `runtime_config_js`
(`src/router/src/ui.rs`) regardless of whether `enabled` is true.

### Build-time env (fallback)

Via `vite.config.ts` `define`; `SIGNALDB_`-prefixed. Used only when no runtime
config is present (local dev, or a collector needing no auth).

| Env                               | Default       | Meaning                                                                                                                    |
| --------------------------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `SIGNALDB_OTLP_ENDPOINT`          | _(empty)_     | OTLP/HTTP endpoint for browser spans; empty disables export (propagation still works). `/v1/traces` is appended if absent. |
| `SIGNALDB_TELEMETRY_SERVICE_NAME` | `signaldb-ui` | `service.name` on exported spans (runtime `service_name` wins when set)                                                    |

`service.version` is taken from `package.json` at build time.

## Visualization panels: every viz panel uses `VizTooltip`

Not telemetry, but the one UI-wide rule with no better home: every panel that
draws data (time-series, histogram, area, heatmap, sparkline, breakdown bar,
flame graph, and anything added later) shows a hover/focus tooltip through the
shared primitive in `src/ui/src/components/VizTooltip.tsx` — a panel that
draws its own tooltip markup is a defect
(`openspec/specs/explore-ui-viz-tooltips`).

- The panel resolves pointer → datum itself (bar index, heatmap cell, nearest
  timestamp) and hands `VizTooltip` a title (the exact x: timestamp at the
  panel's resolution, bucket range, or category), one row per series
  (`swatch`, `label`, `value`; `muted` + `–` for a gap, never a dropped row),
  and a `footer` for the aggregate (total, count, share).
- Host is `position: relative` (`className="viz-host"`); use
  `useVizPointer(hostRef)` for SVG/DOM panels (`track` on `onPointerMove`,
  `anchorTo` on `onFocus`, `clear` on leave/blur). uPlot panels read
  `u.cursor.idx` in a `setCursor` hook (see `MetricsChart.rowsForCursorIndex`).
- Format through `src/ui/src/lib/vizFormat.ts` (`formatTimestamp`,
  `formatTimeBucket`, `formatValue`, `formatRange`, `formatShare`,
  `compactCount`), not ad-hoc `toFixed`/`Intl` calls.
- Data marks that can take focus (bars, cells, segments) get `tabIndex={0}`
  and `aria-describedby` pointing at the tooltip `id` while active; focus sets
  the same "active datum" state as hover. Empty marks show no tooltip.
- Tests assert tooltip _content_ after `fireEvent.pointerMove`/`focus` on the
  mark (jsdom does no layout); for uPlot, test the pure row resolver.

## Semantic conventions used

- `service.name`, `service.version`, `service.namespace`,
  `deployment.environment.name` — resource identity (stable conventions).
  `service.namespace`/`deployment.environment.name` are only present when the
  router injected them (see Configuration above) — omitted, not left
  `undefined`, otherwise.
- `session.id` — RUM session (incubating convention).
- `tenant.id`, `dataset.id` — SignalDB multi-tenant context (custom; mirrors the
  `X-Tenant-ID` / `X-Dataset-ID` request headers). These are not yet stable
  OTel conventions — expect churn as the Client Instrumentation SIG settles RUM
  semantics.
- `signaldb.server.version` — custom, `signaldb.*`-namespaced: the backend
  build that served this session, as opposed to `service.version` (the UI
  bundle's own version). Follows the same "dotted custom attribute, not yet
  a stable convention" pattern as `tenant.id`/`dataset.id`.
