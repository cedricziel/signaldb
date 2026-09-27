## Why

SignalDB already receives everything a real-user-monitoring (RUM) product
needs: the browser OpenTelemetry SDK emits Web Vitals, navigations, clicks,
resource timing and exceptions as log events, and fetch/XHR spans carry a
`traceparent` into the backend — all stamped with `session.id`. The SignalDB
UI dogfoods exactly this pipeline (see the `frontend-instrumentation` skill).
But nothing in the UI reads it back as RUM: Web Vitals are rows in the Logs
view, a user's session is a `session.id=` filter typed by hand, and the join
from a slow click to the backend span that caused it takes three views.

The Claude Design handoff "Real User Monitoring"
(claude.ai/design project `85b1fe47-5601-4e0d-b208-e00b2fcdb175`,
`Real User Monitoring.html`) adds a **Real users** page that does this: per
frontend app, vitals, sessions, errors, network and interactions, with every
frontend request joined to its backend trace.

## What Changes

- New `/rum` page ("Real users", Monitor group in the sidebar), with an app
  switcher over frontend apps (`service.name` of services that send browser
  or mobile RUM events), the shared time range, and tabs:
  - **Overview** — KPI strip (sessions, users, sessions with errors,
    traced-request share), Core Web Vitals (p75 + good / needs improvement /
    poor share, from `browser.web_vital.rating`), sessions-over-time stacked
    by with/without errors, frontend → backend request table, slowest pages,
    top errors, sessions by browser and device.
  - **Pages** — routes with p75 LCP / INP / CLS / TTFB, views and error
    share; a selected route shows its vitals, load breakdown from
    navigation timing, and the backend calls made from it.
  - **Sessions** — sessions grouped by `session.id` with quick filters
    (with errors, slow load), and a session detail: lane timeline (views,
    actions, network, errors, logs), event list, the backend trace of a
    selected request inline, and the session's resource attributes.
  - **Errors** — exception groups scoped to the app (reusing the
    `explore-ui-errors` grouping), with stack frames and, when the failing
    event followed a failed traced request, that request's backend trace.
  - **Network** — fetch/XHR grouped by method + URL template with p75 split
    into client+network vs backend time, error share, traced share, and a
    warning for origins that never receive `traceparent`; resource timing by
    initiator type.
  - **Interactions** — clicks by target element with INP p75 per page.
  - **Setup** — instrumenting a browser app against SignalDB (install,
    initialize, propagate `traceparent`, CORS), plus a live checklist
    (first session received, vitals seen, requests joined to traces).
- `⌘K` palette gains Real users tabs, frontend apps, and jumping to a pasted
  session id.
- Query IR gains a `count_distinct` aggregate (approximate), needed to count
  sessions and users without returning every group. Additive; IR version bump.
- The UI's own browser telemetry stamps the current route template on log
  records so its own Web Vitals group by page.

Scoped out (follow-up changes):

- Mobile (iOS / Android) and Electron-main vitals — app start, frozen/slow
  frames, ANRs, crash-free rate. The OTel mobile conventions and SDK events
  are not settled; the page detects the platform and shows the browser
  panels only for browser apps, and an honest empty state otherwise.
- Rage/dead clicks, INP phase attribution and long-task attribution — the
  browser SDK (0.7) does not emit them.
- Source-map / dSYM upload, public origin-restricted ingest keys, and an
  error "ignore" list — each is a new API with its own change.
- The prototype's static demo data: the page shows real data or an empty
  state, never fixtures (fixtures live in stories only).

Not breaking: no ingest, Flight, WAL or Iceberg change; compatibility APIs
untouched; `count_distinct` is an additive IR aggregate.

## Capabilities

### New Capabilities

- `explore-ui-rum`: the Real users page and its tabs.
- `ui-browser-telemetry`: route template on the UI's own RUM log records.

### Modified Capabilities

- `query-ir-core`: `count_distinct` aggregate function.
- `explore-ui-navigation`: `/rum` route and sidebar entry.

## Impact

- **querier** (IR lowering of `count_distinct` to DataFusion
  `approx_distinct`), **common** (IR types / validation), **router**
  (OpenAPI schema for the new aggregate `fn`).
- **src/ui**: `features/rum/*`, `api/rum.ts`, `features/shell/navModel.ts`,
  `CommandPalette`, `routes.tsx`, `telemetry/*`, generated client regen,
  design-sync page registration.
- **signaldb-sdk** regenerated for the OpenAPI change; no new CLI command —
  the CLI already runs IR documents, and a RUM CLI view is out of scope.
- Docs: `docs/users/explore-ui.md` (Real users section),
  `docs/users/querying-ir.md` (`count_distinct`), a user guide for
  instrumenting a browser app; `frontend-instrumentation` skill.
