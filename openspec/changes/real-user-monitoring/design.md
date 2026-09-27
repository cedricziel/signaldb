## Context

See proposal.md — Why. The page ports the Claude Design prototype
`Real User Monitoring.html` (project `85b1fe47-5601-4e0d-b208-e00b2fcdb175`:
`rum/overview.jsx`, `perf.jsx`, `sessions.jsx`, `errors-setup.jsx`,
`palette.jsx`, `ui.jsx`, `data.js`) into `src/ui`. The prototype runs on
static data; this change keeps its layout and interaction model and feeds it
from the Query IR.

Facts that shape the approach, checked against the `_system/_monitoring`
dataset on the hive deployment (the SignalDB UI's own telemetry):

- RUM data is plain OTel, no special table. Log records from
  `@opentelemetry/browser-instrumentation` 0.7 carry `event_name`
  (`browser.web_vital`, `browser.navigation`, `browser.navigation_timing`,
  `browser.resource_timing`, `exception`, `browser.console`) and
  `session.id`; `tenant.id` is the UI's own addition.
- `browser.web_vital.name` is **lowercase** (`lcp`, `inp`, `cls`, `fcp`,
  `ttfb`); `browser.web_vital.value` is in **ms** (LCP `1809`), unitless for
  CLS; `browser.web_vital.rating` is `good` / `needs-improvement` / `poor`.
- Vital records carry **no page URL**. Grouping vitals by page needs the
  emitter to stamp one; for the UI itself this change adds `url.template`
  (see `ui-browser-telemetry`). For third-party apps the Setup tab documents
  the same processor.
- The resource carries `browser.mobile`, `browser.language`,
  `telemetry.sdk.language = webjs` — but no `browser.brands` or
  `user_agent.original`, so a by-browser breakdown needs those added.
- Spans: fetch/XHR client spans are named by method (`POST`, `GET`), clicks
  are `click` spans from `instrumentation-user-interaction`, page loads are
  `documentLoad` / `documentFetch` / `resourceFetch`. Fetch spans propagate
  `traceparent`, so the router/querier spans are children in the same trace.
- The IR has `quantile` (approx_percentile_cont), scoped aggregates, and the
  v8 `correlate` stage (span ↔ parent join). It has **no distinct count**:
  counting sessions today means grouping by `session.id` and counting rows
  client-side, which is unbounded.

## Goals / Non-Goals

**Goals:**

- The prototype's Overview, Pages, Sessions (+ detail), Errors, Network,
  Interactions and Setup tabs, for browser apps, on real data.
- Every figure is one or a few bounded IR reads; nothing reads a compat API.
- The SignalDB UI becomes a complete RUM source for itself (dogfood), so the
  page is meaningful on every deployment from day one.

**Non-Goals:**

- Mobile/Electron-main vitals, rage/dead clicks, INP phase and long-task
  attribution, source-map upload, origin-restricted public keys, error
  ignore lists (see proposal — Scoped out).
- A new RUM ingest path or table. The FDAP version-alignment constraint is
  unaffected: the only Rust change is an IR aggregate lowered onto
  DataFusion's own `approx_distinct`, using the Arrow types DataFusion
  re-exports. No Flight v1 / v2 storage schema transform, WAL or Iceberg
  migration, so no rollback plan beyond reverting the UI and IR code.

## Decisions

### 1. `count_distinct` in the IR, not client-side counting

Sessions, users and "sessions with errors" are distinct counts. Grouping by
`session.id` returns one row per session — 184k rows for a busy app — only
to count them. Per CLAUDE.md ("if the IR can't express something, extend
the IR"), add the aggregate specified in `query-ir-core` (this change).
"Sessions with errors" is then one scoped aggregate, not a second query.
Exact `COUNT(DISTINCT)` was rejected: its memory grows with cardinality.

### 2. App discovery and URL shape

One `logs` read over the spec's frontend-app predicate,
`aggregate by service.name` with `count` and
`last(resource.telemetry.sdk.language)`, lists the apps and their platform.
The app sits in `?app=` and the tab in the path (`/rum/{tab}`), matching the
path-for-selection / query-for-scope split in `lib/urlState.ts`.

### 3. Frontend → backend split via `correlate`

Consumes the v8 `correlate` stage from `query-ir-span-join` as shipped.
"Browser + network vs backend" per request = client span duration minus its
first server-kind child's duration. The v8 `correlate` stage joins each span
to its parent; query server spans whose parent's service is the app,
aggregate by the parent's name / `url.template` with `quantile(0.75)` of
both durations. "Traced share" = client spans with any child ÷ all client
spans — a scoped count over the same join. One query per panel, not a
per-request trace fetch. The Errors tab's "backend cause" works the same
way: one batched read over the listed groups decides which have a preceding
failed request; the trace itself is fetched only for the selected group.

### 3a. One read per KPI, current and previous window together

Each Overview KPI is one bucketed aggregate over twice the window: the
latest half gives the value and sparkline, the earlier half the delta.
Sessions, users and sessions-with-errors share one `logs` read (three
aggregates, two scoped); traced share is the `correlate` read above. The
same pattern serves Interactions: one `aggregate by (target, page)` read,
joined client-side to the per-page INP p75 the Pages read already fetched.

### 4. Sessions from both signals, bounded

The session list is a `logs` aggregate by `session.id` (first/last
timestamp, count of `browser.navigation`, scoped count of `exception`,
`first`/`last` of `url.template`) with `topk` by last timestamp — logs
because every RUM session emits a navigation or vital. Session detail reads
both signals filtered on one `session.id` (bounded by the window and a row
limit) and merges them client-side into lanes; the inline waterfall reuses
`api/traceDetail.ts` for the selected request's trace. This merge is display
ordering of two bounded reads, not a join. The general mechanism — a
cross-signal `correlate` (stub change `query-cross-signal-correlate`) —
would let one read return the session's logs and spans together; when it
lands, the detail and the Errors tab's preceding-request lookup move onto
it. RUM does not wait for it.

### 5. Reuse before porting

The prototype's `window.SignalDBUI` components map onto existing ones:
`KpiCard`, `Sparkline`, `VizTooltip` (mandatory for every viz panel, per
`explore-ui-viz-tooltips`), `TimeRangePicker`, `CopyValueButton`, the Errors
feature's grouping (`api/errors.ts`, scoped by `where`), the trace
waterfall (`lib/waterfall.ts`) and stack frames with source context
(`stack-frame-source-context`: `api/sourceContext.ts`, `SourceSnippet`).
Only RUM-specific pieces are new: vital card + distribution bar, lane
timeline, client/backend split bar, app switcher. The prototype's own
sidebar, palette and theme toggle are **not** ported — the shell already
has them; the palette gains entries instead.

### 6. Static demo data stays in stories

The prototype's `data.js` becomes Storybook fixtures (derived from each
request's range, per `src/ui/CLAUDE.md`) for `Pages/RealUsers` light/dark
stories and the design-sync registration. The page itself never shows
fixture data.

## Risks / Trade-offs

- [Vitals without a route] Third-party apps that don't stamp `url.template`
  / `url.path` on vital records get an Overview but an empty Pages tab →
  Pages shows a callout linking to the Setup step that adds the processor.
- [Approximate counts] `count_distinct` is ~1–2% off → label as
  approximate in the IR docs; KPI cards round anyway.
- [Session detail cost] A long session can hold thousands of resource-timing
  records → the detail query excludes `browser.resource_timing` by default
  and caps rows; the Network tab covers resources in aggregate.
- [PR size] The prototype is large → ship as a stack (see tasks), each PR
  under ~500 lines, each tab independently useful.

## Migration Plan

None: additive route, additive IR aggregate, additive UI telemetry
attributes. Rollback is a revert.

## Open Questions

- Should the UI's own click telemetry come from the log-based
  `UserActionInstrumentation` or the existing `click` spans? The spec picks
  log records (matches third-party SDK output); the Interactions tab could
  read `click` spans as a fallback.
