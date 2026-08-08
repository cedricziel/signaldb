---
audience: user
type: how-to
status: living
sources:
  - src/ui/**
  - src/router/src/ui.rs
  - src/router/src/endpoints/session.rs
---

# Explore UI

SignalDB ships a built-in explore UI for logs, traces, and metrics, served
by the router at its **root** (`http://<router>:3000/`, as a SPA fallback
behind the API routes). It consumes the same Loki-, Tempo-, and
Prometheus-compatible APIs that Grafana uses, so anything visible in the UI is
equally queryable from Grafana. It also hosts the OAuth connector **consent
screen** at `/oauth/consent` (see [MCP](mcp.md)).

![Explore UI logs view: virtualized log list with level colors, volume histogram, and fields sidebar](../assets/screenshots/explore-logs.png)

## What it does

- **Logs** — filter chips compiled to LogQL (with an "edit as text" escape
  hatch), a per-level volume histogram, a virtualized log list with
  per-attribute filter/exclude actions, a fields sidebar, and live tail.
- **Traces** — a facet sidebar and a span-volume chart stacked by span status
  sit above a group-first view: recent traces arrive grouped by root
  span name (or by service, any observed root-span/resource attribute, or
  two dimensions combined via "Then by"), with per-group trace count,
  request rate, error rate, p50/p95 latency, and last-seen columns —
  all sortable. Selecting a group lists just its traces; selecting a trace
  opens a waterfall with span details and error highlighting. The span
  panel lists that span's events, giving exceptions an error treatment that
  surfaces the message, type, and stacktrace. Open-by-ID works from any level.
- **Metrics** — a visual query builder (metric picker, tag filters,
  aggregation, and range functions, all populated from label metadata) with
  multi-query formulas for ratios, plus a "PromQL" tab as the raw escape
  hatch. See [Building metric queries](#building-metric-queries).
- **Profiles** — a flame graph of stored profiles, filtered by service and
  profile type. Click a frame to zoom into its subtree (its ancestors stay
  as full-width bars above; "reset zoom" or clicking the root returns), and
  type in the highlight box to light up matching frames — e.g. a crate
  prefix like `common::` — while everything else dims, with a matched-share
  readout for finding your code in a library-heavy profile.
- **Query** — a native [Query IR](querying-ir.md) builder for `logs`/`traces`:
  pick a source and result envelope, add filter chips, and the tab emits a
  structured, versioned IR document (no dialect string) via the generated API
  client, rendering the declared `rows`/`series`/`table` result.
- **Correlation** — log rows with a `trace_id` open the trace waterfall;
  the span panel links back to logs filtered by that trace.
- Every view is a URL: each signal has its own path (`/logs`, `/traces`,
  `/metrics`, `/profiles`, `/query`), with time range, filters, and selection
  in query parameters alongside it — so views are separately navigable and
  can be bookmarked, shared, and revisited with the browser back/forward
  buttons. Tenant/dataset administration lives at `/manage`.

### Narrowing traces

The traces tab has a facet sidebar. Expanding a facet lists its values with the
number of matching spans **across the whole selected window** — not just the
traces the list happened to fetch — most frequent first. Selecting a value adds
a filter; filters appear as removable chips and narrow the trace list, the group
table, and the volume chart together, so the chart always describes what the
table shows. Filters live in the URL, so a narrowed view is shareable.

Facets currently cover `service.name`, `span.name`, and `status`. These are the
fields the query API can enumerate exactly today; attribute facets follow once
[#1073](https://github.com/cedricziel/signaldb/issues/1073) lands, and will
appear in the same sidebar without changing how it works.

### Reading the volume charts

The logs and traces tabs both open with a stacked volume chart — logs by
severity level, traces by span status (span rows, not distinct traces).

Both charts are server-side aggregates over the whole selected window. They are
**not** derived from the rows in the list below them, so the row limit never
truncates them: a chart that looks flat is reporting flat data, not a truncated
query.

Point at any bucket — anywhere in its column, however short the bar — for its
timestamp, a per-series breakdown, and the bucket total. Buckets are also
focusable, so the same detail is reachable with the keyboard.

Two controls sit beside the time axis. **Bucket width** sets the chart's
resolution — it defaults to a width chosen for the selected window, and each
offer states how many buckets it produces, so "finer" and "coarser" are
concrete. A width chosen for a narrow window is not carried over to a much
wider one, which would otherwise issue a needlessly expensive query.

One busy bucket can dwarf the rest of the window: at a 36:1 ratio the typical
bucket occupies about 5% of the chart's height. The **log scale** toggle beside
the time axis compresses the vertical range so the baseline stays readable next
to a spike. It applies to the bucket total, with each stacked series keeping its
true proportion of the bar. Both controls travel in the URL — a shared link
opens on the same resolution and scale the sender was using.

A series that is present in a bucket is always drawn, however small its share:
a handful of errors among tens of thousands of other spans stays visible as a
thin band rather than rounding away.

![Explore UI trace waterfall with span details and a link to correlated logs](../assets/screenshots/explore-traces.png)

![Explore UI metrics view charting a PromQL range query across two services](../assets/screenshots/explore-metrics.png)

![Explore UI profiles flame graph with the highlight box narrowing a CPU profile to SignalDB's own frames](../assets/screenshots/explore-profiles.png)

## Building metric queries

The metrics view opens on a **visual builder** so you don't have to hand-write
PromQL. A query row reads left to right as a sentence:

```
[ a ]  metric ▾   from ⟨ filters ⟩   avg by ⟨ group ⟩   function ▾
```

- **Metric** — type or pick a metric name; suggestions come from the
  Prometheus `__name__` label for the current time range.
- **from** — add tag filters (`+ filter`). Label names and their values are
  suggested from the metadata endpoints, so you filter on what exists rather
  than guessing. Each filter has an operator (`=`, `!=`, `=~`, `!~`).
- **aggregation** — choose a space aggregation (`sum`/`avg`/`min`/`max`/
  `count`) and an optional comma-separated **group by** to get one series per
  tag value.
- **function** — an optional range function (`rate`, `irate`, `increase`, or
  an `*_over_time` rollup) with a lookback window (default `5m`).

Labels are annotated with their approximate value count (from
[`/label_stats`](querying-promql.md#label-cardinality)), and grouping by a
high-cardinality label — one that would explode into thousands of series, like
a pod or trace id — shows a `⚠` warning before you run it.

A live preview shows the compiled PromQL beneath the row; **Run** charts it.

### Formulas across multiple queries

Add more rows with **+ query** — each gets a letter (`a`, `b`, …) — and combine
them in the **formula** box. Single letters are substituted with each query's
compiled expression, so a ratio like an error rate is:

```
formula:  (a / b) * 100
```

with `a` = `sum(rate(http_server_errors[1m]))` and `b` =
`sum(rate(http_server_requests[1m]))`. PromQL function names are left
untouched. With no formula, the first row is charted on its own.

### Editing the raw PromQL

The **PromQL** tab is the escape hatch for anything the builder doesn't cover.
Switching to it seeds the box with the query the builder compiled, so you can
start visually and finish by hand. (Editing raw PromQL back into the builder
is not supported yet.) The same PromQL runs unchanged in Grafana or against
the [`/prometheus/api/v1` endpoints](querying-promql.md).

## Signing in

On an embedded deployment (the UI served by the router at `/ui`), the
first query that fails as unauthenticated opens a sign-in form asking only
for a user email and password. Accounts that belong to a single tenant land
directly in it (on its default dataset); accounts spanning several tenants
pick one from a selector listing each membership by name and role. See
[the authentication reference](authentication.md).

![The post-login tenant selector listing each membership with its name and role](../assets/screenshots/login-tenant-selector.png)

Signing in calls `POST /ui/session`, which validates the credentials and
sets an `HttpOnly`, `Secure`, `SameSite=Strict` cookie containing an opaque
random token. The password and tenant API keys never live in the cookie,
page JavaScript, `localStorage`, or URLs. Sessions expire after 12 hours;
`DELETE /ui/session` revokes the server-side session and clears the cookie.

Once signed in, the tenant/dataset selector offers the user's tenant
memberships and the selected tenant's datasets. The chosen values are sent
as `X-Tenant-ID`/`X-Dataset-ID`; the server validates the tenant against the
current user's memberships.

In development the Vite proxy injects credentials from `.env.local`
instead, so no sign-in is needed.

## Availability

Container images (router and monolithic) ship the UI preinstalled. For
source builds, the router serves the directory named by `SIGNALDB_UI_DIR`:

```bash
pnpm install && pnpm ui:build          # builds src/ui/dist
SIGNALDB_UI_DIR=src/ui/dist cargo run --bin signaldb
```

Without `SIGNALDB_UI_DIR`, the root serves a placeholder page. Setting the
variable to a directory without a built UI fails startup on purpose — a
misconfigured deployment should not silently ship without its UI.

## Telemetry

The UI is instrumented with OpenTelemetry (browser SDK). It injects a W3C
`traceparent` into every API call so a user action correlates end-to-end with
the backend traces it triggers, and stamps every span with a RUM `session.id`
plus the active `tenant.id` / `dataset.id`. The initial page load is
correlated in the reverse direction: the `documentLoad` span links to the
server span that served the document, read back from the response's
[`Server-Timing: traceparent`](response-trace-context.md) entry.

Export is **opt-in**. The preferred way to turn it on is the
`[self_monitoring.frontend]` config section — the router serves it to the
browser at runtime, so one image works for every deployment without a rebuild:

```toml
[self_monitoring.frontend]
enabled = true
endpoint = "http://signaldb.example:4318"   # reachable from the browser
api_key = "sk-ingest-only-key"               # world-readable; ingest-only
# tenant_id / dataset_id default to _system / _monitoring
# allowed_origins = ["http://signaldb.example:3000"]  # CORS; empty = any
```

The `api_key` is delivered to the browser and is visible to anyone who can load
the UI, so use an **ingest-only** key and only on a trusted network. When the
UI is internet-facing, point `endpoint` at an OTLP collector that adds
auth/tenant headers and scrubs PII instead of straight at the acceptor. With
export unset, propagation still works and dev builds print spans to the
console. (A build-time `SIGNALDB_OTLP_ENDPOINT` is still honoured as a
fallback.) Contributor detail lives in the `frontend-instrumentation` skill.

## Developing the UI

See [src/ui/README.md](https://github.com/cedricziel/signaldb/blob/main/src/ui/README.md): `pnpm ui:dev` runs a Vite
dev server with hot reload that proxies API calls to any live SignalDB
instance (local or remote) with credentials injected from `.env.local`.
