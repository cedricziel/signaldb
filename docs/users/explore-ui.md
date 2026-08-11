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

- **Catalog** — a service/infrastructure catalog discovered by querying the
  ingested telemetry for OTel semantic-convention resource attributes, not
  from a fixed inventory. See [The catalog](#the-catalog).
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
  surfaces the message, type, and stacktrace, followed by its attributes —
  split into **Span** and **Resource** sections and sorted alphabetically,
  with the sub-header compiling service name, namespace, deployment
  environment, and version from the resource attributes that carry them. A
  value over ~200 characters (a Rust `Debug` dump, a stack trace) collapses
  behind a "More" toggle rather than flooding the panel; the copy button
  always copies the untruncated value. Open-by-ID works from any level.
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
- **Query** — a native [Query IR](querying-ir.md) builder for `logs`, `traces`,
  and profile summaries:
  pick a source and result envelope, add filter chips, and the tab emits a
  structured, versioned IR document (no dialect string) via the generated API
  client, rendering the declared `rows`/`series`/`table` result.
- **Correlation** — log rows with a `trace_id` open the trace waterfall;
  the span panel links back to logs filtered by that trace.
- Every view is a URL: each signal has its own path (`/catalog`, `/logs`,
  `/traces`, `/metrics`, `/profiles`, `/query`), with time range, filters, and
  selection in query parameters alongside it — so views are separately
  navigable and can be bookmarked, shared, and revisited with the browser
  back/forward buttons. Tenant/dataset administration lives at `/manage`.

### The catalog

The catalog answers "what's actually sending telemetry" by discovery, not
configuration: it groups the traces in the selected window by the OTel
resource attributes that identify a **service**, **database**,
**message destination**, **host**, **Kubernetes pod/node**, **container**,
or **process** — the same RED-metrics aggregate (count, error rate, p50/p95,
last-seen) the traces group table computes — and lists whatever it finds
under each entity type in the left nav. There is no hardcoded or sample
data: an entity type with no matching resource attribute in the window
renders an explicit empty state naming the attribute it's looking for (e.g.
"No hosts observed in this window — no `host.name` resource attribute seen
on any span") rather than a placeholder row. A tenant whose telemetry starts
carrying that attribute — an SDK resource detector, an OTel Collector with
`resourcedetection`, Kubernetes downward-API injection — gets that entity
type populated with no further configuration.

Selecting a row drills into the Traces tab filtered to that entity. "Services"
is scoped to server-kind spans specifically: a service's own resource
attributes appear on every span it emits, including calls it makes to its
dependencies, so without that scope its request rate/latency would mix
inbound and outbound traffic.

### Reading a log line

Selecting a log line expands it. Alongside the stream labels it lists the
line's **per-line fields**: the trace context (`trace_id`, `span_id`, with a
link through to the trace) and the log and resource attributes the record
actually carried. Attributes appear per line, so two lines in the same stream
show their own values rather than a shared set.

These have no filter/exclude actions yet. The filter chips compile to a LogQL
stream selector, which is the wrong shape for a field that varies line to line;
filtering on them arrives with the Query IR migration, which builds the
predicate server-side.

One limitation to know about: the Loki wire format carries these as one flat
map, so the three OTel attribute scopes — resource, instrumentation scope, and
the log record — are merged in this view, and instrumentation-scope attributes
are not shown at all. Storage keeps all three separate; see the
[Query IR reference](querying-ir.md) to query them individually today.

### Narrowing traces

The traces tab has a facet sidebar. Expanding a facet lists its values with the
number of matching spans **across the whole selected window** — not just the
traces the list happened to fetch — most frequent first. Selecting a value adds
a filter; filters appear as removable chips and narrow the trace list, the group
table, and the volume chart together, so the chart always describes what the
table shows. Filters live in the URL, so a narrowed view is shareable.

Facets currently cover `service.name`, `span.name`, `status`, and `span.kind`.
These are the fields the query API can enumerate exactly today; attribute
facets follow once [#1073](https://github.com/cedricziel/signaldb/issues/1073)
lands, and will appear in the same sidebar without changing how it works.

Both the facet sidebar and the traces' span-detail panel are resizable: drag
the handle on the sidebar's trailing edge. The facet/field sidebar's width is
shared between the logs and traces tabs and persists across sessions.

### The group table

Traces are presented grouped, one row per distinct value of the grouping
dimensions, carrying **RED** for that group: request count, rate over the
window, error count, and p50/p95 duration, plus when the group was last seen.

Every one of those numbers is a server-side aggregate over the whole selected
window. The row budget (500 groups) bounds how many _groups_ come back, never
the records they are computed from — so a group's p95 is the p95 of all its
records in the window, and changing the row limit does not move it. When more
than 500 groups exist the table says so; it does not claim a total, because the
number of distinct groups is not something the query returns.

**Grain** selects what a row counts:

| Grain    | A row counts        | Duration is          | Filters match          |
| -------- | ------------------- | -------------------- | ---------------------- |
| `traces` | traces (root spans) | the trace end-to-end | the **root** span only |
| `spans`  | matching spans      | each span            | any span               |

Trace grain is the default. Because it scopes the query to root spans, a filter
on a field that only ever appears on a child span — a `db.system` on an inner
call, say — legitimately matches nothing and the table is empty; switch to span
grain to see those matches. Matching a trace because _any_ of its spans matches,
while still grouping by the root, is a structural query and is not available
yet. The grain lives in the URL, so a shared link reproduces it.

**Grouping** is by span name and optionally a second dimension. Beyond the
built-ins (`span.name`, `service.name`) you can type any attribute name —
`http.route`, `deployment.environment` — and the server groups by it directly,
including a bucket for records carrying no value for it.

**Sorting** re-runs the query rather than reordering the rows on screen. This
matters: the table holds the top 500 groups _under the current sort_, so
reordering those locally would answer "the slowest of the 500 most frequent
groups" instead of "the 500 slowest". Sorting by rate is the same ordering as
sorting by count, since rate is count divided by a fixed window.

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

## User menu

Once signed in, a user menu appears in the top bar showing an avatar
(initials from the display name), the user's name, and a dropdown with:

- **Appearance** — toggle between light and dark theme; the choice is
  persisted in `localStorage` and restored on reload.
- **Send data** — opens the Instrumentation page (see below).
- **API keys** — opens the API Keys page (see below).
- **Docs** — opens the SignalDB documentation in a new tab.
- **Switch tenant** — opens the Tenant Selection page (see below).
- **Sign out** — deletes the session, clears the query cache, and
  reloads the page.

The menu closes on Escape or backdrop click.

### Tenant selection (`/select-tenant`)

Shows every tenant the user is a member of, with their role on each.
The current tenant is expanded by default to reveal its datasets;
clicking a dataset navigates to `/logs` with that tenant/dataset
selected. Other tenants are collapsed and fetch their datasets lazily
via `whoami(tenant_id)` on expansion.

### API keys (`/api-keys`)

Tenant-admin-only page for managing ingestion API keys. The same API
functions used by the admin management panel (`listApiKeys`,
`createApiKey`, `revokeApiKey`) power this page, but it is scoped to
the current tenant rather than requiring instance-admin privileges.

Creating a key shows the secret once in a modal with a copy button;
revoking is immediate and irreversible.

### Instrumentation (`/instrumentation`)

Guided, source-specific instructions for sending telemetry to
SignalDB. A sidebar lets the user pick one of six sources:

| Source         | Snippet type                    |
| -------------- | ------------------------------- |
| OTel SDK       | `OTEL_EXPORTER_OTLP_*` env vars |
| OTel Collector | YAML exporter config            |
| Kubernetes     | Helm values / kubectl manifest  |
| Docker         | `docker run` / compose env vars |
| journald       | Promtail config                 |
| Prometheus     | `remote_write` config           |

Every snippet is interpolated with the user's actual tenant ID and
dataset ID from `whoami`, so they can be copied directly. A
verification section at the bottom shows ingestion status per signal
(metrics, logs, traces, profiles) — currently static ("Waiting for
data"), with real checks planned.

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

The UI is instrumented with OpenTelemetry (browser SDK) across two signal
types. **Spans**: it injects a W3C `traceparent` into every API call so a user
action correlates end-to-end with the backend traces it triggers, and stamps
every span with a RUM `session.id` plus the active `tenant.id` / `dataset.id`.
The initial page load is correlated in the reverse direction: the router
injects the server's trace context directly into `index.html` as a
`<meta name="traceparent">` tag, and the UI uses it as the real parent of its
`documentLoad` span (falling back to a same-trace-id _link_, read from the
response's [`Server-Timing: traceparent`](response-trace-context.md) entry,
when no tag is present). See [Trace context in the document
body](response-trace-context.md#trace-context-in-the-document-body) for the
sampling trade-off that comes with real parenting.

**Log records**: Core Web Vitals, navigation/resource timing, route changes,
uncaught errors, and console `error`/`warn` calls are captured as log records
via `@opentelemetry/browser-instrumentation`, stamped with the same
`session.id`/`tenant.id`/`dataset.id`. Browser errors show up here (not as
`browser.error` spans — that hand-rolled span capture was replaced by this).
The UI's resource also carries `service.namespace`, `signaldb.server.version`
(the backend build that served the session — distinct from the UI bundle's
own `service.version`), and `deployment.environment.name`, all sourced from
the same runtime config as the export settings below. Full instrumentation
list and rationale in the `frontend-instrumentation` skill.

Export is **opt-in**. The preferred way to turn it on is the
`[self_monitoring.frontend]` config section — the router serves it to the
browser at runtime, so one image works for every deployment without a rebuild:

```toml
[self_monitoring.frontend]
enabled = true
endpoint = "http://signaldb.example:4318"   # reachable from the browser; both /v1/traces and /v1/logs
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
