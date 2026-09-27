## ADDED Requirements

### Requirement: Real users page per frontend app

The explore UI SHALL serve a Real users page at `/rum/{tab}` (`/rum` opens
`overview`) scoped to one frontend app and the shared time range. A frontend
app SHALL be a `service.name` that sent at least one RUM event in the window:
a log record whose `event_name` is `browser.web_vital`,
`browser.navigation`, `browser.user_action.click` or
`browser.resource_timing`, or any record carrying `session.id`. The selected
app SHALL be recorded in the URL as `app`, and the tenant, dataset and range
SHALL carry over like on the other explore pages.

#### Scenario: Opening the page picks the busiest app

- **WHEN** a user opens `/rum` and `storefront-web` and `admin-web` both sent
  RUM events, `storefront-web` more
- **THEN** the URL becomes `/rum/overview?app=storefront-web` (plus the
  carried-over context) and every panel is scoped to
  `service.name = storefront-web`

#### Scenario: Switching apps

- **WHEN** a user picks `admin-web` in the app switcher
- **THEN** `app=admin-web` replaces the previous value, the tab is kept, and
  any selected route, error group or session is cleared

#### Scenario: No frontend app yet

- **WHEN** no service sent RUM events in the window
- **THEN** the page shows an empty state pointing to the Setup tab instead
  of empty panels

### Requirement: Web Vitals from browser.web_vital events

Vitals SHALL be computed from `browser.web_vital` log records: the p75 of
`browser.web_vital.value` per `browser.web_vital.name` (`lcp`, `inp`, `cls`,
`fcp`, `ttfb` — lowercase as the SDK emits them; values in milliseconds
except `cls`), and the share of records per `browser.web_vital.rating` (`good`,
`needs-improvement`, `poor`). LCP, FCP and TTFB SHALL be shown in seconds,
INP in milliseconds and CLS unitless. The p75 SHALL be rated against the Web
Vitals thresholds (LCP 2.5 s / 4 s, INP 200 ms / 500 ms, CLS 0.1 / 0.25,
FCP 1.8 s / 3 s, TTFB 0.8 s / 1.8 s) and the rating SHALL be conveyed by
shape as well as color.

#### Scenario: Vital card

- **WHEN** an app's `lcp` records in the window have p75 2900 and ratings
  68% good, 22% needs-improvement, 10% poor
- **THEN** the LCP card shows `2.9 s`, "Needs improvement", and a 68/22/10
  distribution bar whose tooltip lists each share with its threshold

#### Scenario: A vital with no records

- **WHEN** no INP record exists in the window
- **THEN** the INP card shows `—` with no rating, not `0 ms`

### Requirement: Overview tab

The Overview tab SHALL show: sessions and users (distinct `session.id` /
`user.id`), the share of sessions with at least one `exception` event, and
the share of the app's client HTTP spans that have a server child span
(traced requests) — each with the previous-window change and a sparkline;
the vitals; sessions over time split into with and without errors; the
top frontend requests with their client/backend split; the slowest pages;
the top error groups; and sessions by browser (the first non-GREASE entry of
the `browser.brands` resource attribute, else parsed from
`user_agent.original`) and device type (`browser.mobile`). Each
row SHALL link to the tab or view that explains it. Every chart and bar
SHALL show its values through the shared tooltip (`explore-ui-viz-tooltips`).
Each KPI's value, previous-window change and sparkline SHALL come from one
query covering both windows, not three.

#### Scenario: Error share KPI

- **WHEN** 184,210 distinct sessions were seen and 7,000 of them carry an
  `exception` event
- **THEN** "Sessions with errors" shows `3.8%`

#### Scenario: A slowest-page row drills into Pages

- **WHEN** a user clicks `/checkout` in "Slowest pages"
- **THEN** the Pages tab opens with `/checkout` selected

### Requirement: Pages tab

The Pages tab SHALL list the app's routes — `url.template` when the record
carries one, else `url.path` — with views (count of `browser.navigation`),
p75 LCP, INP, CLS and TTFB, and error share, sorted by the share of poor
ratings. The selected route (URL `route`) SHALL show its vitals with
distributions, a load breakdown from the `browser.navigation_timing` phases
(p75), and the backend endpoints called from it.

#### Scenario: Selecting a route

- **WHEN** a user selects `/products/:id`
- **THEN** the URL gains `route=/products/:id` and the detail shows that
  route's vitals and the client spans started while it was the current page

### Requirement: Sessions list

The Sessions tab SHALL list sessions — records grouped by `session.id` —
with the session id, `user.id`, device and browser, location when present,
start time, duration (first to last record), page views, entry and exit
route, and signal pills (error count, slow LCP). Quick filters SHALL narrow
to sessions with errors or slow loads (LCP rated poor), and a free-text
filter SHALL accept `session.id`, `user.id` or `attribute=value`.

#### Scenario: Filtering to sessions with errors

- **WHEN** a user picks "With errors"
- **THEN** only sessions with at least one `exception` record are listed

### Requirement: Session detail timeline

Opening a session (URL `session`) SHALL show the spans and log records with
that `session.id` — every record except `browser.resource_timing`, which
the Network tab covers in aggregate — up to a cap of 2,000 records, on a
timeline with lanes Views, Actions, Network, Perf,
Errors and Logs, an ordered event list, and the session's resource
attributes. Selecting a network event whose span has backend children SHALL
show that trace's waterfall inline, with the time spent in the browser and
network (client span minus its first server child, via the `correlate`
stage of `query-ir-span-join`) separated from backend time, and links to
the full trace and its backend logs. When the session holds more records
than the cap, the page SHALL say so, show the earliest records up to the
cap, and offer to load the next page. Selecting an
exception SHALL show its stack frames with source context
(`stack-frame-source-context`) and, when a failed request preceded it
in the same view, that request as the likely cause.

#### Scenario: A failed checkout request

- **WHEN** a session has a `POST /api/checkout` client span with status 502
  followed 2.8 s later by a `TypeError` exception
- **THEN** both are marked as errors on the timeline, the exception's panel
  names the 502 request as preceding it, and "Show trace" selects the request
  and shows its backend waterfall

#### Scenario: A session over the cap

- **WHEN** a session holds 3,500 non-resource-timing records
- **THEN** the timeline shows the first 2,000, states that 1,500 more exist,
  and "Load more" fetches the next 2,000

#### Scenario: Jumping to the trace

- **WHEN** a user clicks "Open in Traces" on the inline waterfall
- **THEN** the browser navigates to `/traces/{traceId}` carrying the tenant
  context

### Requirement: Errors tab

The Errors tab SHALL list exception groups (the `explore-ui-errors` grouping)
restricted to the app's `service.name`, flagging groups first seen in the
current `service.version`, with events, users, sessions, first/last seen, a
histogram, stack frames, and a breakdown by browser. When the group's latest
events were preceded in the same session by a failed traced request, the tab
SHALL show that request and its backend trace as the backend cause. Which
listed groups have a backend cause SHALL be decided in one query for the
whole list; a trace SHALL be fetched only for the selected group. "Latest
session" SHALL open the most recent session containing the group.

#### Scenario: Backend cause

- **WHEN** a group's latest event followed a `POST /api/checkout` → 502 whose
  trace has a `payments` span in error
- **THEN** the backend cause panel shows the request, the `payments` service
  and the trace waterfall

### Requirement: Network tab

The Network tab SHALL group the app's client HTTP spans by method and URL
template, showing origin, calls, p75 split into client+network and backend
time, error share (status ≥ 400 or span error), and traced share (client
spans with a server child in the same trace). Origins whose requests are
never traced SHALL raise a callout explaining `traceparent` propagation and
CORS, linking to Setup. Requests to the telemetry export endpoint SHALL be
marked as SDK export, not as untraced. For browser apps, a resources table
SHALL summarise `browser.resource_timing` by initiator type (count, transfer
size, p75 duration, largest).

#### Scenario: Untraced third-party origin

- **WHEN** 38,100 requests to `reviews.partner-cdn.com` have no server child
- **THEN** the callout names that origin and the count, and the row shows
  `0%` traced and "no trace"

### Requirement: Interactions tab

The Interactions tab SHALL list `browser.user_action.click` targets
(`browser.css_selector`, else `browser.tag_name`) with the page, click
count, and the INP p75 of the page view they occurred in, and link each to
the sessions containing it.

#### Scenario: Clicks by element

- **WHEN** `button#pay` was clicked 19,820 times on `/checkout`
- **THEN** its row shows `button#pay`, `/checkout` and `19,820`

### Requirement: Setup tab

The Setup tab SHALL explain instrumenting a browser app with the upstream
OpenTelemetry browser SDK against SignalDB — installing, initializing with
the selected `service.name` and an OTLP endpoint the app's operator runs
(an OpenTelemetry Collector or the app's own backend) that forwards to
SignalDB with the API key attached server-side, propagating `traceparent` to the app's API origins and
allowing it in CORS — with copyable snippets, and a live status checklist:
first session received, vitals received, and the share of requests joined
to backend traces. It SHALL NOT show steps for capabilities SignalDB does
not have, and SHALL NOT tell users to put a SignalDB API key in browser
code: SignalDB keys are bearer credentials with no origin restriction, so
any key shipped to a browser is public.

#### Scenario: Exporter configuration

- **WHEN** a user copies the initialization snippet
- **THEN** it exports to a placeholder collector URL on the app's own
  origin, carries no `Authorization` header or key, and the next snippet
  shows the collector's `otlphttp` exporter holding the key

#### Scenario: Checklist before any data

- **WHEN** the selected app has sent no `browser.web_vital` record
- **THEN** "Vitals received" is unchecked and the other steps stay visible

### Requirement: Platform-aware labels

The page SHALL derive the app's platform from `telemetry.sdk.language`
(`webjs` → browser, `swift` → iOS, `java`/`kotlin` with `os.name` Android →
Android). For non-browser platforms it SHALL relabel Pages/Errors/Interactions
as Screens/Crashes/Taps, hide browser-only panels (Web Vitals, resources,
load breakdown), and show an empty state for mobile vitals until they are
supported.

#### Scenario: An iOS app

- **WHEN** the selected app's records carry `telemetry.sdk.language=swift`
- **THEN** the tabs read Screens, Crashes and Taps and no Web Vitals panel is
  shown

### Requirement: Real users command palette entries

The command palette SHALL offer the Real users tabs and each frontend app,
and SHALL open a session when the query is a session id seen in the window.

#### Scenario: Pasting a session id

- **WHEN** a user pastes a `session.id` into the palette and presses Enter
- **THEN** `/rum/sessions?session={id}` opens for the app that session
  belongs to
