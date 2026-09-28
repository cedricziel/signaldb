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
- **THEN** `app=admin-web` replaces the previous value and the tab is kept

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

The Overview tab SHALL show: sessions (distinct `session.id`), the share of
sessions with at least one `exception` event, and page views (count of
`browser.navigation`) — each with the previous-window change and a
sparkline; the vitals; sessions over time split into with and without
errors; the top error groups; and sessions by browser (the first
non-GREASE entry of the `browser.brands` resource attribute, else parsed
from `user_agent.original`) and device type (`browser.mobile`). Every chart
and bar SHALL show its values through the shared tooltip
(`explore-ui-viz-tooltips`). Each KPI's value, previous-window change and
sparkline SHALL come from one query covering both windows, not three.

#### Scenario: Error share KPI

- **WHEN** 184,210 distinct sessions were seen and 7,000 of them carry an
  `exception` event
- **THEN** "Sessions with errors" shows `3.8%`

### Requirement: Setup tab

The Setup tab SHALL explain instrumenting a browser app with the upstream
OpenTelemetry browser SDK against SignalDB — installing, initializing with
the selected `service.name` and an OTLP endpoint the app's operator runs
(an OpenTelemetry Collector or the app's own backend) that forwards to
SignalDB with the API key attached server-side, propagating `traceparent` to the app's API origins and
allowing it in CORS — with copyable snippets, and a live status checklist:
first session received, page views received, and vitals received. It SHALL NOT show steps for capabilities SignalDB does
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

### Requirement: Real users command palette entries

The command palette SHALL offer the Real users tabs and each frontend app.

#### Scenario: Jumping to a tab

- **WHEN** a user types "Setup" in the palette and picks the Real users
  entry
- **THEN** `/rum/setup` opens with the current tenant, dataset and range
  carried over
