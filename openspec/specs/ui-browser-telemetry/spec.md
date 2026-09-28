# ui-browser-telemetry Specification

## Purpose

Defines what the SignalDB UI's own browser telemetry records beyond the
SDK defaults, so the UI is a complete real-user-monitoring source for
itself: the route template, browser identity and click events.

## Requirements

### Requirement: UI log records carry the current route template

Every log record the SignalDB UI emits SHALL carry `url.template`, the
React Router pattern of the route active when the record was emitted (for
example `/traces/:traceId`), never a concrete path with ids. Records emitted
before the router mounts SHALL carry no `url.template`.

#### Scenario: A Web Vital on a trace page

- **WHEN** an INP record is emitted while `/traces/7c1e…` is open
- **THEN** it carries `url.template = /traces/:traceId`

#### Scenario: Before the router mounts

- **WHEN** a TTFB record is emitted before the first route renders
- **THEN** it carries no `url.template`

### Requirement: UI resource identifies the browser

The UI's telemetry resource SHALL carry the OTel browser resource
attributes `browser.brands`, `browser.platform` and `user_agent.original`
alongside the existing `browser.mobile` and `browser.language`, so its own
sessions can be broken down by browser. `browser.brands` and
`browser.platform` SHALL be omitted where `navigator.userAgentData` is
unavailable (Safari, Firefox).

#### Scenario: Chrome

- **WHEN** the UI runs in Chrome 129
- **THEN** the resource carries `browser.brands` including `Chromium 129`

#### Scenario: Firefox

- **WHEN** the UI runs in Firefox
- **THEN** the resource carries `user_agent.original` and no
  `browser.brands`

### Requirement: UI emits click events

The UI SHALL enable the browser SDK's user-action instrumentation so every
click emits a `browser.user_action.click` log record with the target's
`browser.css_selector` / `browser.tag_name`. A click whose target is not an
`HTMLElement` (an SVG icon inside a button) SHALL be attributed to its
closest `HTMLElement` ancestor; SDK 0.7 drops such clicks, so the UI
retargets them before the instrumentation sees them. It SHALL NOT capture
element text or input values.

#### Scenario: Clicking a sidebar link

- **WHEN** a user clicks the "Traces" sidebar link
- **THEN** a `browser.user_action.click` record is emitted carrying
  `session.id`, `url.template` and a selector for the link, and no link text

#### Scenario: Clicking an icon inside a button

- **WHEN** a user clicks the SVG icon inside the theme toggle button
- **THEN** one `browser.user_action.click` record is emitted with the
  button's selector and `browser.tag_name = BUTTON`
