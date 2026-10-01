## Why

`real-user-monitoring` (archived as `2026-09-28-real-user-monitoring`) shipped the Real users page
with its Overview and Setup tabs, the `count_distinct` IR aggregate and the
UI's own RUM telemetry. The rest of the Claude Design handoff
(`Real User Monitoring.html`, project
`85b1fe47-5601-4e0d-b208-e00b2fcdb175`) was specified there but not built.
Without it, the page counts sessions and vitals but can't answer "which
page", "which session" or "which backend call".

## What Changes

- **Network** tab and the Overview "Frontend → backend" panel: client HTTP
  spans split into client+network and backend time via `correlate`,
  traced share, untraced-origin callout, resources by initiator type.
- **Pages** tab and the Overview "Slowest pages" panel: per-route vitals,
  load breakdown, backend calls.
- **Interactions** tab: clicks by target with the page's INP p75.
- **Sessions** tab and session detail: lane timeline, event list, inline
  backend waterfall, exception with preceding failed request.
- **Errors** tab: app-scoped exception groups with backend cause.
- Overview gains users and traced-request KPIs and rows that link into the
  tabs; Setup gains the traced-requests checklist step; switching apps
  clears the selected route, error group and session.
- Palette: jump to a pasted session id.
- Platform labels for mobile apps (relabelled tabs, browser-only panels
  hidden).
- A user guide for instrumenting a browser app, linked from Setup.

Scoped out: as in `real-user-monitoring`'s proposal.

Not breaking: UI only; reads go through the Query IR with stages that
already exist (`aggregate`, `correlate`).

## Capabilities

### Modified Capabilities

- `explore-ui-rum`: the remaining tabs, Overview panels, Setup step,
  palette session lookup and platform labels.

## Impact

- **src/ui**: `features/rum/*`, `api/rum.ts`, `CommandPalette`, stories
  and design-sync fixtures for the new tabs.
- Docs: `docs/users/explore-ui.md`, a browser instrumentation guide.
