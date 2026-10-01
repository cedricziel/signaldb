## Why

The explore UI has no tenant-wide landing page: `/` drops users on the Logs
view, and answering "is anything broken, what's running, how much am I
sending" means visiting the catalog, errors and each signal view in turn.
The Claude Design handoff "System Overview" (`system-overview` / `app-shell`
templates) adds that page, rendered inside the navigation shell introduced by
`ui-sidebar-navigation`.

## What Changes

- New `/overview` page, which becomes the root route and the sidebar
  wordmark's target:
  - KPI strip: request rate, error rate and p95 over root spans with the
    previous-period change, ingest (records) and a services health card.
  - Deploys lane, inferred from `service.version` changes on spans.
  - Service map with zoom/pan controls and reduced-motion-aware animation.
  - Services table (health, sparkline, rate, errors, p95, last deploy).
  - Ingest volume per signal, top error groups, slowest endpoints.
  - Setup checklist dialog (`/overview?setup`, also a palette action).
- Environment scope via `?env=` (`deployment.environment.name`).
- `Sparkline` gains deploy markers and a hover crosshair; `ServiceGraph`
  keeps measuring its width across loading → loaded.
- `fetchServiceGraph` and `fetchErrorGroups` accept extra `where` stages.

Not breaking: no API change; `/logs` and every other route are unchanged,
only the root redirect target moves.

## Capabilities

### New Capabilities

- `explore-ui-overview`: the System Overview page.

### Modified Capabilities

- `explore-ui-navigation`: the root path redirects to `/overview`.

## Impact

- **src/ui** only: `features/overview/*`, `api/overview.ts`, small options on
  `api/serviceGraph.ts` and `api/errors.ts`, `components/Sparkline`,
  `components/ServiceGraph`, `routes.tsx`, `lib/urlState.ts` (`env`).
- No HTTP API, CLI or SDK change: every figure is a Query IR read. Ingest is
  shown in records because no per-tenant byte counts are exposed to the UI;
  showing bytes would need a new API (out of scope).
