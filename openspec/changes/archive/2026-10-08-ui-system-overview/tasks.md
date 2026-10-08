## 1. Data

- [x] 1.1 Tests for the environment scope, deploy inference, decoders and ingest merging (`api/overview.test.ts`).
- [x] 1.2 `api/overview.ts`: system KPIs over root spans, per-service activity, version sightings → deploys, record counts per signal, slowest endpoints.
- [x] 1.3 `where` scoping for `fetchServiceGraph` and `fetchErrorGroups`, with tests.

## 2. Page

- [x] 2.1 Tests for health rules, KPI figures, deploy labels and setup steps (`overviewModel.test.ts`).
- [x] 2.2 `/overview` route, root redirect and wordmark target; `?env=` in `ExploreState`.
- [x] 2.3 KPI strip, deploys lane, service map with zoom/pan, services table, ingest, error groups, slowest endpoints, setup checklist.
- [x] 2.4 Page tests (`OverviewView.test.tsx`) and e2e scenarios for the landing page and the palette's setup action.
- [x] 2.5 `Sparkline` markers/crosshair and the `ServiceGraph` width-measurement fix.

## 3. Docs

- [x] 3.1 `docs/users/explore-ui.md`: "The overview" section; navigation updates.
