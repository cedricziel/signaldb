Each numbered group is one PR in a stack (under ~500 changed lines each).

## 1. IR: `count_distinct`

- [x] 1.1 Failing tests in `cargo test -p querier` (IR lowering) for `count_distinct` over logs and traces: plain, scoped, nulls skipped, unsupported types rejected, and the version gate rejecting an earlier `irVersion`.
- [x] 1.2 Add `count_distinct` to the IR aggregate enum and validation (`common`), lower it to DataFusion `approx_distinct` (`querier`), bump the IR version.
- [x] 1.3 Integration test in `tests-integration`: a `POST /api/v1/query` counting distinct `session.id` over ingested OTLP logs.
- [x] 1.4 Update the OpenAPI spec for the new aggregate `fn`; regenerate `src/signaldb-sdk` and `src/ui/src/api/gen`. (No change needed: the OpenAPI spec does not enumerate IR aggregate functions, so neither client changes.)
- [x] 1.5 Docs: `docs/users/querying-ir.md` aggregate table and a `count_distinct` example; update the MCP `query-ir` skill text if it lists aggregate functions.

## 2. UI self-instrumentation

- [x] 2.1 Failing tests (`pnpm --filter ./src/ui test`) for a `RouteTemplateLogRecordProcessor` (stamps `url.template` from the active route; nothing before mount) and for the browser resource attributes.
- [x] 2.2 Implement the processor, wire it into `telemetry/logs.ts`, feed it the matched route pattern from the router.
- [x] 2.3 Add `browser.brands` / `browser.platform` / `user_agent.original` to `telemetry/resource.ts`.
- [x] 2.4 Enable `UserActionInstrumentation` (click, no text capture) in `telemetry/logs.ts`, retargeting SVG clicks to the closest `HTMLElement`, with tests that no element text is recorded and an icon click is attributed to its button.
- [x] 2.5 Update the `frontend-instrumentation` skill's module map and signal table.

## 3. Real users page: shell + Overview + Setup

- [x] 3.1 Failing tests for `api/rum.ts` decoders (apps discovery, KPIs — one read per KPI yielding value, delta and sparkline —, vitals, sessions-over-time, browser/device breakdown) and `rumModel.ts` (vital rating/format with lowercase names and ms values, distribution shares).
- [x] 3.2 `api/rum.ts` IR queries and `features/rum/useRumData.ts` hooks keyed by range scope + app.
- [x] 3.3 Navigation: `rum` page in `navModel.ts` (Monitor, after Catalog), icon, `/rum/:tab` route with unknown-tab redirect, `?app=` in URL state; tests in `navModel.test.ts` and `App.test.tsx`.
- [x] 3.4 `RealUsersView` with app switcher, tab strip, Overview (KPIs, vitals with `VizTooltip` distributions, sessions stacked chart, slowest pages, top errors, breakdowns) and Setup (snippets, live checklist); empty state when no app. (Slowest pages omitted per group scope — a later group's Pages tab.)
- [x] 3.5 `Pages/RealUsers` Storybook stories (Default, Dark, Empty) with range-derived fixtures; design-sync registration (`.design-sync/pkg/build.sh` PAGES line, `config.json` titleMap/overrides).
- [x] 3.6 Command palette: Real users tabs and frontend apps.
- [x] 3.7 e2e: sidebar → Real users → Overview renders (mocked IR responses, no live backend — matches `e2e/navigation.spec.ts`'s existing pattern).

## 4. Network + frontend → backend

- [ ] 4.1 Failing tests for the `correlate`-based client/backend split and traced-share decoders.
- [ ] 4.2 Network tab (requests table, untraced-origin callout, resources by initiator type) and the Overview "Frontend → backend" panel.

## 5. Pages + Interactions

- [ ] 5.1 Failing tests for per-route vitals, navigation-timing breakdown and click-target aggregation.
- [ ] 5.2 Pages tab (route list sorted by poor share, route detail, load breakdown, backend calls) with `?route=`; missing-route callout.
- [ ] 5.3 Interactions tab (clicks by target, INP p75 of the page).

## 6. Sessions + session detail

- [ ] 6.1 Failing tests for session list aggregation/filters and for merging a session's spans and logs into lanes and events.
- [ ] 6.2 Sessions tab with quick filters and attribute filter; `?session=` detail with lane timeline, event list, inline trace waterfall (reusing `lib/waterfall.ts`), exception panel with preceding failed request, attributes.
- [ ] 6.3 Palette: jump to a pasted session id.

## 7. Errors

- [ ] 7.1 Failing tests for app-scoped error groups, "new in release" and the preceding-failed-request lookup (one batched read for the list, not per row).
- [ ] 7.2 Errors tab reusing `api/errors.ts` scoped by `service.name`, stack frames, by-browser breakdown, backend cause, latest session link.

## 8. Platform labels + docs

- [ ] 8.1 Failing tests for platform detection from `telemetry.sdk.language`; relabel tabs and hide browser-only panels for mobile.
- [ ] 8.2 Docs: "Real users" section in `docs/users/explore-ui.md`; a user guide for instrumenting a browser app (routed via the docs skill), linked from the Setup tab.
- [ ] 8.3 Surface parity: HTTP API is the IR (covered by 1.x); CLI runs IR documents already — no RUM CLI view (scoped out in the proposal).
