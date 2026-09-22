# Tasks: Explore UI on the Query IR

Groups ≈ PRs, in design D6 order. TDD throughout.

## 1. Logs tab on the IR

- [x] 1.1 `api/ir/logs.ts`: rows query (filters → `where`, limit, order) and level volume (`aggregate` by `log.severity_text`, `step`) with unit tests on the documents they build
- [x] 1.2 Logs view, histogram and live-tail polling use it; detail view shows attribute scopes separately
- [x] 1.3 Remove the LogQL "edit as text" box; delete LogQL compile code

## 2. Discovery via `describe`

- [x] 2.1 `api/ir/discovery.ts`: `fields`, `values` (with partial-tier flag), `metricNames`, `profileTypes`
- [x] 2.2 Wire logs, traces facets, profiles and metrics pickers; add the partial-list hint
- [x] 2.3 Profiles: `profileTypes` aggregates the existing `sample.type`/`sample.unit`/`period.type`/`period.unit` logical fields — no new logical field needed (the `profile.type` alias from an earlier pass was reverted as redundant)

## 3. Trace search on the IR

- [x] 3.1 Compile trace filter chips to `where`; search via `traces` source root-span rows — already true of the group table/volume chart (api/traceGroups.ts, api/traceGroupMembers.ts); `compileTraceQL`'s output was unused for the actual fetch
- [x] 3.2 Delete TraceQL compilation and `tempoSearch`/`tempoSearchTags`

## 4. IR counter rate

- [ ] 4.1 Tests: reset scenario, PromQL-agreement test, rejection without `step` / on non-metric sources
- [ ] 4.2 `rate`/`increase` in `Agg`, planner window, schema version bump, docs, MCP tool text
- [ ] 4.3 Metrics builder compiles range-function rows to the IR

## 5. IR formulas

- [ ] 5.1 Tests: error ratio, missing series, divide by zero, invalid expression
- [ ] 5.2 Multi-query document + formula evaluator in the querier; docs
- [ ] 5.3 Metrics builder formulas on the IR; remove PromQL tab and `api/prom.ts`

## 6. Cleanup

- [ ] 6.1 Delete `api/loki.ts`, `api/pyroscope.ts`, Tempo search functions; drop compat prefixes the UI no longer needs from `lib/proxiedPaths.ts`
- [ ] 6.2 Test guard: hand-written UI request code may not call `/loki`, `/prometheus`, `/tempo`, `/pyroscope` or their generated SDK functions (excludes `api/gen/**`, `lib/proxiedPaths.ts`, connection-info fixtures)
- [ ] 6.3 Update `docs/users/explore-ui.md`; "open as IR" action on each tab
