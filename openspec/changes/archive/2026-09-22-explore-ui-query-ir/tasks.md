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

- [x] 4.1 Tests: reset scenario, rejection without `step` / on non-metric sources (query-ir `validate.rs` + querier `ir_planner.rs`; no PromQL-agreement test — see handback)
- [x] 4.2 `rate`/`increase` in `Agg`, planner window, schema version bump, docs, MCP tool text (no supported-function listing in the MCP tool description to update)
- [x] 4.3 Metrics builder compiles range-function rows to the IR (`rate`/`increase` only — `irate`/`*_over_time` have no IR stage and are no longer offered by the builder; see the handback in the change's PR/commit history for the full list of dropped options)
- [x] 4.4 IR v7: `irate`/`avg_over_time`/`min_over_time`/`max_over_time`/`sum_over_time`/`count_over_time` per-series range functions, an aggregate `across` reducer (`sum`/`avg`/`min`/`max`/`count`), and a `window` lookback independent of `step` — `query-ir` (`AggFn`, `Agg.across`/`Agg.window`, validation) and `querier::query::ir_planner::lower_rate_aggregate` (RANGE-frame window evaluation, `across` reduction); docs
- [x] 4.5 Metrics builder restores `irate`/`*_over_time`, a window input, and the `across` reducer on top of IR v7 (`RangeFnSpec.across`/`.window`, `QueryRow`'s window/across controls); old `{fn}`-only URLs still load

## 5. IR formulas

- [x] 5.1 Tests: error ratio, missing series, divide by zero, invalid expression (`query-ir/src/formula.rs`)
- [x] 5.2 Multi-query document + formula evaluator in the querier; docs — `POST /api/v1/query` accepts `{queries, formulas, result}` (discriminated by `queries`), authorizes every inner query's source, executes each via its own Flight ticket, and evaluates formulas with `query-ir`'s evaluator; OpenAPI/TS client/Rust SDK regenerated
- [x] 5.3 Metrics builder formulas on the IR; remove PromQL tab and `api/prom.ts`

## 6. Cleanup

- [x] 6.1 Delete `api/loki.ts`, `api/pyroscope.ts`, Tempo search functions; drop compat prefixes the UI no longer needs from `lib/proxiedPaths.ts` (`api/pyroscope.ts`/`api/tempo.ts` already held no compat client, just shared types — renamed to `api/profileTypes.ts`/`api/traceTypes.ts` rather than deleted; `lib/proxiedPaths.ts`'s prefixes are still needed by the dev proxy/service-worker denylist for Grafana-facing paths, so kept as-is)
- [x] 6.2 Test guard: hand-written UI request code may not call `/loki`, `/prometheus`, `/tempo`, `/pyroscope` or their generated SDK functions (excludes `api/gen/**`, `lib/proxiedPaths.ts`, connection-info fixtures) — `src/ui/src/test/compatGuard.test.ts`
- [x] 6.3 Update `docs/users/explore-ui.md` (the "open as IR" action was left undone — see the change's handback for why)
