# Design: Explore UI on the Query IR

## Context

The IR already covers most of what the compat-backed tabs need: `logs`/`traces`/
`profiles`/`metrics` sources, `where` predicate trees, `aggregate` with `step`,
`quantile`, `histogram_quantile`, `topk`, and `describe` for fields and values
(`src/query-ir/src/stage.rs`). Missing: counter rate, cross-query formulas. The
errors and catalog tabs and the trace/profile detail views are already on the IR
(`api/queryIr.ts`, `api/profilesIr.ts`, `api/traceDetail.ts`).

## Goals / Non-Goals

**Goals:** every Explore read on the IR; no feature lost except the raw
PromQL/LogQL editors; each step shippable on its own.

**Non-Goals:** streaming tail, `correlate`/`match`, changing compat APIs.

## Decisions

### D1. One typed IR client, per-signal builders

Add small builder modules (`api/ir/logs.ts`, `traces.ts`, `metrics.ts`,
`discovery.ts`) on top of the existing `api/queryIr.ts` client. Each returns
typed view models, so feature components change their data source, not their
rendering. Alternative — generic JSON documents built in components — rejected:
spreads IR shape knowledge across the UI.

### D2. Filter chips compile to `where`, not to a dialect string

`lib/traceFilters.ts` and the logs chip model already hold structured filters;
compile them straight to the IR predicate tree. The TraceQL/LogQL compilers are
deleted with their tests. Chip state in the URL keeps its current encoding so
bookmarks survive.

### D3. Discovery through `describe`

Keys: `describe target:"fields"` on the tab's source. Values:
`describe target:"values"`; the tier in the response drives the "partial list"
hint. Metric names: values of `metric.name` on `metrics`. Profile types: values
of the profile type field on `profiles` (add it as a logical field if the
resolver lacks one). Label stats: coverage and cardinality from `describe
fields`.

### D4. Counter rate as an `aggregate` function

Add `rate` and `increase` to `Agg` (usable only with `step`, only on `metrics`
and `metrics_histogram`), computed per series before the group reduce, with
reset handling matching Prometheus' extrapolation-free `increase`. An aggregate
function rather than a new stage keeps `sum by (x) (rate(y))` a single
`aggregate` stage: `{fn:"rate", field:"metric.value"}` then an outer reduce. The
planner computes it with a window over series ordered by timestamp.
Alternative — a separate `rate` stage before `aggregate` — more composable but
doubles the planner work; revisit if `irate`/`delta` are needed.

### D5. Formulas as a multi-query document

A new top-level document shape: `{ queries: { a: <doc>, b: <doc> }, formulas:
[{ name, expr }], result: "series" }`. Each query must yield `series`. Formula
evaluation is done in the querier after the inner queries, joining on label set
+ timestamp; the expression grammar is `+ - * /`, numbers, query names,
parentheses. Schema version bump. Alternative — formulas in the UI — rejected:
MCP and CLI need the same thing, and it would put query logic in the browser.

### D6. Sequencing

1. Logs tab (no IR change). 2. Discovery everywhere. 3. Trace search.
4. IR counter rate → metrics builder rows with range functions.
5. IR formulas → metrics formulas; delete prom.ts, PromQL tab.
6. Guard + delete remaining compat clients + docs.
Each step is its own PR under 500 lines.

## Risks / Trade-offs

- `describe values` on a high-cardinality unpromoted attribute may be sampled →
  pickers show a partial list (explicit hint, typed values still allowed).
- Removing the raw editors is visible to power users → the IR tab gets a "open
  current view as IR" action so nothing is a dead end.
- Rate semantics differ subtly from PromQL extrapolation → documented; the
  PromQL-agreement test pins the non-extrapolated case.

## Open Questions

- Whether `irate` is needed by any builder preset (assume no until one asks).
