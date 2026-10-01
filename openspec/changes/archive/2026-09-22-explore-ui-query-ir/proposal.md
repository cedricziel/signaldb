# Proposal: Explore UI reads only through the Query IR

## Why

CLAUDE.md makes the Query IR (`POST /api/v1/query`) the only first-party read
surface; the Loki/Prometheus/Tempo/Pyroscope APIs exist for external clients and
are lossy by design. The Explore UI still breaks that rule in four places:

- **Logs** — rows, volume histogram and label/value pickers go through
  `api/loki.ts` (LogQL). The Loki wire format flattens attribute scopes into one
  map, which is why the log detail view cannot tell a resource attribute from a
  log attribute.
- **Metrics** — every builder query except a single row with no range function
  and no formula, plus all label/metric-name pickers and label stats, go through
  `api/prom.ts` (PromQL).
- **Traces** — search results come from Tempo `GET /api/search` (TraceQL built by
  `lib/traceFilters.ts`) and the facet key picker from `searchTags`.
- **Profiles** — profile types, services and label pickers come from
  `api/pyroscope.ts` (the flamegraph itself is already on the IR).

Each of these means a UI feature can only be as good as the compat dialect
allows, and a gap in the IR stays hidden because the UI routes around it.

## What Changes

- Logs tab: rows, per-level volume, key and value discovery run on the IR
  (`logs` source, `rows`/`series` results, `describe` metadata). Live tail keeps
  its polling model, now polling the IR.
- Traces tab: search runs on the IR (`traces` source, filter chips compiled to a
  `where` predicate tree instead of TraceQL); facet keys come from `describe`.
- Profiles tab: profile types, services and label pickers come from `describe`.
- Metrics tab: every builder query compiles to the IR. This needs two IR
  extensions that are already on the IR roadmap:
  - a **counter rate** capability (`rate` / `increase` over a `step`, with
    counter-reset handling), on `metrics` and `metrics_histogram`;
  - **formulas** — arithmetic across the named queries of one request.
  Metric-name discovery uses `describe values` on `metric.name`; label stats use
  `describe fields` coverage/cardinality.
- **BREAKING (UI only):** the "edit as text" LogQL box and the raw PromQL tab are
  removed from Explore. The Query IR tab is the text escape hatch. Grafana and
  every compat endpoint are unaffected.
- `api/loki.ts`, `api/prom.ts`, `api/pyroscope.ts` and the Tempo search/tag
  functions in `api/tempo.ts` are deleted; a lint/test guard stops the UI from
  calling a compat prefix again.

Out of scope: live-tail streaming (stays polling), the `correlate` and `match`
IR stages, and any change to the compat APIs themselves.

## Capabilities

### New Capabilities

- `explore-ui-query-surface`: the Explore UI reads all signal data and discovery
  metadata through the Query IR.

### Modified Capabilities

- `query-ir-core`: adds counter rate and cross-query formulas (specified in the
  delta under `explore-ui-query-surface` as the requirements the UI depends on;
  folded into `query-ir-core` on archive).

## Impact

- UI: `src/ui/src/api/*`, `features/{logs,metrics,traces,profiles}`,
  `lib/traceFilters.ts`, `lib/proxiedPaths.ts`, `docs/users/explore-ui.md`.
- IR: `src/query-ir` (new stage/agg + formula document shape, schema version
  bump), `src/querier/src/query/ir_planner.rs`, `docs/users/querying-ir.md`,
  MCP `query_ir` tool description.
- Users who relied on the raw PromQL / LogQL editors in Explore move to the IR
  tab or Grafana.
