## Why

The traces tab's group table is the last surface on that tab still computed in
the browser. `tempoSearch` fetches at most `limit` traces (default 500, capped 5000) and `groupTraces()` buckets _those_ rows in JS, deriving each group's
count, error count, p50 and p95 from them. Every number in the table therefore
describes the newest N traces, not the selected window: the counts are a sample,
and a group's p95 is the p95 of whatever slice happened to be fetched.

This is the same truncation artefact the volume chart and the facet sidebar
already removed by moving onto the Query IR — both carry comments saying so
explicitly. The table is the holdout, and it is the surface users actually read
numbers off. The goal is that the browser renders rows it is handed and computes
nothing: the row budget should buy ~500 _groups_ aggregated over the whole
window, not 500 spans to group locally.

## What Changes

- The group table is populated by a Query IR `table` aggregate
  (`POST /api/v1/query`) evaluated server-side over the entire window, replacing
  client-side grouping of the Tempo search response.
- `groupTraces()`, `groupDimensions()` and the percentile helper in
  `src/ui/src/lib/traceGroups.ts` are deleted. The view keeps key parsing,
  labelling and formatting — presentation, not aggregation.
- A **grain toggle** selects what a row counts: `traces` adds a root-span
  predicate (`parent_span_id = "0000000000000000"`) so a row counts traces and
  measures end-to-end duration; `spans` omits it so a row counts spans, matching
  the span-level volume chart and facet counts already on the tab. The toggle is
  URL state.
- Each group line carries **RED**: request count and rate, error count, and
  duration percentiles — all from one grouped query.
- The IR's `aggregate` stage gains an **optional per-aggregate scoping
  predicate**, so an error count is `count` scoped to `status.code = Error`
  alongside the unscoped total in the same stage. This is what makes RED
  expressible in one row per group: carrying `status.code` as an extra grouping
  dimension instead would split each group across status rows, and per-status
  percentiles cannot be recombined into a group percentile.
- Group truncation is reported rather than silent: the view requests one row
  more than it displays and says so when the extra row comes back.
- Drilling into a group fetches that group's traces with a second IR query
  filtered to the group's dimension values, replacing the in-memory bucket the
  local grouping produced.
- The dimension picker keeps deriving candidate attribute names from the drill-in
  sample until native field discovery exists (`query-field-discovery`), but any
  chosen dimension is now applied server-side.

Not breaking: no ingest, query-compat, Flight or storage surface changes. The
Tempo search endpoint is unchanged and still backs the drill-in list's trace
detail.

## Capabilities

### New Capabilities

- `explore-ui-trace-grouping`: the traces tab's group table — that its counts and
  latency percentiles are exact for the selected window rather than derived from
  a fetched sample, what one row counts under each grain, how truncation of the
  group set is surfaced, and how drilling into a group narrows to its traces.

### Modified Capabilities

- `query-ir-core`: an aggregate may carry an optional predicate scoping which
  records it consumes, so one grouped query can report a total and a subset
  measure over the same groups. No new stage, aggregate function or envelope.
- `explore-ui-navigation`: the enumerated list of non-signal state kept in the
  query string gains the grouping grain, so a shared link reproduces whether the
  table counts traces or spans.

## Impact

- **common**: `src/common/src/query_ir/stage.rs` (the `Agg` shape) and
  `validate.rs` (validating the scoping predicate against the same resolver the
  `where` stage uses).
- **querier**: `src/querier/src/query/ir_planner.rs` — lowering a scoped
  aggregate to a DataFusion filtered aggregate.
- **router**: no code change; the endpoint forwards the document verbatim. No
  OpenAPI or generated-client change either — `QueryIrRequest.pipeline` is
  declared `Vec<Object>` (stages are opaque at the HTTP boundary, validated by
  the querier), so the `Agg` shape was never in the schema and adding a field to
  it does not move the generated Rust SDK or TypeScript client.
- **src/ui**: `src/features/traces/TracesView.tsx`, `src/lib/traceGroups.ts`
  (aggregation removed), `src/lib/urlState.ts` (grain param), a new
  `src/api/traceGroups.ts` alongside the existing `api/traceVolume.ts` and
  `api/traceFacets.ts`, plus their tests.
- Additive to the IR: an aggregate with no scope behaves exactly as today, so no
  existing document changes meaning. Not breaking — no OTLP, Tempo/LogQL/PromQL,
  Flight wire or Iceberg/WAL surface is touched.
- Interacts with `query-field-discovery` (stub): native dimension discovery
  would replace the sample-derived picker.
- Interacts with `query-structural-traces` (stub): any-span→trace filter
  semantics ("traces having a span where X") are out of scope here and remain
  that change's to deliver.
