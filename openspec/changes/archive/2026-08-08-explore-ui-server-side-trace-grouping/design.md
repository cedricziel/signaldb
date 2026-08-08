## Context

See proposal.md — Why. What shapes the approach is what the IR endpoint already
does and does not offer, verified against a live deployment (hive,
`_system`/`_monitoring`) while writing this:

- `POST /api/v1/query` accepts `aggregate { by, aggs, step }` with
  `count | sum | avg | min | max | quantile`, plus `order`, `topk`/`bottomk` and
  `limit`, lowered to DataFusion in `querier/src/query/ir_planner.rs`. An
  `aggregate` terminal requires `"result": "table"`; `"rows"` is rejected.
- `by` resolves **span attributes**, not just projected columns: grouping by
  `http.route` returns real route groups. Attribute grouping is therefore not
  blocked on the attribute registry.
- Documents are written in logical dotted names (`span.name`, `service.name`,
  `status.code`) but the response's `columns` come back in **physical** form
  (`span_name`, `service_name`, `status_code`). The client reads results
  positionally or by physical name; it cannot assume the name it sent.
- The `rows` projection for `traces` is a fixed eight columns (`trace_id`,
  `span_id`, `parent_span_id`, `span_name`, `service_name`,
  `start_time_unix_nano`, `duration_nanos`, `status_code`).
- A trace's root span is identified by the sentinel
  `parent_span_id = "0000000000000000"`. It is **not** `""` and not null —
  `eq ""` and `not exists` both return zero rows, which reads as "no data".
- `count` takes no `of` operand: there is no count-distinct. Counting traces
  works only because the root-span predicate yields exactly one row per trace.
- There is no conditional aggregate, and the envelope carries no truncation flag
  or total-group count — only `result`, `window`, `columns`, `rows`.
- An **unresolvable** field answers 200 with a single group labelled `null`
  holding the window total (#1070). `traceFacets.ts` already documents this.

The two other IR-backed surfaces on this tab (`api/traceVolume.ts`,
`api/traceFacets.ts`) establish the pattern: build a `QueryIrRequest`, submit via
`runIrQuery`, map the envelope to a view type. Both are span-grain today.

FDAP version alignment does not bite here: no Rust crate is touched, so no
Arrow/Parquet/DataFusion types are imported and the "use DataFusion's re-exports"
constraint has nothing to apply to. Likewise there is no Flight v1-wire vs
v2-storage transform in scope and no WAL or Iceberg migration — the change is
confined to `src/ui`, and the v1/v2 distinction surfaces only as the physical
column names noted above, which the querier already resolves.

## Goals / Non-Goals

**Goals:**

- Every number in the group table produced by the server over the full window.
- The row budget spent on groups; the browser folds and formats, nothing more.
- Both grains expressible as one predicate difference in the same document, so
  the two code paths do not diverge.

**Non-Goals:**

- Any change to the IR grammar, aggregate functions, envelope or querier
  planner. If this change wants something the IR lacks, it works within the
  limitation and documents it rather than extending the backend.
- Any-span→trace filter semantics — `query-structural-traces` owns that.
- Native dimension discovery — `query-field-discovery` owns that.
- Live tail or pagination of the group set.

## Decisions

### One document per table, grain as a single predicate

The group query is one `table` aggregate. The grain toggle adds or omits exactly
one `where` stage:

```jsonc
{
  "irVersion": 1,
  "from": "traces",
  "range": { "from": "<start_ns>", "to": "<end_ns>" },
  "result": "table",
  "pipeline": [
    // trace grain only:
    {
      "where": {
        "field": "parent_span_id",
        "op": "eq",
        "value": "0000000000000000",
      },
    },
    // active facet filters, ANDed, same mapping traceFacets.ts already uses
    // the RED aggregate — see "RED per group line via a scoped aggregate"
    { "aggregate": { "by": ["span.name", "service.name"], "aggs": [] } },
    { "order": [{ "of": "n", "dir": "desc" }] },
    { "limit": 501 },
  ],
}
```

Alternative considered: two separate builders for the two grains. Rejected — the
grain is one predicate, and duplicating the builder is how the two drift.

### RED per group line via a scoped aggregate

The table's purpose is RED per group: **R**ate, **E**rrors, **D**uration. That is
four numbers on one line — count, rate, error count, percentiles — and it needs a
count over a subset of each group while the other aggregates cover the whole
group. The IR gains an optional predicate on an aggregate:

```jsonc
{
  "fn": "count",
  "as": "errors",
  "where": { "field": "status.code", "op": "eq", "value": "Error" },
}
```

lowering to a DataFusion filtered aggregate — `count(*) FILTER (WHERE
status_code = 'Error')` in SQL terms. The predicate reuses the existing
`Predicate` type and resolver wholesale, so validation, field resolution and
literal coercion are the `where` stage's, not a second grammar.

Alternative considered and rejected: **carry `status.code` as an extra grouping
dimension** and fold the status rows in the view. It needs no backend change, but
it fails on three counts, the last fatally:

- it puts a fold back in the browser, which is the thing this change exists to
  remove;
- it multiplies rows by the status cardinality, so a 500-group table costs ~1500
  rows and `order` then ranks `(dims, status)` rows rather than groups, making
  error counts near the cutoff wrong;
- **percentiles do not recombine.** p95 of a group's `Ok` records and p95 of its
  `Error` records cannot be merged into the group's p95 by any arithmetic the
  browser can do. The best available substitute is the max across status rows,
  which is a different number wearing the p95 label. A RED table whose D is
  quietly wrong is not worth the saved backend work.

Rate stays a client-side division — `count / window_seconds`, the existing
`formatRate()`. That is unit presentation, not aggregation: it introduces no
sampling and no recombination error, and pushing it server-side would mean the
server owning the display unit.

The resulting document is one row per group:

```jsonc
{
  "aggregate": {
    "by": ["span.name", "service.name"],
    "aggs": [
      { "fn": "count", "as": "n" },
      {
        "fn": "count",
        "as": "errors",
        "where": { "field": "status.code", "op": "eq", "value": "Error" },
      },
      { "fn": "quantile", "of": "duration.nanos", "arg": 0.5, "as": "p50" },
      { "fn": "quantile", "of": "duration.nanos", "arg": 0.95, "as": "p95" },
    ],
  },
}
```

with `limit` at `budget + 1` — the status cardinality no longer eats the budget.

### Status matching is a value question, not a schema one

`traceVolume.ts`'s `normalizeStatus()` defends against two spellings (`Error`
and `STATUS_CODE_ERROR`). **Resolved at the source:** the write path maps the
OTLP status enum to a fixed string —
`conversion_traces.rs` turns `2` into the literal `"Error"`, pinned by a unit
test — so persisted `status.code` is only ever `Unspecified`, `Ok`, or `Error`.
`STATUS_CODE_ERROR` never reaches storage; the normaliser is defensive about a
case that does not arise.

The scoping predicate still cannot call a normaliser, so it matches with
`regex` on `(?i)error` rather than `contains "Error"`. `contains` is
case-sensitive and would be correct only by luck; the case-insensitive regex
expresses the same rule `normalizeStatus()` applies and stays correct if a
future ingest path ever admits the longer spelling. A test pins both spellings
against the pattern.

### Sorting is an `order` stage, so a sort change refetches

The fetched page is the top `GROUP_BUDGET` groups **under the current sort**.
Re-sorting that page in the browser would therefore answer a different question
than the header claims: clicking "p95" would surface the slowest of the 500 most
_frequent_ groups, not the 500 slowest. That is a wrong answer dressed as a
cheap one, so the sort control maps to the `order` stage and each sort is a new
query. `max(start_time_unix_nano)` is carried as an aggregate so "Last seen"
remains sortable server-side alongside the RED measures.

Rate is exempt: over a fixed window it is `count / seconds`, strictly increasing
in `count`, so the Rate header orders by `n` and needs no separate aggregate.

Refetching on every sort is what makes the skeleton states below load-bearing
rather than decorative.

### No "Services" column

Today's table lists the distinct services in a group when `service.name` is not
a grouping dimension. That needs `count_distinct` or `array_agg`; the IR has
neither, and `count` explicitly rejects an `of` operand. Rather than approximate
it client-side — which would reintroduce exactly the sampling this change
removes — the column is dropped: a user who wants the breakdown adds
`service.name` as a dimension, and the dimension picker now accepts any
attribute name, so grouping by something else is the general answer.

### Truncation by over-fetch, not by counting groups

The envelope reports no total and there is no count-distinct, so "showing 500 of
N" is not expressible. Instead request one row beyond the budget and treat its
presence as "there are more". The view says the list is truncated without
claiming a total. Alternative considered: a companion `by: []` count — that gives
the record total, not the group total, so it cannot answer the question.

### A companion window-total query, to disarm the unresolvable-dimension trap

A dimension the backend cannot resolve returns one `null` group holding the
window total, which renders as a plausible "(not set)" row covering everything —
a silent wrong answer. A resolvable dimension that some records lack returns a
`null` group **alongside** real groups, which is legitimate.

The two are indistinguishable when every record happens to lack the value. The
view issues one cheap companion aggregate (`by: []`, same filters and grain) for
the window total and treats "exactly one group, labelled null, whose count equals
the window total" as unresolvable, reporting the dimension as unavailable rather
than rendering it. The companion also supplies a denominator for share-of-total
display. Alternative considered: trusting the dimension list — not possible while
the picker derives names from a sample.

### Drill-in as a second document

Selecting a group issues a `rows` query with the group's dimension values added
as `where` equalities, ordered by `start_time_unix_nano` desc and limited. The
fixed eight-column projection is exactly a trace list's needs. `null` dimension
values compile to an `exists` negation rather than `eq null`.

### Deletions

`groupTraces()`, `groupDimensions()` and `percentile()` in `lib/traceGroups.ts`
go, with their tests. `groupKey`/`groupLabel`/`parseGroupBy`/`formatRate` stay —
they are presentation. Leaving the aggregation helpers behind "just in case" is
how the client-side path comes back.

## Risks / Trade-offs

- **Trace-grain filters on child-span fields return nothing** → Accepted and
  specified. The empty table is honest; the alternative is a join this change
  does not own. Mitigation: the grain toggle lets a user switch to spans and see
  the matches. Flag it in the empty state.
- **The IR grows a feature for one UI table** → Mitigated by it being the general
  primitive, not a trace-specific one: "measure over a subset of each group" is
  what every error-rate, cache-miss-ratio and slow-request panel needs, on logs
  and metrics as much as traces. The alternative was a table whose D column is
  wrong.
- **Status spelling varies across deployments** (`Error` vs `STATUS_CODE_ERROR`)
  → A scoping predicate cannot normalise. Mitigated by matching the spelling the
  deployment returns, sharing the decision with `normalizeStatus()`, and
  verifying against a live deployment before the UI task closes. A mismatch shows
  as a permanent zero error count — add a test with both spellings.
- **Three IR queries per view (groups, window total, volume) plus facets** →
  Mitigated by React Query caching on the existing key structure; the group and
  total queries share a key prefix so a grain or filter change invalidates both
  together. Watch p95 of `/api/v1/query` on self-monitoring after rollout.
- **Response columns come back physically named** → Mitigated by reading results
  by index against the requested `by` order, and asserting the column count in a
  test, rather than looking columns up by the logical name that was sent.
- **The root sentinel is a magic string** → It is a wire-format fact, not a UI
  choice; define it once as a named constant with the reasoning in a comment, so
  the next reader does not "fix" it to `""`.

## Migration Plan

Two ordered steps, because the UI depends on the IR feature:

1. **Backend first.** The scoped aggregate lands in `common` + `querier` and is
   additive — an aggregate with no scope validates and lowers exactly as today,
   so every existing document keeps its meaning and the deploy is safe on its
   own, with nothing consuming it yet.
2. **UI second**, once a build carrying step 1 is deployed. No client
   regeneration sits between them: `QueryIrRequest.pipeline` is `Vec<Object>` in
   the OpenAPI schema, so stages are opaque to the generated clients and the UI
   can emit a scoped aggregate without any type change. The ordering constraint
   is purely runtime — a UI sending `where` on an aggregate to a router whose
   querier predates step 1 gets the document rejected at validation, not
   silently ignored.

No persisted state, no data migration, no WAL or Iceberg change. Existing
`?group=` and `?groupBy=` URLs keep working; the grain param is new and absent
means traces, today's behavior. Rollback is a revert of either step — reverting
the UI alone leaves an unused but harmless IR field.

## Open Questions

- Whether the volume chart should follow the grain toggle. It is span-grain
  today and correct as such; making it follow is a small, separable follow-up.

## Verified against a live deployment

An unscoped form of the document above was run against hive
(`_system`/`_monitoring`, 1h window) while writing this design — the scoped
`errors` aggregate does not execute yet, which is what step 1 of the migration
plan builds. It returns `columns: [span_name, service_name, status_code, n, p50,
p95]` — logical names in, physical names out, as described — with 17 group rows,
e.g. `["POST /v1/traces", "signaldb", "Unspecified", 48, 19267602.75,
53107648.35]`. Note the only status value present in that window is
`Unspecified`, so it does **not** confirm the `Error` spelling the scoping
predicate must match; that is the open verification called out above.
The resolver accepts **either** `duration.nanos` or `duration_nanos` for the
quantile operand; the dotted form is used for consistency with the other
dimensions.
