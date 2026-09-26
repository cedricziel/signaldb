## Context

See proposal.md — Why. Relevant current state:

- The IR planner (`querier/src/query/ir_planner.rs`) lowers a document to a
  lazy DataFusion `DataFrame`. Two terminal stages already break the pattern in
  ways we can reuse: `heatmap` builds bucket expressions inline and aggregates
  in-engine; `histogram_quantile` and the `flamegraph` envelope **collect** the
  filtered rows and finish the computation in Rust, then reinject a small
  result. `flamegraph` crosses Flight as a single-row batch holding a JSON
  column (`flamegraph_json` + `truncated`), and the router decodes it into the
  HTTP envelope (`router/src/endpoints/query.rs::to_flamegraph_result`).
- Attribute containers are Arrow `Map<Utf8, Utf8>` columns (a legacy JSON-string
  variant still exists on some `metrics_sum` tables and is reconciled at scan).
  DataFusion is built with `default-features = false, features = ["parquet"]`
  — the `nested_expressions` function set (`unnest`, `map_entries`) is **not**
  compiled in. Existing key enumeration (`trace.rs::get_tags`) already iterates
  map arrays in Rust via `logs::attr_documents`.
- Field resolution goes through `FieldResolver` (registry + `LogicalSchema::
core()`); an unpromoted attribute currently resolves as `string`; `body` is
  retrieval-only.
- FDAP constraint: use only Arrow/Parquet types re-exported by DataFusion for
  any batch/array work in the planner.
- No Flight wire-schema, WAL, or Iceberg layout is touched: the new envelope is
  carried the way `flamegraph` already is (JSON-in-a-batch), and the request
  document is JSON.

## Goals / Non-Goals

**Goals:**

- One scan of the matched records regardless of field count.
- Deterministic output (ordering, sampling, bucketing) for a fixed dataset.
- Bounded memory: caps on fields, values per field, buckets, and per-cohort
  reservoirs, all enforced before or during the fold, never after.
- Reuse the IR's predicate lowering, absent semantics, and resolver unchanged
  for the `selection` predicate and the `fields` list.
- A JSON payload that is self-describing enough for the UI, MCP clients, and
  humans (shares, counts, participation, statistic name).

**Non-Goals:**

- Multi-attribute (conjunctive) explanations à la `DIFF` with `MAX ORDER > 1`;
  single-field contrast only in this change (the payload leaves room: a later
  `order` knob).
- Automatic anomaly detection / selection proposal (MacroBase's classification
  step). The client picks the selection.
- Persisting comparisons or a comparison history.
- Cross-source comparisons (logs vs traces).

## Decisions

### D1 — Compute the contrast in a Rust fold over the filtered stream, not in SQL

**Choice.** The planner lowers everything up to and including the cohort flag in
DataFusion: `where` stages → projection of `[cohort_flag, promoted logical
fields…, attribute-map columns…, extract-derived columns…]` → `execute_stream`.
A `CompareAccumulator` folds each `RecordBatch`: for every row it reads the
cohort flag, then for each promoted column and each `(key, value)` map entry
updates a per-field `FieldAcc { participation[2], values: HashMap<Value,
[u64;2]> | reservoir[2] }`. After the stream ends it ranks, trims, buckets and
serializes.

**Promoted vs map-backed de-duplication.** Promotion adds a `label_<key>`
column but leaves the key in the source attribute map (the compactor
backfills the column from the map). The fold therefore consults the resolver
once per field before the scan and builds a `promoted_keys: HashSet<(container,
key)>`; while walking a map, entries whose `(container, key)` is in that set
are skipped, and the field is read from the promoted column only. Precedence
on disagreement is the promoted column — it is what `where`/`aggregate`
already read, so `compare` cannot disagree with them. Fixtures: (a) equal
values → counted once; (b) conflicting values → promoted value counted, map
value ignored.

**Why.** (a) `unnest(map_entries(...))` needs `nested_expressions`, which the
workspace deliberately does not compile in; enabling it for one stage widens
the binary and the surface for little gain. (b) Per-field caps (`maxValues`,
nominal detection, reservoirs) and the two-cohort layout are awkward as SQL
`GROUP BY key, value, cohort` followed by windowed top-N, and would need a
second aggregation to trim. (c) It is the same shape as `get_tags` and the
`histogram_quantile` collect-then-finish path, so the codebase already accepts
"break the lazy chain at a terminal stage." (d) One scan is guaranteed by
construction.

**Alternative considered.** SQL unnest + `GROUP BY (key, value, cohort)` +
`ROW_NUMBER() OVER (PARTITION BY key ORDER BY cnt DESC)` — cleaner plan, more
parallelism, but needs the nested-functions feature, a second pass for
participation and reservoirs, and still ends in Rust for scoring. Kept as a
fallback if the fold's throughput proves insufficient (see Risks).

### D2 — Cohort flag as a computed column from the lowered `selection` predicate

The `selection` predicate is lowered with the same `lower_predicate` used by
`where`, but into a `CASE WHEN <pred> THEN 1 ELSE 0 END` projection (absent →
`0`, honouring the IR's "only `true` selects" rule). This reuses coercion,
absent semantics, exception-event resolution on traces, and extract-derived
fields without a second code path.

### D3 — Field set: registry ∪ observed map keys, capped

With `fields: ["*"]`, the field set is the union of (i) the source's registered
logical fields that resolve and are filterable, (ii) every attribute key
observed in the map columns during the fold, (iii) extract-derived fields. Keys
are attributed to their container (`resource.` / `scope.` / `log.` / `span.` /
`profile.`) so both the merged view (unqualified name) and the per-container
view can be reported; the merged view is what is compared by default, the
per-container view only when explicitly requested by prefixed name.

The set is capped (default 500 distinct fields). Keys beyond the cap after the
fold are counted, not tracked, and reported as `skipped: {reason: "field-cap",
count}` — never silently. `body` and other retrieval-only fields, and names
the resolver rejects, are `skipped` with `retrieval-only` / `unresolvable`.

### D4 — Dimension vs measure classification

Decided per field at the end of the fold from the canonical value type plus
observed cardinality:

- `string`, `bool` → dimension. Numeric-looking strings stay dimensions (an
  unpromoted attribute is typed string today; the registry-typing work will
  promote these to measures automatically).
- `int64` with ≤ 32 distinct values (HTTP status, retry counts) → dimension,
  ordered numerically.
- `int64`, `float64`, `duration_ns`, `timestamp_ns`, `bytes`(numeric) otherwise
  → measure.
- Any dimension whose distinct count exceeds `max(4·maxValues, 0.5·participants)`
  is **nominal**: reported with distinct counts and participation only, no
  value list, score forced below every non-nominal field. This is what keeps
  `trace.id`/`span.id` from dominating.

### D5 — Ranking statistic

Per field, with `p_sel`, `p_base` = participation and `V_sel`, `V_base` = the
normalized value (or bucket) distributions over participating records:

```
JSD_v = JSD₂(V_sel ‖ V_base)                    // base-2, ∈ [0,1]
JSD_p = JSD₂(Bern(p_sel) ‖ Bern(p_base))        // presence divergence
score = ½ · (p_sel · JSD_v + JSD_p)             // ∈ [0,1]
```

Jensen–Shannon divergence (Lin, 1991) is symmetric, bounded, defined when a
value is absent from one cohort (unlike KL), and comparable across fields of
different cardinality. Multiplying the value term by `p_sel` implements the
"selection barely carries it → cannot outrank" rule. Sort key is
`(p_sel > 0, !nominal, score desc, name asc)`. Per dimension value we also
emit `risk_ratio = share_sel / share_base` and `support = share_sel` so
clients can apply the `DIFF` operator's `MIN SUPPORT` / `MIN RATIO` thresholds
(Abuzaid et al., 2018) client-side. The response carries
`"statistic": "jsd-participation-v1"`; changing the formula bumps that name.

**Alternatives.** χ² / G-test: unbounded, sensitive to cohort size ratio.
Difference of proportions on the top value only: misses multi-value shifts.
Pure risk ratio: explodes on tiny baseline shares (why `DIFF` pairs it with
support). JSD is the closest bounded analogue to what practitioners
converge on for two-cohort field ranking.

### D6 — Measure bucketing and summaries from a deterministic reservoir

Each measure keeps a per-cohort reservoir of `R` values (default 8 192; the
`sample` knob overrides within `[1 000, 65 536]`). Sampling is deterministic
**and order-independent**: it is keyed on a stable per-record identity, not on
scan position. Each source declares its identity columns (`traces`: `trace_id`
+ `span_id`; `logs`: `timestamp` + `trace_id`/`span_id` + a hash of `body`;
`profiles`: `profile.id`; `metrics`: `metric.name` + resource-attribute hash +
`timestamp`), and a record is admitted iff `xxhash64(identity, seed=const)`
falls below a threshold `T` chosen for the target rate. The record cap (Risks)
uses the same admission test over the whole fold; measure reservoirs apply a
second, per-field threshold to bound `R`. Threshold sampling is a
Bernoulli-style filter, so the admitted set is a pure function of the data —
partition order, batch boundaries, and parallelism cannot change it — and the
reservoir stays hard-bounded because a full reservoir raises its threshold to
the current maximum admitted hash (the classic bottom-k sketch), which is also
order-independent. Bucket edges are the
combined reservoir's quantiles (up to 16, deduplicated, `duration_ns` snapped
to 1-2-5 nanosecond boundaries for readability); shares are the reservoir's
per-bucket fractions; `min`/`max`/`median` likewise. When a cohort's count
exceeded `R` the field carries `sampled: true` with both sample sizes.
Duration/timestamp values are emitted as integer nanoseconds like everywhere
else in the IR.

Dimensions never sample: their per-value counts are exact for the cap; values
beyond `maxValues` are trimmed _at serialization_ by keeping the union of the
top-`maxValues` per cohort, and `truncated: true` is set. During the fold the
map is bounded by the nominal threshold from D4 (once exceeded, the field
degrades to nominal counting via a HyperLogLog-free exact `HashSet` capped at
the threshold — beyond that only the flag and counts advance).

### D7 — Envelope transport and types

`common::query_ir` gains `Stage::Compare(Compare)` (`selection: Predicate`,
`fields: Vec<String>` where `["*"]` is the wildcard, `max_values: Option<u32>`,
`sample: Option<u64>`), `ResultEnvelope::Comparison`, and a
`RelationType::Comparison` so legal-stage inference rejects anything after it.
`MAX_IR_VERSION` becomes 4 and the v4 registry lists the stage. Validation
enforces: terminal, envelope match, `fields` projection forbidden, bounds
(`max_values ≤ 200`, explicit field list ≤ 500, `1 000 ≤ sample ≤ 65 536`).
Every emitted float is finite: zero-denominator shares/participation encode
as `0`, `min`/`max`/`median` of an empty cohort as `null`, and `risk_ratio`
with a zero baseline share as `null` — the serializer asserts no
`NaN`/`Infinity` reaches `comparison_json`.

The querier serializes the finished `Comparison` struct (defined in `common`,
`utoipa::ToSchema`) with `serde` into a single-row batch `comparison_json:
Utf8` — the flamegraph precedent — and the router decodes it into
`QueryIrResponse.comparison: Option<Comparison>`. Generated TS/Rust clients
pick the schema up from OpenAPI (`cargo xtask openapi`).

### D8 — UI

A `features/compare/` module: `ComparePanel` (side sheet) driven by a
`useComparison(scope, selection)` hook that builds the IR document from the
tab's active filter predicate + range and calls the generated client. Entry
points: (i) `TraceVolumeHeatmap` gains a drag-to-select overlay emitting
`{durationRange, timeRange}`; (ii) grouped-table rows get a "compare to the
rest" action emitting `{field, value}`. Charts are `DimensionBars` (paired
bars) and `MeasureHistogram` (overlaid), both through the shared `VizTooltip`
per the UI-wide rule. Panel state (`selection`, `fieldFilter`) lives in URL
search params, same as facets. Row actions dispatch to the existing filter /
group-by reducers.

## Risks / Trade-offs

- [Rust fold is single-threaded per query and touches every attribute entry] →
  Bound the input with the existing window guards plus a hard row cap
  (default 2 M matched records; beyond it, records are admitted by the
  identity-hash threshold of D6 uniformly across _both_ cohorts, the response
  states scope `records`, and the record cap takes precedence over the
  measure `sample`). Fold cost is O(rows × entries)
  with hashing on `(key, value)`; measure with the querier `do_get` bench on
  hive-sized data before merging. If it is too slow, D1's SQL alternative is
  the escape hatch without changing the contract.
- [Wildcard field set explodes on high-key-cardinality tenants] → field cap
  (D3) with explicit `skipped` reporting; UI surfaces it.
- [Ranking disagrees with human intuition on skewed cohorts] → the payload
  carries raw shares, counts, risk ratio, and support, so the UI/MCP can
  re-rank or threshold; the statistic is versioned by name.
- [Reservoir sampling makes measure shares approximate] → flagged per field;
  dimensions stay exact.
- [Legacy JSON-string attribute columns] → the fold decodes them via the same
  reconciliation used by `metrics` scans; covered by a test on a JSON-typed
  fixture.
- [Absent-typed attributes (all strings today)] → numeric attributes appear as
  dimensions until registry typing lands; documented, and D4's low-cardinality
  rule keeps them readable.

## Migration Plan

Additive: IR v4, new stage/envelope, new OpenAPI schema; no storage, WAL,
Flight schema, or config migration. Rollback = revert; stored v1–v3 documents
are unaffected. Deploy querier and router together (router must know the
`comparison` envelope to decode it; an old router would return an unknown-
envelope error, not corrupt data).

## Open Questions

- Default `maxValues` (50) and reservoir size (8 192) are guesses; tune on hive
  data during implementation without changing the contract.
- Whether the UI's logs tab should get a heatmap-box entry point (it has no
  latency heatmap today) or only the group-row entry point in this change.
