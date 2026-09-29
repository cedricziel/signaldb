## Context

See `proposal.md` — Why. The load-bearing facts that shape the approach:

- `query-ir-core` (merged) already ratifies the logical/physical split at the
  **query door**: queries name logical dotted OTel names, the registry owns each
  field's canonical type, promotion is pure perf, physical column names are
  rejected in queries. The substrate it resolves to, however, is untyped —
  "promoted column **or attribute-JSON extraction**" over `Map<String,String>`.
- Type loss happens at **two** hops, corrected from the first draft. (1)
  `conversion_common.rs` `extract_value` already destroys `BytesValue` (→ UTF-8
  or empty), drops `StringValueStrindex` (→ null), and collapses duplicate/ordered
  keys (serde_json `Map` = BTreeMap) at the OTLP→internal boundary. (2)
  `writer/src/storage/iceberg.rs` `json_strings_to_map_array` then stringifies the
  surviving scalars into `Map<String,String>`. **Int/double/bool and full i64 DO
  survive the JSON-in-Utf8 carrier** (`serde_json` has native i64/u64/f64, no
  `arbitrary_precision`) — the wire is not the culprit for scalars, so phase-1
  typing of scalars from the JSON carrier is feasible; bytes/interned/dup-keys are
  lost earlier and need the `extract_value` fix.
- Two uncoordinated schema systems exist: `query_ir` (logical namespace +
  registry) and `schema_parser`/`schemas.toml` (physical Iceberg schema, with
  `computed`/materialized-label concerns mixed in, versioned v1/v2).
- Parquet keeps **no per-key statistics or bloom filters inside a MAP** (stats/
  blooms are per-leaf: the `key` leaf and the `value` leaf span all keys). So a
  typed map is cast-free but **unprunable**; only a promoted column or a derived
  containment index prunes. The codebase already learned this — `attr_tokens`
  (schema/mod.rs) is a derived tokenized-list column with a bloom, built precisely
  because the map itself does not prune.
- Variant is **not a usable target in this fork**: `PrimitiveType::Variant` exists
  only as a spec enum; it maps to opaque `DataType::Binary`
  (`iceberg-rust-spec/.../arrow/schema.rs`), `Value::try_from_bytes` → `NotSupported`,
  and the shredding/reader/writer suite is `unimplemented!`. DataFusion has no
  Variant type either. Variant is therefore out of scope as a deliverable.
- `attr_demand.rs` already records per-key query demand for the compactor's
  promotion analyzer; #895 bounds Iceberg metadata growth. Both are levers this
  design reuses rather than reinvents.

## Goals / Non-Goals

**Goals:**

- Make "OTel-native" a precise, testable property of the **logical schema**, and
  demote every physical-shape decision (typed maps, promoted columns,
  partitions, ID encodings, per-type metric tables) below the registry so it
  never leaks to a query or dialect surface.
- Give the registry a **typed** physical resolution target so a logical field's
  canonical type is _retrieved_, not _reconstructed by cast_.
- Enforce the canonical type at **write** (ingest through the logical
  schema/registry), so types are stored, not rebuilt at read.
- Establish the invariant tests and the reconciliation of the two schema systems
  as the spine the subsumed fragments hang off.

**Non-Goals:**

- Landing ingest→storage→query in one unit. This is a charter; §Migration Plan
  sequences a dependent PR stack.
- Parquet `Variant` — out of scope as a deliverable (not usable in this fork; see
  Context). The binary residue keeps a future Variant path open.
- Phase-1 fidelity is scoped: the Flight/WAL wire stays JSON-in-Utf8; scalars
  (incl. full i64) and — via the `extract_value` fix — **bytes and interned strings**
  survive it. **Duplicate keys and key order do not** survive a `serde_json::Map`
  round-trip, so that fidelity requires building the binary residue at the acceptor
  _before_ JSON serialization, or the typed-wire phase — it is not delivered by the
  `extract_value` fix alone (corrected per review).
- Cross-version semconv attribute renaming (schema transformation) — hints come
  from one pinned semconv snapshot.
- Redesigning the compaction engine or partition strategy beyond what
  demand-driven typed-column promotion requires.

## Decisions

### D1 — Three doors, one contract: logical schema is the only nativeness surface

Ingest, query, and every dialect bind to one canonical **logical schema**
(resource→scope→signal; dotted OTel names; typed `AnyValue`; log `body` as
`AnyValue`; one metric model; `trace_id`/`span_id`/resource-identity/exemplars
as join keys). The **registry** is the sole logical→physical bridge, consulted
at both write and read. The **physical schema** is free to be arbitrarily
clever/ugly.

_Why:_ the query door already works this way; the failure is that the ingest
door bypasses the contract and the substrate under it is untyped. One contract,
enforced at all three doors, is the whole idea. _Alternative rejected:_
per-dialect logical schemas (status quo) — guarantees drift and re-leaks
physical shape into queries.

### D2 — Type authority: stored value is AnyValue-as-sent; precedence picks the canonical home, never rewrites

The stored value is **always** the `AnyValue` as sent. The registry owns one
canonical type per **(tenant, dataset, field)**, where **`field` is the full
logical identity — signal + attribute level (resource/scope/record) + dotted name**
(so same-named resource and record attributes are distinct fields). This full
identity keys the registry, the resolution cache, and the promoted-column set
alike. Chosen by precedence: (1) config
override; else (2) a **semconv type hint** from a pinned snapshot, selected by the
applicable **resource-/scope-level** `schema_url`; else (3) the observed `AnyValue`
type (first-seen). Precedence only selects which typed home is canonical — it
never coerces/rewrites the sender's value. The canonical type is **monotonic**:
later conflicting data does not retype the field or existing rows.

_Why:_ semconv types are advisory recommendations, not a license for a backend to
irreversibly retype a sender's bytes — coercing-at-write would be data corruption
contradicting the lossless rule (reviewer convergence). OTLP has `schema_url` only
on Resource/Scope and it is usually empty, so tier (3), observed AnyValue, is the
**primary** path in practice, not a fallback. _Alternatives rejected:_ semconv-
coerce-at-write (corrupts data); per-record schema_url typing (no such field
exists; and it makes a field's type a function of each row, which
`query-ir-core`'s single-type literal coercion cannot bind to). Cross-version
semconv **renaming** (schema transformation) is out of scope — hints come from one
pinned snapshot.

### D3 — One canonical home; off-type values go to a lossless binary residue (no multi-home)

A field lives in **exactly one** canonical typed home. A value whose sent type does
not match the canonical type — and every array/kvlist/bytes value — is retained in
a self-describing **binary** residue (CBOR/msgpack), retrievable but not
typed-queryable. A field is **never** scattered across multiple typed homes.

_Why:_ the earlier multi-home + coalesce design was refuted by four reviewers — DF
`coalesce`/`CASE` coerce branches to a common supertype (→ re-stringify) or
`try_cast` back, i.e. exactly the read-time reconstruction this change exists to
abolish, and it made the promotion invariant (D5) false for conflicted keys. One
home makes resolution a pure retrieval and keeps losslessness via the residue.
_Trade-off:_ off-type occurrences are retrievable but not filterable until an
operator repins the type; that is the honest cost of never corrupting the value.

### D4 — Tiered substrate: cold one-home store + binary residue, warm derived index, hot budgeted promotion

- **Cold (lossless).** One canonical typed home per field — a per-type map
  (`<container>_str/_int/_double/_bool`) — plus the binary
  residue for off-type/array/kvlist/bytes.
- **Warm (the only pre-promotion pruning; opt-in per table).** A derived typed
  containment index — a typed generalization of `attr_tokens` (per-type
  `key→value` tokens with a bloom on the list leaf). This is what prunes
  unpromoted equality predicates; the typed map itself does not prune (no
  per-key Parquet stats). The spike measured the token columns at 24–36% of
  storage and +100–266% write cost, so the index ships behind a per-table
  budget/policy, not default-on. Its implementation needs (a) a custom
  footer+bloom pre-filter as a `TableProvider` hook (DataFusion never exploits
  list-leaf blooms for `array_has`), (b) the bloom NDV set explicitly to
  rows-per-row-group × attrs-per-row, and (c) a selectivity gate that skips the
  pre-filter for non-selective predicates.
- **Hot (fast).** Demand-driven promotion creates a redundant typed **copy** of
  one (level, key) home: a column `attr_<level>_<key>` (level resource, scope or
  record) typed as the key's canonical type, added via **Iceberg field-id
  evolution** (ids never reused), with its origin recorded in the column `doc`.
  The per-type map stays the one canonical home and keeps every value. Keys made
  of lowercase alphanumeric segments joined by single `.`/`_` get a readable
  name (`.`→`_`, `_`→`__`); any other key gets
  `attr_<level>_<stem>___<8-hex FNV-1a>`. A name already held by a column of
  another origin is skipped, never retyped. Demand and presence come from the
  per-level `attribute_level_stats` catalog table. Promoted columns share
  `max_labels_per_table` with legacy `label_<key>` columns, which remain only
  for `[schema.materialized_labels]` pins and already-existing columns (the
  compat dialects keep reading them). Demotion drops a column not queried
  within `demote_after_idle`, then the least recently queried ones while the
  table is over budget.

_Why copy, not move:_ demotion stays a metadata-only, lossless column drop; the
writer never needs to know what is promoted; and no file ever holds a key in two
homes mid-transition. _Why per level:_ one level-less column cannot honour the
IR's record→scope→resource precedence for a key sent at several levels (the
old backfill filled resource→scope→record), so each level gets its own column.

_Why not Variant (was "option B"):_ removed from the decision surface — in this
fork Variant is opaque `Binary` with `unimplemented!` shredding and DataFusion has
no Variant type; making it real is a multi-repo, multi-quarter upstream effort, not
a spike. The binary residue is forward-compatible if Variant ever lands. _Why the
warm index:_ "typed maps gain pushdown" is false at the Parquet layer; without the
index the warm tier is an unpruned (if cast-free) scan.

### D5 — Promotion is only ever performance — and now it actually holds

Because D3 gives each field one canonical home, resolution never coalesces across
competing homes, and a promoted column is only a copy of that home (read as
`coalesce(promoted, home)` per level), so promotion cannot change results or
types. The **testable invariant**: identical result set AND types
with all promotion off vs. on — scoped to canonical-typed fields (residue values
are retrievable, a separate axis). This is strictly stronger than `query-ir-core`'s
original "same-result" (value equality over a cast) because there is no cast.

### D6 — Ingest enforces at write; wire stays JSON in phase 1

The acceptor/writer path resolves each attribute's canonical type via the
registry (D2) and encodes into the typed substrate (D4), replacing
`json_strings_to_map_array`. The Flight/WAL wire remains JSON-in-Utf8 as a
transitional carrier (it already preserves JSON types), so **WAL format is
untouched in phase 1** — deliberate, given the WAL-corruption history. Typed
wire is a later, explicitly-BREAKING phase for full fidelity.

Placement stays in the writer: the acceptor can't carry a placement over the
JSON-in-Utf8 wire, and only the writer establishes canonical types. The
acceptor looks types up read-only through a non-blocking cached snapshot and
warns the sender through OTLP `partial_success`, rejecting nothing. Off-type
values are counted once, by the writer, after its commit lands.

### D7 — Reconcile the two schema systems: storage schema becomes the logical schema's physical realization

`schemas.toml`/`schema_parser` is refactored so the physical Iceberg schema is
_derived as the realization of_ the logical schema, with `computed`/promoted/
partition marked as physical-only annotations. Logical evolution (semconv/
`schema_url`) and physical evolution (storage migrations) become two independent
version clocks instead of today's conflated v1/v2 axis.

### D8 — Subsumption mapping (what the fragments become)

- `query-field-discovery` → introspection/discovery is a read over the logical
  schema + registry (available sources, queryable fields as dotted names +
  canonical type, value suggestions). Folded into `otel-native-logical-schema` +
  `attribute-type-authority`; delivery-side tail/pagination remains a later
  stack layer.
- `query-metrics-model` → **folded in here** across two capabilities:
  `metric-native-query` (relation types instant/range/scalar, temporality/
  histogram-aware **operators** — not SQL lowering — vector-matching binop, scalar
  envelope) and `typed-metric-storage` (a typed OTLP metric substrate replacing the
  `data_json` blob: bucket-native histograms, typed temporality/monotonicity,
  first-class exemplar `trace_id`/`span_id`, Summary as passthrough). The metric
  layout **is** reshaped — the blob cannot serve bucket-native quantiles or
  exemplar joins, so "read whatever exists" was untenable (reviewer convergence).
- `query-cross-signal-correlate` → **folded in here** as `cross-signal-correlate`:
  the `correlate` stage, its join keys (incl. exemplars, resource-identity),
  bounded fan-out, time-window scoping, and join-kind taxonomy. Key-encoding
  differences resolve through the one logical key; the pushdown-preserving
  canonicalisation (canonicalise the narrow/winners side) is an execution-layer
  detail left to implementation.
- `query-structural-traces` → **folded in here** as `structural-trace-query`: the
  `match` stage, hierarchical relations (incl. `events`/`links`), and the
  no-silent-depth-cap correctness guarantee. The _execution engine_ choice
  (recursive-CTE vs per-trace evaluator vs materialised ancestry) stays a
  spike-gated implementation task — the spec fixes correctness, not strategy.
- #811 registry epic → its "registry as key→physical source of truth" is
  `attribute-type-authority` + the typed resolution target in `query-ir-core`.

### D9 — Registry consistency: monotonic, per tenant+dataset, cache-invalidated on version bump

The registry's canonical type per (tenant, dataset, field) — `field` being the full
signal+level+name identity from D2 — is monotonic (D2) — so
already-written data never disagrees with a later type; new conflicting values go
to the residue rather than flipping the type. Write-path and plan-path read the
same versioned resolution; a config/schema-version bump is the only mutation and it
invalidates cached resolutions. This closes the "mutable derived source of truth
with no invalidation" hole and prevents cross-tenant type contamination.

**Migration rule for a canonical-type change.** When a config/version bump changes a
field's canonical type, existing rows in the old typed home are **not** retyped in
place (monotonicity). They remain readable through a version-aware read-path
within the typed substrate: an old-home value that safe-casts to the new
canonical type reads as the new type, one that does not reads via the
residue. The compactor migrates old-home values forward on its next pass (to the new
home where lossless, else the residue). The one-home invariant is preserved because
the _registry_ names exactly one canonical home at any version; "old home" rows are
a migration artifact the read-path unifies, not a second live home. A type change is
therefore a forward-only, version-gated event, not a free toggle.

### D10 — Typed metric substrate: one wide `metrics` table plus `metric_exemplars`

The JSON that layer 7 replaces is in storage, not on the wire: the writer
already explodes the wire's `data_json` into one row per point, but keeps
`bucket_counts`, `explicit_bounds`, the exponential-histogram bucket counts,
`quantile_values` and `exemplars` as JSON strings. Layer 7 replaces those
columns; `data_json` stays on the Flight/WAL wire until the typed wire (12.2),
so the WAL stays byte-unchanged (D6).

- **One wide physical table.** The five per-type tables (`metrics_gauge`,
  `_sum`, `_histogram`, `_exponential_histogram`, `_summary`) are replaced by
  one `metrics` table carrying `metric_type` and sparse per-type columns:
  `value`; `count`/`sum`/`min`/`max`; `explicit_bounds` `list<double>` and
  `bucket_counts` `list<int64>`; `scale`, `zero_count`, `zero_threshold`,
  `positive_offset`/`negative_offset` and `list<int64>` bucket counts; the
  Summary's `quantiles`/`quantile_values` as parallel `list<double>`.
  `aggregation_temporality` and `is_monotonic` exist on every row, null where
  OTLP does not define them for the type. The physical layout then matches
  the one logical metric model (7.5) instead of being hidden behind a union.
- **Exemplars in their own table.** `metric_exemplars` holds one row per
  exemplar with flat `trace_id`/`span_id` columns (the hex encoding traces
  use), the exemplar time and value, and `filtered_attributes` as a typed
  attribute container. A `series_id` digest (metric name and type, resource
  identity, instrumentation scope name and version, record attributes),
  stored on both tables, links an exemplar to its series.
  Flat keys make `trace_id` filterable and prunable, which the correlate stage
  (layer 9) joins on. One WAL entry commits to both tables; each table keeps
  its own idempotency marker, so a replay stays duplicate-free.
- **Query surface.** The IR `metrics` source covers every metric type, with
  `metric.type`, `metric.temporality` and `metric.monotonic` as fields; the
  `metrics_histogram` source is removed. Exemplars are a sibling IR source,
  `exemplars` — a sub-entity of the metric model, as span events are of spans,
  not a per-type surface.
- **Cutover.** The new tables are created under new names, so no recreate gate
  is needed. The writer's table reconciler drops (with purge) the five legacy
  tables; their data is not migrated.

### D11 — Metric-native query: point streams, series algebra, one engine for IR and PromQL

Layer 8 decisions (taken 2026-09-29). PromQL stops being a second engine: it
lowers to IR documents (as LogQL and TraceQL already do through `ql-ir`) and the
PromQL evaluator in `querier::query::{promql,metrics}` is deleted within the
layer. The IR grows whatever PromQL needs.

- **Relations.** The range relation is the metric *point stream* itself — the
  `metrics` RowSet with grain `point`, holding `series_id`, `start_timestamp`,
  temporality and type. OTel points already carry what a PromQL range vector
  reconstructs from scrapes (identity, interval, temporality), so there is no
  range-selector construct: a window is a parameter of the operator that reads
  the stream. `where` keeps the stream's identity; `extract`, `aggregate`,
  `topk`/`bottomk`, `order`, `limit` drop it. The instant relation is an
  aligned `Series` (one value per series per evaluation instant). `Scalar` is a
  new relation: one value per evaluation instant, no labels. Feeding a Series,
  a Scalar or an identity-less RowSet to an operator that needs a point stream
  is a validation error (400).
- **Series labels.** A Series produced from a point stream carries the full
  label set of its series: `metric.name`, resource attributes as
  `resource.<key>` (with `service.name` as itself), and point attributes by
  their own key — the IR's logical names, stringified. The relation type
  records the label set as *known* (after a `by`) or *open*. In the querier a
  Series is `(bucket, labels Map<Utf8,Utf8>, value Float64)`; grouping and
  matching go through label-set UDFs (keep / drop / fingerprint / replace /
  join), so `without`, `ignoring` and `label_replace` work on label sets not
  known at plan time. The PromQL surface maps names at its own boundary
  (`__name__` ↔ `metric.name`, `job`/`service_name` ↔ `service.name`, other
  dots ↔ `_`), never inside the model.
- **Evaluation instants.** Metric Series are evaluated at `t = from + k·step`
  and labelled `t`. An instant value is a series' latest point in
  `(t − lookback, t]` (lookback default `5m`); range operators read
  `(t − window, t]`. `offset` shifts the read window back, `at` pins `t`.
  Log/trace/profile aggregates keep epoch-aligned `date_bin` buckets.
- **rate / increase / irate** are one DataFusion window function (UDWF)
  partitioned by `series_id`, ordered by `timestamp`. Cumulative: sum of
  successive differences inside the window; a point whose `start_timestamp`
  moved forward is a reset and contributes its full value; the first point
  contributes its full value only when its `start_timestamp` lies inside the
  window, else it is the baseline; with no `start_timestamp` (0/null — e.g.
  Prometheus remote-write) a value decrease is the reset signal. Delta: the
  sum of the points in the window. `rate = increase / window_seconds`; no
  extrapolation. Gauges and non-monotonic sums are rejected (400, naming
  `delta`/`deriv`) — on the IR and on PromQL.
- **Histograms.** A UDAF merges bucket data across series (explicit bounds
  must match; exponential buckets are downscaled to the smallest scale and
  the largest zero threshold wins, folding buckets inside it into the zero
  count — the OTel SDK merge rule); a scalar UDF interpolates the quantile.
  Explicit: linear within the bucket, as today. Exponential: bucket `i` is
  `(base^i, base^(i+1)]`, `base = 2^(2^−scale)`; rank walk negative (largest
  magnitude first) → zero → positive; exponential interpolation inside a
  bucket, linear across the zero bucket, clamped to OTel `min`/`max` when
  present. Rate mode differences each series' buckets per the rate rules
  above before merging. Summary stays rejected (400).
- **Vector matching** is a `binop` stage whose right operand is a sub-document
  (or a number). It plans as one custom logical node + `ExecutionPlan`
  (the `correlate_cap` pattern) that joins on the fingerprint of the matched
  label set (`on` / `ignoring`), enforces one-to-one or the declared
  `group: left|right` side, and rejects many-to-many and a duplicate output
  label set with a 400. Arithmetic drops `metric.name`; `bool` comparisons
  yield 0/1; `and`/`or`/`unless` are set operations on label sets. A Scalar
  operand broadcasts. `formulas` remain the multi-query convenience.
- **Series algebra stages (`irVersion` 10).** `sample` (point stream → Series:
  latest, rate/increase/irate/delta/idelta/deriv/resets/changes,
  `*_over_time`, over `of` = `metric.value` or `metric.count`/`metric.sum`),
  `reduce` (Series → Series: sum/avg/min/max/count/group/stddev/stdvar/
  quantile/topk/bottomk/count_values, `by` or `without`), `map` (value
  functions incl. math, clamp, round, timestamp, calendar), `labels`
  (replace/join), `filter` (compare with a scalar, optional `bool`), `binop`,
  `sort`, `absent`, `over_time` (a subquery: re-window a Series evaluated at
  its own resolution), `scalar` (Series → Scalar, NaN unless exactly one
  series) and `vector` (Scalar → Series). `histogram_quantile` gains `window`
  and a `histogram_fraction` sibling; `from: "time"` and `from: "constant"`
  are Scalar pseudo-sources. The legacy `aggregate` range functions and
  `histogram_quantile` keep their document shape and run on the same
  operators.
- **Scalar envelope.** `result: "scalar"` returns `points: [[t_ns, value]]`
  with no labels; PromQL maps it to `resultType: "scalar"` (instant) and to a
  label-less matrix (range, as Prometheus does).
- **Result changes (stated, not hidden).** Series identity is `series_id`, not
  (service, materialized labels); rate-mode `histogram_quantile` returns a
  value where it returned NaN for services emitting several series of one
  metric; instant-mode `histogram_quantile` takes each series' latest point
  instead of summing cumulative snapshots; metric Series timestamps move from
  bucket start to evaluation instant; PromQL instant queries use a 5m
  lookback instead of a 1h bucket; exponential-histogram quantiles return a
  value instead of 501; rate/increase/irate over gauges and UpDownCounters
  return 400; PromQL honours `on`/`ignoring`/`group_*` and rejects
  many-to-many; `scalar()`/`time()` return scalars.

## Risks / Trade-offs

- **Warm tier is unpruned without the derived index** → the typed map is cast-free
  but does not prune (no per-key Parquet stats). Mitigation: the warm containment
  index (D4) is in scope as an opt-in, budgeted per-table tier; the perf story is
  index-or-promotion, and the specs de-conflate "cast-free" from "pruned".
- **Small files dominate every layout** → on hive's small flush files every
  layout lands at 140–360 ms/query at 2,000 files, and the typed layout's larger
  footer (+2.0 KB/file) makes it worse than legacy until compacted; compacted it
  is 31.6% smaller and 7–9× faster on map scans. Mitigation: compaction lands
  before, or with, the cutover (Migration Plan layer 4).
- **One-shot cutover abandons pre-cutover data** → the typed layout replaces the
  legacy `Map<String,String>` layout with no coexistence read-path and no
  compactor rewrite (user decision; post-1.0 breaking-changes policy). Tables are
  recreated in the typed layout at cutover; pre-cutover files are not readable by
  the new binary. Accepted because deployments are retention-bounded (default
  30d) — stated openly, not hidden.
- **Off-type values become unfilterable** → D3 keeps them lossless in the residue
  but not typed-queryable until an operator repins the type; that is the accepted
  cost of never corrupting the sender's value.
- **Promotion churn / unbounded live schema** → promotion via Iceberg field-id
  evolution, per-table budget + idle/LRU demotion (D4); metadata-file retention
  rides #895 (which bounds files, not schema width — the budget bounds width).
- **Promoted columns prune only partially** → the compactor backfills promoted
  columns but the writer does not fill them, so row-group pruning on a promoted
  filter works only where the key is present in every row of a row group. Full
  per-file pruning (writer-filled columns plus a scan split by file
  completeness) is a tracked follow-up.
- **Write-path cost is the registry lookup, not the builders** → the per-attribute
  registry resolution on the acceptor path (WAL-corruption-sensitive) needs a
  cache; the benchmark must isolate lookup cost, not just builder count.
- **Correlate/structural correctness under perf bounds** → time-window bounds change
  anti/left-join truth (windowed absence) and fan-out caps must not apply to
  semi/anti; structural "no silent cap" is only met by a per-trace evaluator or
  materialized ancestry, not a recursive CTE. Encoded in the specs, not left to
  implementation.
- **`extract_value` is on the critical path for losslessness** → bytes/interned
  fidelity must be fixed there (an early stack layer), else the "lossless residue"
  claim is false regardless of substrate. Duplicate-key/order fidelity is **not**
  achievable in `extract_value` alone (the JSON-in-Utf8 wire's `serde_json::Map`
  collapses it) — it needs acceptor-side binary residue before serialization, or
  the typed-wire phase.

## Migration Plan

Implemented as a dependent PR stack (charter now, stack later). Layer numbers
match `tasks.md`; results of layer 0 are in `spike/results.md`.

0. **Spike (blocking, done).** Proved the warm containment index, the typed
   layout + field-id promotion evolution through the pinned provider, and
   benchmarked all layouts on real hive data. Verdict: commit the typed layout,
   with the compaction and warm-index conditions below.
1. **`extract_value` fidelity fix:** preserve bytes and interned strings at the
   OTLP boundary (prereq for any losslessness claim). Duplicate-key/order fidelity
   is deferred to acceptor-side binary residue or the typed-wire phase (layer
   12.2), since `serde_json::Map` on the phase-1 wire collapses it.
2. **Logical schema + reconciliation:** declare the canonical logical schema and
   refactor `schema_parser`/`schemas.toml` so physical is its realization (D1, D7).
3. **Type authority + registry consistency:** one canonical type per (tenant,
   dataset, field) via config→semconv-hint→observed, monotonic, cache-invalidated
   (D2, D9).
4. **Tiered substrate + one-shot cutover:** land cold one-home store + CBOR
   residue + opt-in warm index; tables are created/recreated in the typed layout —
   no coexistence read-path, no legacy safe-cast (D3, D4; breaking-changes
   policy). **Depends on compaction** keeping per-table file counts low: on
   uncompacted small flush files the typed layout is slower and larger than
   legacy, so the cutover must not ship ahead of it. Split into several PRs.
5. **Ingest enforcement:** route ingest through the registry to the canonical home
   or residue, replace `json_strings_to_map_array` (D6).
6. **Promotion as pure-perf + invariant test:** typed promotion via
   `attr_demand`/compactor, Iceberg field-id evolution, per-table budget + LRU
   demotion; assert the demote-and-still-correct invariant (D5).
7. **Typed metric substrate** (`typed-metric-storage`): bucket-native histograms,
   typed temporality, exemplar join keys, Summary passthrough — replacing
   `data_json` in the same one-shot cutover. **BREAKING** metric layout.
8. **Metric-native operators** (`metric-native-query`, D11): rate/increase,
   histogram quantiles, vector matching as custom operators over the typed
   substrate; PromQL lowers to the IR and its evaluator is deleted.
9. **Correlate.**
10. **Structural `match`** (per-trace evaluator baseline).
11. **Surface parity, subsumption, docs.**
12. **Later stack layers** (own changes, out of this charter's specs):
    delivery-side tail/pagination; typed wire + WAL.

Rollback is simple under the one-shot cutover: the layout change is
**forward-only by policy**. There is no coexistence read-path and no
compatibility matrix — a pre-cutover binary cannot read post-cutover tables and
vice versa. Rolling back the cutover means redeploying the old binary and
recreating tables in the old layout, losing data written since the cutover — an
accepted cost given the post-1.0 breaking-changes policy and retention-bounded
deployments. Layers that precede any layout change (spike, `extract_value` fix
in behavior-compatible form, logical schema, registry) remain ordinarily
revertable; layers after the cutover (promotion, metrics, operators) evolve the
typed layout via Iceberg schema evolution and roll back with their own feature
flags.

## Open Questions

- ~~Binary residue encoding~~ — resolved by spike 0.2: a top-level `Binary`
  column holding one CBOR document per row (`Map<String,Binary>` is not
  supported by the pinned provider).

- ~~Promoted-column budget and LRU-demotion thresholds~~ — resolved in layer 6:
  `[compactor.attr_promotion]` (`max_labels_per_table`, `demote_after_idle`,
  default `7d`).
