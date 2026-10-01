# Design: Materialized Ancestry (exploration)

## Context

**The current engine.** `src/querier/src/query/structural_match.rs` lowers a
`match` stage in three steps:

1. Flag every span in the windowed traces scan with one boolean per
   span-set, aggregate `bool_or` per `trace_id`, and right-semi-join back so
   only spans of traces where every span-set matched something survive
   (candidate pruning).
2. Hash-repartition on `trace_id` and sort by `(trace_id, start time)` — the
   exec's declared input distribution and ordering.
3. `StructuralMatchExec` buffers one trace at a time (span and byte budget
   enforced before admission), builds a `span_id → node` hash map, cuts
   parent cycles, and answers `descendant`/`ancestor` with one top-down and
   one bottom-up pass, `child` with one pass and `sibling` with a hash of
   `parent_span_id`. O(n) per trace.

`descendant` therefore needs **every** span of a candidate trace, not just the
flagged ones: the intermediate hops carry the chain.

**The storage facts that decide where ancestry could be computed.**

- `traces` is partitioned by `timestamp_hour` and sorted
  `(timestamp, trace_id)` (`TableSchema::sort_key_columns`). A trace is not
  contiguous in a file; it is scattered by start time and can straddle hour
  partitions.
- Span ids are 16-char hex strings (`span_id`, `parent_span_id`: `string`);
  roots carry an empty or all-zero `parent_span_id`, both of which the
  evaluator treats as "no parent in this trace".
- The writer commits per `(tenant, dataset, table)` group on a commit tick;
  one trace's spans land in as many files as the commit ticks and exporters
  that delivered them. OTel SDKs export a span when it **ends**, so children
  usually reach the writer **before** their parents.
- The compactor rewrites exactly one **closed** hour partition per job, after
  `partition_lateness` has elapsed (`src/compactor/src/planner.rs`).
- `unflushed-data-visibility` will make the querier union memtable rows with
  the Iceberg scan; those rows have never been near a compactor.

**Measured trace shapes, `_system/_monitoring`** (Query IR via MCP,
2026-10-01). 24 h: 128,736 spans. Max spans per trace: 8 over 24 h, 9 over
7 d (`aggregate by trace_id count` + `topk`). A 30-minute window
(2,370 spans, 817 traces) pulled as rows (`trace_id, span_id,
parent_span_id`):

| spans/trace | traces | share |
| ----------: | -----: | ----: |
|           1 |    178 |   22% |
|           2 |    274 |   34% |
|           3 |      3 |  0.4% |
|           4 |    199 |   24% |
|           5 |    153 |   19% |
|         6–8 |     10 |  1.2% |

Max depth per trace: 0 (178), 1 (274), 2 (3), 3 (207), 4 (155). Fan-out: of
2,354 parents, 2,344 have one child, 10 have 2–3. Every trace has exactly one
root. These are **chains** of at most 9 spans: router → querier → DataFusion
style request paths.

## Goals / Non-Goals

**Goals:** decide whether materialized ancestry is worth building, pick the
column shape and where it is computed if it is, and state how correctness
survives incomplete ancestry.

**Non-Goals:** implementing any writer, schema, compactor or querier change;
changing the IR (the strategy is invisible to it); `child`/`sibling`
(already a single pass or hash join).

## Decisions

### D1: Column shape — ancestor id list, nearest first

| Shape                                    | Answers `descendant` alone?                                         | Stable as spans arrive?                           | Size per span            |
| ---------------------------------------- | ------------------------------------------------------------------- | ------------------------------------------------- | ------------------------ |
| `ancestor_ids: list<string>`             | yes: `a ∈ ancestor_ids(d)`                                          | yes — a span's list depends only on its own chain | depth × 16 B (+ offsets) |
| materialized path string `root/…/parent` | only via substring/prefix match, not hashable for a set-vs-set join | yes                                               | ~ depth × 17 B           |
| `depth` + `root_span_id`                 | no — prunes (same root, deeper) but cannot decide                   | yes                                               | 8 B + 16 B               |
| interval label `(pre, post)`             | yes: `pre(a) < pre(d) ∧ post(d) < post(a)` (range join)             | **no** — one late span renumbers the whole trace  | 16 B                     |

The list wins: it is the only shape that both decides the relation by hash
probe and never has to be rewritten for a span when _other_ spans arrive.
Interval labels are smaller but are a whole-trace property, which collides
with D2. `depth` is `cardinality(ancestor_ids)` and need not be stored.
Encoding as `list<fixed_size_binary(8)>` would halve raw bytes but forces a
hex decode to join against `span_id`; keep it the same type as `span_id`.

**Cost is quadratic in chain depth.** Σ depth over a chain of n spans is
n(n−1)/2: a 500-span chain stores ~125k ids for 500 spans, a 2,000-span
chain ~2 M. The ancestry computation MUST cap depth (spans deeper than the cap get
null, never a truncated list — truncation would be a silent false negative).

### D2: Compute at compaction, never in the writer

The crux is that a child's parent is usually not there yet when the child is
written:

- **Writer, per batch.** Children end and export first; the parent arrives in
  a later batch, a later WAL entry, often a later commit. A writer-side
  ancestry needs a durable `(trace_id, span_id) → ancestor_ids` lookup over
  everything ever committed — a second index the writer must keep, point-read
  on every span, and keep in step with retention. Parquet is immutable, so a
  child written with partial ancestry cannot be fixed later except by
  rewriting its file — which is what compaction does.
- **Writer, partial + fix-up.** Write what is known, fix at compaction. Every
  child exported before its parent (the common case) gets null anyway, so the
  writer pays the lookup for little coverage. Rejected.
- **Compactor, per closed hour.** A closed partition (hour ended + lateness)
  holds all spans that _started_ in that hour. Parents start before children,
  so a span's chain resolves inside the hour or exits to an earlier hour. The
  job resolves in-hour chains from its own input and, for the chain's first
  out-of-hour parent, reads that parent's `ancestor_ids` from the previous
  hour's (already rewritten) files with a `trace_id IN (…)` filter, prepending
  recursively. Unresolvable chains (parent missing, parent later due to clock
  skew, previous hour not rewritten, cycle, over the depth cap) stay null.

Compaction is the only place with both the whole hour in hand and the
ability to rewrite files. Its cost: every closed traces partition must be
rewritten at least once, including partitions the small-file planner would
otherwise skip (D4), plus one previous-hour lookup per straddling chain.

### D3: Correctness — the evaluator stays, and wins every tie

Incomplete ancestry is not an edge case, it is the default for the data most
queries read: the open hour, the lateness window and (after
`unflushed-data-visibility`) the memtable all have null ancestry. So the fast
path can never replace the evaluator; it can only take traces off it.

- **Fallback granularity.** Per scan: if Iceberg manifest statistics report
  `null_value_count(ancestor_ids) > 0` for any file the `match` scan touches,
  or the scan includes memtable rows, run the evaluator for the whole query.
  Per-trace splitting (probe the complete traces, evaluate the rest, union) is
  possible but needs a trace to be wholly complete, which a scan cannot know
  without reading every span of it — the very cost being avoided. Start with
  per scan.
- **Semantics must match.** Three places the probe and the evaluator could
  disagree, each resolved toward the evaluator's documented behavior:
  - _Window-cut chains._ The evaluator sees only in-window spans, so
    `A → B → C` with `B` outside the window makes `C` a root and `A` not its
    ancestor; the column says otherwise. Because the scan window is on start
    time and starts are monotone along a chain, this needs clock skew between
    services. The probe must restrict to ancestors whose span is in the scan
    (it does, by joining against the in-window anc span-set), and the
    intermediate `B` problem is then the only residue — the spec must pick
    one answer. Recommended: true ancestry; the evaluator is the one to change.
  - _Parent cycles._ The evaluator cuts each cycle at its smallest span id
    within the window; a precomputed list cannot know the window. Spans on a
    cycle get null ancestry → evaluator.
  - _Duplicated spans._ Rows sharing a `span_id` share an ancestry list; the
    hash join emits every row of a matched key, which equals the evaluator's
    "one node, all rows witness".
- **Budget.** The spec's write-time budget for this strategy is the depth cap
  of D1 with a null (fallback) outcome, plus the evaluator's existing budget
  for the fallback traces.

### D4: Schema evolution, migration, backfill, rollback

- `schemas.toml` gains `traces.physical-v6` inheriting v5 with one
  `field_additions` entry `ancestor_ids: list<string>, required = false`.
  `TableManager::ensure_schema_evolved` already applies additive evolution on
  load; old files read the new field id as null, which D3 treats as
  incomplete. The column is storage-only: the Flight v1 wire schema is
  unchanged and the writer fills null via its existing schema coercion. Use
  the Arrow/Parquet types re-exported by DataFusion throughout.
- **Backfill** is not a separate job: the compaction planner treats a closed
  partition whose files report null `ancestor_ids` (and that has resolvable
  spans) as a rewrite candidate. Backfill cost is one rewrite of the
  retained traces history, oldest hour first so cross-hour lookups find
  rewritten predecessors.
- **Rollback.** Stop computing it (config flag) and stop choosing the probe;
  the column stays in the schema, as Iceberg never drops a field id. Needs a
  test that a binary whose `schemas.toml` stops at v5 opens a v6 table.

### D5: What it accelerates, and pushdown

- **`descendant` / `ancestor`**: the probe needs only the two flagged
  span-sets (anc rows keyed by `(trace_id, span_id)`, desc rows with their
  lists), not the intermediate hops, and needs **no repartition or sort** —
  it is an unnest + hash join. That, not the O(n) kernel, is the potential
  win.
- **`child` / `sibling`**: already a hash join on `parent_span_id` in
  principle; they gain nothing from the column. Lowering them as joins is a
  cheaper experiment that needs no schema change (tasks 1.x).
- **Pushdown**: limited. Parquet statistics do not index list contents, so
  `array_has(ancestor_ids, x)` prunes no row groups. With a selective anc
  predicate the plan can collect anc `trace_id`s first and push
  `trace_id IN (…)` into the desc scan — but the table is sorted by
  `(timestamp, trace_id)`, so `trace_id` min/max per row group is wide and
  this prunes little without a bloom filter on `trace_id`. The candidate
  semi-join the evaluator already does gives the same trace-level pruning.

## Benchmark

`src/querier/benches/structural_ancestry.rs`, run with
`cargo bench -p querier --features benchmarks --bench structural_ancestry
[-- <shape>]` (also picked up by `scripts/run-benches.sh`, whose
`--output-format bencher` it honours). It is a plain `harness = false`
timer, not criterion, to keep `querier`'s dependency set unchanged; hosting
the bench in
`tests-integration` (which has criterion) pulls the whole service stack into
a release build.

### Methodology

- **Shapes.** `monitoring`: 10,000 chain traces whose span counts are drawn
  from the measured distribution above (~29k spans, about 5 h of
  `_system/_monitoring`); `deep`: 20 × 500-span chains; `wide`: 10 ×
  10,000 spans, a root with 9,999 children; `bushy`: 20 × 5,000-span random
  recursive trees (depth ≈ ln n). Query: `root >> write` (anc = name `root`,
  desc = name `write`; in `bushy` 5% / 10% of spans).
- **Data.** Each shape is written once to an in-memory Parquet file (ZSTD 3,
  one row group, trace-major order — which favours the evaluator, since real
  files are `timestamp`-major) holding `trace_id, span_id, parent_span_id,
span_name, ancestor_ids`.
- **(a) evaluator** per iteration: decode `trace_id, span_id,
parent_span_id, span_name`; flag both span-sets with an Arrow `eq`;
  concat; split by `trace_id`; call the real per-trace kernel
  (`querier::bench_descendant_masks` → `trace_masks` → `evaluate`, the code
  `finish_trace` runs). Excludes the exec's candidate semi-join, repartition
  and sort, and the exec's `match_max_trace_*` bound checks and memory-pool
  accounting, so it is a **lower bound** on the evaluator's cost.
- **(b) ancestry** per iteration: decode `trace_id, span_id, ancestor_ids,
span_name`; flag; hash the anc rows by `(trace_id, span_id)`; probe each
  desc row's ancestor list. No grouping or sort. A prototype only.
- **decode (a) / decode (b)**: each strategy's floor — decode its four
  columns and flag, nothing else.
- **materialize**: computing `ancestor_ids` for complete traces from
  `(span, parent)` — the write-side cost a compactor would pay.
- Both strategies are asserted to return the same `(anc witnesses, desc
witnesses, matching traces)` before timing. Within a shape all variants
  run alternately, one sample each per round (one warm-up round, then up to
  30 rounds or 20 s), and the median is reported.
- **Environment.** 4-core shared VM with other workers' builds running
  (load average 3.6–4.6 during the runs); `cargo bench` bench profile
  (opt-level 3) with `CARGO_PROFILE_BENCH_LTO=false`,
  `CARGO_PROFILE_BENCH_CODEGEN_UNITS=16`, `CARGO_PROFILE_BENCH_DEBUG=0` to
  keep the build tractable on a shared disk. Medians and min/max are
  recorded; treat differences under ~15% as noise.

### Results

Two consecutive `cargo bench` runs (2026-10-01 07:48 and 07:49 UTC, load
average 3.6–4.6 from other workers' builds). Medians in ms; each cell is
run 1 / run 2. Samples per variant: 30, except `deep` (4; each iteration
takes seconds). Variants are interleaved sample by sample. Run-to-run spread
of the medians is under 10%; per-sample max is up to 1.6× the median under
load, so only differences well above 15% are read as real.

| shape (spans)                    |       evaluator |        ancestry |  decode (a) |    decode (b) |   materialize |
| -------------------------------- | --------------: | --------------: | ----------: | ------------: | ------------: |
| monitoring, 10k traces (28,855)  |     20.5 / 20.6 |   **7.2 / 7.4** |   3.5 / 3.6 |     4.3 / 4.4 |   0.46 / 0.43 |
| deep, 50 × 2,000 chain (100k)¹   | **26.5 / 26.1** |   3,170 / 3,289 | 11.9 / 10.3 | 3,298 / 3,398 | 3,315 / 3,363 |
| wide, 10 × 10,000 fan-out (100k) |     19.4 / 20.3 | **13.3 / 13.8** |   6.0 / 6.3 |     7.1 / 7.3 |   0.94 / 0.87 |
| bushy, 20 × 5,000 random (100k)  | **23.2 / 23.8** |     30.6 / 31.1 |   8.6 / 8.6 |   24.5 / 24.6 |   16.5 / 15.5 |

¹ Measured with the earlier 50 × 2,000 deep shape; the committed bench now
uses 20 × 500 (lighter for a shared nightly runner), not yet re-run.

`decode (a)`/`decode (b)` are each strategy's floor (read its four columns
and flag the span-sets, nothing else). Subtracting them isolates the
matching work:

| shape      | evaluator kernel |      ancestry kernel | per trace (evaluator) |
| ---------- | ---------------: | -------------------: | --------------------: |
| monitoring |           ~17 ms |                ~3 ms |               ~1.7 µs |
| deep       |           ~15 ms | negligible vs decode |               ~300 µs |
| wide       |           ~14 ms |              ~6.5 ms |               ~1.4 ms |
| bushy      |           ~15 ms |                ~6 ms |              ~0.75 ms |

Compressed column sizes (ZSTD 3), and `ancestor_ids` relative to `span_id`:

| shape      | `span_id` | `parent_span_id` | `ancestor_ids` | ratio |
| ---------- | --------: | ---------------: | -------------: | ----: |
| monitoring |    330 KB |           218 KB |         259 KB | 0.78× |
| deep       |  1,068 KB |         1,067 KB |  **15,477 KB** | 14.5× |
| wide       |  1,068 KB |           0.4 KB |         0.5 KB |    ~0 |
| bushy      |  1,068 KB |           666 KB |       1,033 KB | 0.97× |

Both strategies returned identical `(anc witnesses, desc witnesses, matching
traces)` on every shape (asserted before timing).

**Reading the numbers.**

- On the shape this deployment actually has, the probe is **~2.8× faster**
  (20.5 → 7.3 ms per 10k traces). The gap is not the evaluator's algorithm;
  it is ~1.7 µs of fixed cost per trace (a Utf8→Binary cast, two hash maps
  and half a dozen `Vec`s allocated per call) paid on 1–9-span traces. In
  absolute terms it is ~13 ms per ~5 h of `_system/_monitoring` data, and
  the measured evaluator excludes the exec's repartition and sort, which
  the probe would skip (both keep the candidate semi-join).
- On wide traces the probe wins ~1.5×; on random bushy trees the evaluator
  wins ~1.3× because the list column triples decode cost.
- On deep chains the column is **quadratic**: 2,000-span chains store
  ~2 M ancestor ids, 15 MB compressed for 100k spans, and decoding it takes
  3.3 s against the evaluator's 26 ms. Materializing it costs the same again.
  A depth cap is mandatory, and past the cap the evaluator answers anyway.

## Recommendation

**Do not build materialized ancestry now.** Revisit only if a tenant with
deep or very large traces shows `match` queries dominated by the
repartition/sort and evaluator in `EXPLAIN ANALYZE` (task 1.1).

1. The win on today's shape (~13 ms per 5 h of data, before scan, planning
   and the network) comes from per-trace fixed overhead in the evaluator,
   which can be removed in the evaluator itself — reuse buffers across
   traces, skip the cast and hash maps for traces under a few dozen spans
   (a linear scan of ≤ 9 ids beats hashing) — with no schema change, no
   migration and no second engine. That is the follow-up worth doing.
2. The data people query most (the last hour) can never have complete
   ancestry: it is not compacted yet, and writer-side ancestry is ruled out
   by out-of-order arrival (D2). The fast path would serve only historical
   queries, while the evaluator must be kept, budgeted and parity-tested for
   everything else.
3. The cost profile is lopsided: modest gains on shallow/wide traces, a
   loss on bushy trees, and quadratic storage plus a 100× regression on deep
   chains unless capped.

**If it is ever built, build it at compaction**, as D1–D4 describe: a
nullable `ancestor_ids list<string>` written by the compactor for closed
hour partitions, depth-capped with a null outcome, chosen per scan only when
manifest statistics show no null ancestry, and the per-trace evaluator as
the fallback that defines the answer.

## Risks / Trade-offs

- [Two engines for one relation] → every structural bug must be fixed twice
  and parity-tested forever. Mitigation: only adopt behind a measured win.
- [Quadratic storage on deep chains] → depth cap with null outcome.
- [Recent data never benefits] → the fast path serves historical queries
  only; the default UI range (`now-1h`) always falls back.
- [Window-cut semantics differ] → spec must choose; recommended: true
  ancestry.

## Migration Plan

None now. If adopted: ship `physical-v6` and the compactor computation behind
a config flag (default off), backfill by planner rewrite, then enable the
probe lowering behind a second flag once parity tests and a production
`EXPLAIN ANALYZE` show the win. Rollback per D4.

## Open Questions

- Do any tenants have deep traces (hundreds+ spans, depth > 20)? This
  deployment's `_system` traces do not; the decision should be re-run
  against the largest real tenant before closing it for good.
- Is the repartition/sort in front of `StructuralMatchExec` the dominant
  cost of a real `match` query? Not measured here (task 1.1).
- Window-cut chains: true ancestry or in-window ancestry?
