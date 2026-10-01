# Materialized Ancestry for Structural Trace Matching (exploration)

## Why

The `match` stage (`irVersion` 12, spec `structural-trace-query`) runs on a
per-trace evaluator: it prunes to candidate traces, repartitions and sorts the
surviving spans by `(trace_id, start time)`, rebuilds each trace's adjacency in
memory and answers `descendant`/`ancestor` with two linear passes. The
`otel-native-schema` change (task 10.2) and the archived
`query-structural-traces` proposal left one door open: **materialized
ancestry**, an `ancestor_ids` (or path) column written ahead of time so that
`descendant` collapses to a membership test with no recursion and no
per-trace grouping. It touches the trace schema, the writer and an Iceberg
schema migration, so it was deferred as an optional fast path.

This change is an exploration: decide whether that fast path is worth
building, and if so where the column gets computed, using measured trace
shapes and a benchmark of both strategies on the same data. It does not
implement anything.

## What Changes (if adopted; currently NOT recommended)

- **Recommendation: do not build it now.** On `_system/_monitoring` traces
  (1–9 spans, depth ≤ 4, chains) an ancestry probe is ~2.8× faster than the
  evaluator's per-trace kernel (7.3 vs 20.5 ms per 10k traces, decode
  included), but the gap is the evaluator's fixed per-trace overhead
  (~1.7 µs/trace), fixable inside the evaluator with no schema change. On
  bushy traces the probe loses ~1.3×, and on deep chains the column grows
  quadratically (15 MB per 100k spans, 120× slower) unless depth-capped.
  The data queried most — the open hour — can never have complete ancestry,
  so the evaluator must stay as the fallback anyway. design.md has the
  numbers and the reasoning.
- **If revisited** (deep traces at scale, or a profile showing the
  repartition/sort ahead of the evaluator dominates), the shape to build is:
  - a nullable `ancestor_ids: list<string>` column on traces
    (`physical-v6`, additive Iceberg evolution), nearest ancestor first;
  - computed **by the compactor** when it rewrites a closed hour partition,
    never by the writer, and written only for spans whose chain resolves to
    a root (`parent_span_id` empty or zero) inside the trace's data seen so
    far; a span with an unresolved chain, or deeper than a depth cap,
    keeps a null `ancestor_ids` (never a truncated list);
  - a per-file "ancestry complete" marker (Iceberg file-level column
    statistic `null_count(ancestor_ids) = 0`), and a planner rule that
    takes the probe/join path only when every file the scan touches is
    complete for the traces in play, falling back to the per-trace
    evaluator otherwise — never a partial answer;
  - no backfill job: existing files gain the column as null and are
    evaluated as today until compaction rewrites them.

## Capabilities

### New Capabilities

<!-- None. -->

### Modified Capabilities

- `structural-trace-query`: only if adopted — the "Strategy is
  bounded-memory-capable" requirement already names materialized ancestry;
  the delta (sketched under `specs/`) adds the completeness rule: the probe
  path is used only over data whose ancestry is known complete, and gives
  the same answer as the evaluator, including for duplicated spans, parent
  cycles, and spans whose ancestors fall outside the query window.

## Impact

Exploration only. The branch carries a benchmark
(`src/querier/benches/structural_ancestry.rs`) and a `#[doc(hidden)]`
hook (`querier::bench_descendant_masks`) that exposes the evaluator's
per-trace kernel to it; neither changes query results.

If adopted later:

- **common**: `schemas.toml` `traces.physical-v6` adding `ancestor_ids`
  (on-disk Iceberg layout change — **BREAKING-adjacent**, additive, readable
  by older queriers that ignore the column; rollback = stop writing it, the
  column stays and is ignored).
- **compactor**: compute `ancestor_ids` while rewriting a closed hour
  partition, reading the previous hour's ancestry for spans whose parents
  live there.
- **querier** (first, independent of this change): cut the evaluator's
  per-trace fixed cost for small traces.
- **querier**: a second lowering for `descendant`/`ancestor` (unnest +
  hash join) chosen per scan by completeness, with the evaluator as the
  fallback; `child`/`sibling` unchanged.
- **writer**: none (by design — see design.md D2).
- **tests-integration**: parity tests evaluator vs probe on incomplete,
  cross-hour, cyclic and duplicated traces.
