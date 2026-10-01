# Tasks: Materialized Ancestry (sketch — not scheduled)

> The exploration recommends **not** building this now (design.md,
> "Recommendation"). These tasks are the plan to pick up if the revisit
> trigger in design.md fires. Section 0 is done on this branch.

## 0. Exploration (this change)

- [x] 0.1 Collect `_system/_monitoring` trace shapes via the Query IR (span
      count per trace, depth, fan-out)
- [x] 0.2 Benchmark the evaluator kernel vs an ancestry probe on those
      shapes plus deep/wide/bushy stress cases
      (`src/querier/benches/structural_ancestry.rs`)
- [x] 0.3 Record results and the recommendation in design.md

## 1. Cheaper experiments first

- [ ] 1.0 Failing perf guard + fix (`cargo bench -p querier --bench
structural_ancestry -- monitoring`): cut the evaluator's fixed
      per-trace cost on small traces (reuse buffers across traces, no
      per-trace Utf8→Binary cast, linear id lookup below a small span count);
      target the probe's ~7 ms per 10k monitoring traces

- [ ] 1.1 Profile a real `match` query on the largest tenant: split time
      across scan, candidate semi-join, repartition/sort and the evaluator
      (`EXPLAIN ANALYZE`); only continue when sort/evaluator dominate
- [ ] 1.2 Failing test (`cargo test -p querier structural_match`): `child`
      and `sibling` lowered as hash joins on `parent_span_id` give the same
      witnesses as the evaluator; then implement that lowering (no schema
      change)

## 2. Schema (common)

- [ ] 2.1 Failing test (`cargo test -p common schema`): `traces.physical-v6`
      adds nullable `ancestor_ids: list<string>`; an existing v5 table
      evolves additively and old files read it as null
- [ ] 2.2 Add `physical-v6` to `schemas.toml`, keep it off the Flight v1
      wire (storage-only column, writer fills null)
- [ ] 2.3 Verify an older binary (v5 `schemas.toml`) opens a v6 table
      without error (rollback path)

## 3. Compute at compaction (compactor)

- [ ] 3.1 Failing tests (`cargo test -p compactor ancestry`): complete
      chain, parent in the previous hour, parent missing, parent cycle,
      duplicate span, depth over the cap → null where unresolved
- [ ] 3.2 Compute `ancestor_ids` while rewriting a closed hour partition,
      looking up cross-hour parents in the previous hour's rewritten files
- [ ] 3.3 Planner: treat a closed partition with
      `null_value_count(ancestor_ids) > 0` and resolvable spans as a
      rewrite candidate (this is the backfill)
- [ ] 3.4 Self-monitoring: spans resolved / unresolved / over-cap per job

## 4. Query (querier)

- [ ] 4.1 Failing parity tests (`cargo test -p querier structural_match`):
      probe vs evaluator on complete, partially null, cross-window and
      cyclic data
- [ ] 4.2 Lower `descendant`/`ancestor` to unnest + hash join when the
      scan's files report no null `ancestor_ids`; otherwise the evaluator
- [ ] 4.3 Integration coverage in `tests-integration` (writer → compactor →
      querier, before and after compaction)

## 5. Surfaces and docs

- [ ] 5.1 No IR, HTTP, CLI or UI change (strategy is internal); `EXPLAIN`
      names the chosen strategy
- [ ] 5.2 Update `docs/` (storage layout, compaction) via the docs skill and
      the `flight-schemas`/`storage-layout` skills for `physical-v6`
