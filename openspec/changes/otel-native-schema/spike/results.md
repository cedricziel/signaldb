# Spike results — otel-native-schema layer 0 (task 0.4)

Synthesis of the four spike documents, 2026-08-04. Detail lives in:
`warm-index.md` (0.1), `coexistence.md` (0.2), `data-characterization.md` +
`bench.md` (0.3). Code: `spikes/otel-native-spike/` (standalone crate; demo
binaries `warm_index_demo`, `coexistence_demo`, `bench_demo`, `residue_probe`).

## Verdict: commit the typed layout — with two sequencing conditions

The tiered typed substrate is feasible and pays for itself, **provided**:

1. **Compaction lands before, or with, the layout cutover.** On hive's real
   population (p50 file 9–17 KB, 1.7–132 rows/file, footers 59–74% of bytes),
   every layout is dominated by per-file overhead: all four variants land in
   140–360 ms per query at 2,000 files whether they read 32 MiB or 0.02 MiB,
   and the typed layout's doubled footer (+2.0 KB/file) would add ~166 MB of
   pure footer to `_system/_monitoring/logs` alone. Compacted, the picture
   inverts: the typed layout is **31.6% smaller** than legacy and 7–9× faster
   on map scans. File count beats every layout choice — sequence accordingly.
2. **The warm index ships behind a budget/policy, not default-on**, and its
   layer-4.3 implementation must (a) use a custom footer+bloom pre-filter (a
   `TableProvider` hook — DataFusion 54.1 never exploits list-leaf blooms for
   `array_has`, by construction), (b) set the bloom NDV explicitly to
   rows-per-row-group × attrs-per-row (parquet-rs defaults to row count, which
   under-sizes containment blooms ~14× and collapsed int pruning from 99.7% to
   71% until corrected), and (c) skip the pre-filter for non-selective
   predicates (a common-value probe kept 78% of files and added 84 ms for
   nothing).

## What each task established

**0.1 — warm typed containment index (proven, with caveats).** Per-type binary
tokens (`key ‖ 0x1F ‖ canonical-value-bytes` in `List<Binary>` with a list-leaf
bloom) prune 90–99.7% of files with zero false negatives at ~0.1 ms/file probe
cost. DataFusion source analysis shows native pruning is impossible for
`array_has` (no `LiteralGuarantee`; `List` columns unresolvable to their leaf
by the pruner) — the custom hook is mandatory, not an optimization. Cost
correction from 0.3: bloom bytes are 1.88%, but the token _columns_ are
**24–36% of total storage** — hence the budget/policy condition.

**0.2 — typed layout + promotion evolution through the pinned provider
(viable).** 10/11 probes pass: per-type maps round-trip with typed access and
predicates; promotion adds a typed column via genuine Iceberg field-id
evolution (id continues past the tree max); ONE scan spans pre-/post-promotion
generations with null-fill; projection is field-id-based (not positional);
demotion works. The one failure: `Map<String,Binary>` residue content nulls
through the provider (bytes verified intact on disk; plain-parquet reads serve
them) — the provider's field-id override machinery only reshapes top-level
fields. Mitigation chosen by the bench and validated end-to-end: a **top-level
`Binary` residue column** (one CBOR blob per row). Fixing the fork remains an
option; either way this is a layer-4.2 detail, not a blocker.

**0.3 — benchmark on real hive data (the layout pays, compacted).** 200k real
rows, 83 keys, four layouts × two file-size regimes, 3 reps, seeded:

- **Bare typed map prunes exactly zero files** everywhere — the spec's central
  premise confirmed on production data. What typing buys instead: 6–10× less
  bytes scanned, ~36% smaller attribute payload (homogeneous columns compress
  radically better: residue 28.8×, short strings 54.8×), and correctness (see
  below).
- **Promotion is the only mechanism DataFusion prunes natively** (bloom + stats
  on the promoted column; 33–48× speed-up compacted) — but it prunes _bytes,
  not file-opens_, so it too is neutralized at small-flush file counts.
- **Conflicted key = correctness, not just perf**: the legacy string map
  returns 11,781 rows for `= 42` where only 1,845 integer occurrences exist
  (the 5% off-type OTLP-string `"42"` rows are false positives); the typed
  layout returns exactly the typed matches, with off-type values retrievable
  from the residue. This is the change's thesis made measurable.
- **Residue**: decode is negligible (1.2–1.7 µs/entry); bytes are not (45% of
  the compacted typed layout on this tenant, which stores schema blobs as
  attributes) — never read it unless the query asks.
- **Registry lookup is a non-issue**: 14–48 ns/attribute at hit rate
  (0.2–0.7 µs/row = 2–5% of the write path), 483 ns cold. Implementation notes:
  resolve scope once per batch without allocating; never key a global map by an
  owned composite tuple (10–15× slower).

**0.4 — write-path regression check.** There IS a measurable write cost: V1
typed maps + residue cost **+33%/row** over legacy (5.4 → 7.2 µs/row; ratio
stable across two runs on a loaded machine), V2 promotion adds nothing over V1,
V3's token columns cost +100–266%. In absolute terms 1.8 µs/row against a write
path measured in milliseconds (WAL, Flight, commit) is acceptable for the
correctness and compression it buys; the warm index is the component that must
justify itself per-table — which is condition 2. **Conclusion: no blocking
write-path regression for V1/V2; V3 is opt-in by design.**

## Product bugs found incidentally (file as issues, independent of this change)

1. **`attr_tokens` never row-group-prunes today.** The querier's
   `array_has(attr_tokens, …)` conjunct only row-filters after IO; the bloom
   written via `bloom_filter_property_for_attr_tokens()` is never probed on the
   row-group path (same DataFusion limitation the warm index hits).
2. **`MetricsSet::sum_by_name` silently zeroes `PruningMetrics`** in DataFusion
   54.1 — any SignalDB code reading pruning counters that way reports 0
   unconditionally. (Bit spike 0.1's own demo; corrected in `bench.md`.)
3. **Iceberg metadata backlog on hive**: 54,531 metadata files / 20 GB against
   210 MB of data on jobradar traces — #895 is deployed but the pre-existing
   backlog needs cleanup.

## Feed-forward into the layer stack

- Layer 4.2: residue = top-level `Binary` CBOR column (or fix the fork's nested
  field-id handling first); per-type maps as specced.
- Layer 4.3: custom pre-filter hook; explicit bloom NDV; selectivity gate;
  fix `warm_index_demo`'s metric reading if the demo is kept.
- Layer 6: promotion machinery validated end-to-end (field-id evolution,
  null-fill, demotion) — no unknowns left in the provider for this.
- Compaction sequencing becomes an explicit dependency of the cutover layer —
  reflect in `design.md` Migration Plan when layer 4 is planned in detail.
- Variant confirmed out of scope (unchanged from design).
