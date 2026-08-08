# Compactor Partition-Scoped Lifecycle Rework

## Why

The 2026-08-04 five-expert DataFusion/Arrow/Iceberg audit found the compactor to be the highest-risk component in SignalDB (epic #952). Its core design rewrites the **entire table** per compaction cycle — collecting every live row into an unbounded DataFusion session, then committing a whole-table `replace` that loses the snapshot race against 5-second ingest commits essentially always, orphaning a full duplicate table copy per attempt. Its defaults mean it never actually compacts (`min_input_file_size_kb = 1024` excludes exactly the small files compaction exists to merge, #934) and never reclaims storage (retention/compaction leave physical deletion to orphan cleanup, which defaults to disabled + dry-run, #935). And orphan detection derives its live-file set from snapshot _age_, so live `EXISTING` files in reused manifests — or the whole table, once idle past the age window — become deletion candidates, with only the optional `revalidate_before_delete` flag preventing data loss (#925, P0).

This is also the foundation the `otel-native-schema` charter change stands on: its migration layers 7–8 (attribute-layout rewrite via compaction) assume a compactor that can rewrite incrementally and safely on a live table, which the current whole-table design cannot do.

## What Changes

- **Partition-scoped compaction**: planning, rewrite, and commit operate on one closed `timestamp_hour` partition at a time, never the whole table (#933). Planner, rewriter, and committer change together (the current code explicitly couples their scope).
- **Delta commits**: compaction commits `remove {input files} / add {output files}` instead of whole-table `replace`, so concurrent ingest appends to _other_ files no longer invalidate the commit.
- **Bounded execution**: rewrites stream (`execute_stream`) through a memory-limited `RuntimeEnv`; peak memory is proportional to the compaction unit, not table size.
- **Candidacy that triggers on small files**: small-file _count_ per partition becomes the trigger; the current min-size filter (which excludes small files) is removed/inverted (#934). `max_files_per_job` is either enforced or deleted.
- **Manifest-derived partition identity**: partition values are read from manifest-entry partition structs, never parsed out of file-path strings; unclassifiable files are a loud error, not silently kept (#930). **BREAKING** for any deployment relying on path-layout assumptions (none known).
- **Correct live-file-set definition**: orphan detection derives liveness from the _current snapshot's full manifest list_ (union of retained snapshots' manifests when time-travel retention applies), never from snapshot age (#925). Fixes the idle-table empty-live-set data-loss hazard; subsumes the scalability concern in #475.
- **Defaults that reclaim storage**: with the live-set fix in place, orphan cleanup becomes enabled by default (grace period retained), so a default deployment's retention and compaction actually free bytes (#935). **BREAKING** default-behavior change: deployments that relied on orphan cleanup being off must set `[compactor.orphan_cleanup] enabled = false`.
- **Sorted, declared output**: compacted files continue to be written sorted; declaring that order in table metadata is specified by the companion `declared-sort-orders` change, and this change must not regress it.

## Capabilities

### New Capabilities

- `compaction`: what compaction must guarantee — scoping (closed partitions), candidacy triggers, delta-commit semantics under concurrent ingest, resource bounds, output integrity (row-count parity, sorted output), and observability of skipped/dropped work.
- `lifecycle-reclamation`: what retention + orphan cleanup must guarantee — the authoritative live-file-set definition, physical-reclamation defaults, grace periods, manifest-derived partition classification, and the invariant that logical deletion is eventually followed by physical reclamation.

### Modified Capabilities

_None — no existing spec in `openspec/specs/` covers the compactor today._

## Impact

- **Crates**: `compactor` (planner, rewriter, executor, commit, orphan detector, retention enforcer), `common` (compactor + orphan-cleanup config defaults, iceberg table manager touchpoints), `tests-integration` (retention/orphan/compaction suites).
- **Issues**: implements #933, #925, #930, #934, #935; subsumes #475; epic #952.
- **Dependencies**: JanKaul/iceberg-rust fork transaction API — delta commits need `remove`/`add` file operations scoped narrower than whole-table `replace`; if the fork lacks them, an upstream contribution is part of this change (same pattern as the bloom-filter and metadata-reclamation PRs #379/#382).
- **Interactions**: `otel-native-schema` layers 7–8 (compaction-driven layout rewrites) build on the scoped rewrite primitive; `declared-sort-orders` consumes compaction's sorted output; retention partition drops and compaction commits must serialize against each other on the same table.
- **Config surface**: `[compactor]` candidacy keys change meaning (**BREAKING** config); `[compactor.orphan_cleanup]` default flips to enabled.
