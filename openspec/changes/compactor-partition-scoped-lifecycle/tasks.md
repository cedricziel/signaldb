# Tasks: Compactor Partition-Scoped Lifecycle Rework

Sequenced per design.md: live-set correctness (D5) lands before the default flip (D7); the fork transaction op (D2) is front-loaded because the executor depends on it. Each numbered group is roughly one PR (git-stacked-prs style). TDD throughout: failing test first.

## 1. Orphan-detection live-set correctness (D5, #925, subsumes #475)

- [x] 1.1 (landed in #1007) Regression tests: reused-manifest EXISTING files stay live; idle-table live set equals table content (zero candidates); genuinely unreferenced file past grace is a candidate
- [x] 1.2 (landed in #1007) Rebuild live-set construction from current-snapshot manifest list ∪ retained snapshots' manifests; remove the snapshot-age filter from detection (`detector.rs`, `manifest.rs`); age knob remains expiration-only
- [x] 1.3 (landed in #1007) Stream manifest entries into a path-keyed set (no per-file struct materialization) and add a large-table test bound (#475)
- [x] 1.4 Make pre-delete re-validation unconditional; remove the `revalidate_before_delete` config key (BREAKING config, ~~loud parse failure~~ — see note) and update `signaldb.dist.toml` + compactor docs
  - **Design correction:** D5/the task assumed removal would surface as a loud parse failure because "unknown keys already fail config parsing". They do not — neither `common::config::OrphanCleanupConfig` nor the compactor's copy uses `#[serde(deny_unknown_fields)]`, so a leftover key is silently ignored. Adding `deny_unknown_fields` is not a safe drive-by: these structs are also populated through figment's env-var provider. Documented as a breaking change in `docs/operations/compactor/configuration.md` instead; tightening the config structs is worth its own change.
  - **Scope refinement:** re-validation is unconditional before any *real* deletion, and skipped in `dry_run` — a dry run deletes nothing, so there is no delete to guard, and it is the one mode that legitimately runs without a detector attached.

## 2. Manifest-derived partition identity (#930)

- [x] 2.1 (landed in #930) Tests: partition classification works when file paths carry no `timestamp_hour=` component; unclassifiable file is retained and emits metric+log naming table and file
- [x] 2.2 (landed in #930) Replace path parsing with `data_file.partition()` reads in `iceberg/partition.rs` and `retention/enforcer.rs`; delete the path-parsing helpers
- [x] 2.3 (retention path landed in #930; compaction planner now records the same counters — #1017) Add `compactor_unclassifiable_files_total` metric and fail-safe (retain) semantics per spec

## 3. Fork transaction API: scoped delta commit (D2 prerequisite)

- [x] 3.1 (#1017) The fork's `overwrite(files, files_to_overwrite)` already provides scoped remove/add; it panicked on any filtered manifest retaining a survivor (required v2 counts left `None`). Fixed + pin bumped to the combined branch; upstream PR raised. Verify/extend the iceberg-rust fork transaction surface with a remove-files/add-files operation validated against an explicit input set (upstream PR to JanKaul/iceberg-rust, pattern of #379/#382); pin the fork rev
- [x] 3.2 (#1017) `test_overwrite_keeping_surviving_entries_in_filtered_manifest` in the fork (red/green) plus `concurrent_append_does_not_invalidate_the_delta_commit` in signaldb. Integration test at the fork boundary: concurrent append to another partition does not conflict; removal of an input file does

## 4. Planner: closed-partition, small-file-count candidacy (D1, D3, #934)

- [x] 4.1 (#1017; `max_files_per_job` sub-case moot — the key does not exist) Tests: hundreds-of-tiny-files partition qualifies; open (current-hour) partition never selected; at-target partition not selected (convergence); `max_files_per_job` caps inputs
- [x] 4.2 (#1017) Implement per-partition grouping from manifest partition values; closed-partition gate via `compactor.partition_lateness` (new config, default 2× commit horizon)
- [x] 4.3 (small-file trigger landed in #934; `min_input_file_size_kb` and `max_files_per_job` are both absent from the codebase, satisfying "enforced or deleted") Replace candidacy predicate with small-file-count trigger; delete `min_input_file_size_kb` (BREAKING config); enforce `max_files_per_job`; update dist config + docs

## 5. Executor/rewriter: bounded streaming rewrite (D4, #933)

- [x] 5.1 (#1020 added `oversized_partition_stays_within_its_memory_budget`: a 1 MB budget against a 10k-row partition must complete with rows preserved, or fail with an attributable error leaving the live set untouched — never OOM. Note it currently resolves via the success branch, so it pins the no-OOM/no-corruption contract rather than proving a spill occurred.) Tests: rewrite of one partition leaves other partitions' files byte-identical; peak-memory test with a small `FairSpillPool` budget completes or fails attributably (no OOM); row-count parity preserved
- [ ] 5.2 **partial** (#1017: scoped to `rewrite_partition` with a pushed-down partition predicate; still `collect()`s rather than `execute_stream`) Scope `rewrite_table` → `rewrite_partition(inputs)`: register only input files, `execute_stream` instead of `collect`, per-partition sort retained
- [x] 5.3 (#1017; uses `RuntimeEnvBuilder::with_memory_limit` — the same idiom as the querier — rather than constructing `FairSpillPool` directly) Build compaction `RuntimeEnv` with `FairSpillPool(compactor.memory_limit_mb)` + spill config (new config keys)
- [ ] 5.4 Roll output files at target _encoded_ size using writer bytes-written feedback; test that merged output approximates target file size

## 6. Commit: delta semantics + typed conflicts (D2, part of #933)

- [x] 6.1 (#1020 added `delta_commit_aborts_when_its_inputs_are_no_longer_live`: an input file removed underneath the job — the shape a concurrent partition drop leaves — aborts the commit, classifies as a conflict, and creates no snapshot, so any already-written output stays unreferenced and reclaimable) Tests: commit succeeds while ingest appends concurrently to another partition (no retry starvation); retention dropping the target partition aborts the commit; failed commit leaves output files unreferenced (reclaimable)
- [x] 6.2 (#1017) Replace whole-table `replace` with the scoped delta commit; conflict check = input files still live in target partition at commit time
- [x] 6.3 (typed `CommitError::SnapshotConflict` predates this change; `commit_delta` raises it for a mutated input set and keeps post-commit verification) Replace substring conflict classification with typed errors (also fixes the self-authored verification errors); keep post-commit catalog verification
- [ ] 6.4 Per-table async mutex serializing compaction/retention/expiration loops in-process (D6)
  - **Blocked on a premise that does not hold today.** D6 assumes the three loops can overlap in-process. They cannot: `CompactorService::run_lifecycle_loop` is a single spawned task whose `tokio::select!` arms each `.await` their cycle to completion, and exactly one loop is spawned per process (`compactor/src/main.rs:212`, `signaldb-bin/src/main.rs:365`). Compaction, retention, snapshot expiration and lease expiry are already strictly serial, so a per-table mutex added now would guard a concurrency that does not exist.
  - The mutex becomes load-bearing exactly when **#1011** ("run lifecycle cycles as independent tasks so long compaction cannot delay stale-lease expiry") lands — that change is what introduces the overlap D6 is written against. Recommend implementing 6.4 as part of #1011 rather than ahead of it; cross-process safety meanwhile rests on catalog CAS plus D2's input-scoped validation, which #1017 delivered.

## 7. Defaults flip + release surface (D7, #935)

- [ ] 7.1 Integration test: default config end-to-end — ingest small files → compaction merges → retention expires → orphan cleanup physically deletes within interval+grace
- [x] 7.2 (landed in #1008; #1017 updated CLAUDE.md, `signaldb.dist.toml` and the compactor docs for partition scoping) Flip defaults: `orphan_cleanup.enabled = true`, `dry_run = false`, grace 24h; update CLAUDE.md compactor section, `signaldb.dist.toml`, operations docs; release-note BREAKING behavior + config deletions
- [x] 7.3 (#1017) Metrics/observability pass: deferred-open-partition counter, per-job bytes/files in-out, conflict outcomes; verify `compactor_deletion_failures_total` still wired

## 8. Close-out

- [ ] 8.1 Full workspace lint/format/machete; run compactor + tests-integration retention/orphan/partition-drop/snapshot-expiration suites
- [ ] 8.2 Update GitHub: close #925/#930/#933/#934/#935 via PRs, tick epic #952, comment resolution on #475; note the fork-surface addition on #950
