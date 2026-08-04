# Tasks: Compactor Partition-Scoped Lifecycle Rework

Sequenced per design.md: live-set correctness (D5) lands before the default flip (D7); the fork transaction op (D2) is front-loaded because the executor depends on it. Each numbered group is roughly one PR (git-stacked-prs style). TDD throughout: failing test first.

## 1. Orphan-detection live-set correctness (D5, #925, subsumes #475)

- [ ] 1.1 Regression tests: reused-manifest EXISTING files stay live; idle-table live set equals table content (zero candidates); genuinely unreferenced file past grace is a candidate
- [ ] 1.2 Rebuild live-set construction from current-snapshot manifest list ∪ retained snapshots' manifests; remove the snapshot-age filter from detection (`detector.rs`, `manifest.rs`); age knob remains expiration-only
- [ ] 1.3 Stream manifest entries into a path-keyed set (no per-file struct materialization) and add a large-table test bound (#475)
- [ ] 1.4 Make pre-delete re-validation unconditional; remove the `revalidate_before_delete` config key (BREAKING config, loud parse failure) and update `signaldb.dist.toml` + compactor docs

## 2. Manifest-derived partition identity (#930)

- [ ] 2.1 Tests: partition classification works when file paths carry no `timestamp_hour=` component; unclassifiable file is retained and emits metric+log naming table and file
- [ ] 2.2 Replace path parsing with `data_file.partition()` reads in `iceberg/partition.rs` and `retention/enforcer.rs`; delete the path-parsing helpers
- [ ] 2.3 Add `compactor_unclassifiable_files_total` metric and fail-safe (retain) semantics per spec

## 3. Fork transaction API: scoped delta commit (D2 prerequisite)

- [ ] 3.1 Verify/extend the iceberg-rust fork transaction surface with a remove-files/add-files operation validated against an explicit input set (upstream PR to JanKaul/iceberg-rust, pattern of #379/#382); pin the fork rev
- [ ] 3.2 Integration test at the fork boundary: concurrent append to another partition does not conflict; removal of an input file does

## 4. Planner: closed-partition, small-file-count candidacy (D1, D3, #934)

- [ ] 4.1 Tests: hundreds-of-tiny-files partition qualifies; open (current-hour) partition never selected; at-target partition not selected (convergence); `max_files_per_job` caps inputs
- [ ] 4.2 Implement per-partition grouping from manifest partition values; closed-partition gate via `compactor.partition_lateness` (new config, default 2× commit horizon)
- [ ] 4.3 Replace candidacy predicate with small-file-count trigger; delete `min_input_file_size_kb` (BREAKING config); enforce `max_files_per_job`; update dist config + docs

## 5. Executor/rewriter: bounded streaming rewrite (D4, #933)

- [ ] 5.1 Tests: rewrite of one partition leaves other partitions' files byte-identical; peak-memory test with a small `FairSpillPool` budget completes or fails attributably (no OOM); row-count parity preserved
- [ ] 5.2 Scope `rewrite_table` → `rewrite_partition(inputs)`: register only input files, `execute_stream` instead of `collect`, per-partition sort retained
- [ ] 5.3 Build compaction `RuntimeEnv` with `FairSpillPool(compactor.memory_limit_mb)` + spill config (new config keys)
- [ ] 5.4 Roll output files at target _encoded_ size using writer bytes-written feedback; test that merged output approximates target file size

## 6. Commit: delta semantics + typed conflicts (D2, part of #933)

- [ ] 6.1 Tests: commit succeeds while ingest appends concurrently to another partition (no retry starvation); retention dropping the target partition aborts the commit; failed commit leaves output files unreferenced (reclaimable)
- [ ] 6.2 Replace whole-table `replace` with the scoped delta commit; conflict check = input files still live in target partition at commit time
- [ ] 6.3 Replace substring conflict classification with typed errors (also fixes the self-authored verification errors); keep post-commit catalog verification
- [ ] 6.4 Per-table async mutex serializing compaction/retention/expiration loops in-process (D6)

## 7. Defaults flip + release surface (D7, #935)

- [ ] 7.1 Integration test: default config end-to-end — ingest small files → compaction merges → retention expires → orphan cleanup physically deletes within interval+grace
- [ ] 7.2 Flip defaults: `orphan_cleanup.enabled = true`, `dry_run = false`, grace 24h; update CLAUDE.md compactor section, `signaldb.dist.toml`, operations docs; release-note BREAKING behavior + config deletions
- [ ] 7.3 Metrics/observability pass: deferred-open-partition counter, per-job bytes/files in-out, conflict outcomes; verify `compactor_deletion_failures_total` still wired

## 8. Close-out

- [ ] 8.1 Full workspace lint/format/machete; run compactor + tests-integration retention/orphan/partition-drop/snapshot-expiration suites
- [ ] 8.2 Update GitHub: close #925/#930/#933/#934/#935 via PRs, tick epic #952, comment resolution on #475; note the fork-surface addition on #950
