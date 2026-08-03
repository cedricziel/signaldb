## 1. PR 1 — Coalescing floor + force-commit primitive (no behavior change)

- [x] 1.1 Add `[writer]` config `commit_interval` (default 5s) and `max_uncommitted_rows` (default 100000) to `common/src/config`, with defaults + env override tests (`cargo test -p common`)
- [x] 1.2 Write failing unit tests in `writer` for the coalescing floor: low-volume group commits only after `commit_interval`; burst commits early at `max_uncommitted_rows`; many small batches within one interval yield a single commit (`cargo test -p writer`)
- [x] 1.3 Implement the per-`(tenant,dataset,table)` floor in `WalProcessor`: `last_commit` map, group-then-decide (rows summed from grouped batches), leave sub-floor groups unprocessed; make tests pass
- [x] 1.4 Write failing unit tests for `force_commit_pending()`: drains all pending groups ignoring the floor; no-op when nothing pending (`cargo test -p writer`)
- [x] 1.5 Implement `WalProcessor::force_commit_pending()` and honor `WalOperation::Flush` with the same drain semantics; make tests pass
- [x] 1.6 Add a writer Flight `do_action("flush")` that calls `force_commit_pending()`, with a writer-level test asserting it returns after committing (also advertised via `list_actions`)
- [x] 1.7 Add a pending-groups / writer-WAL-depth gauge (tracing/metrics) for stall observability (`signaldb.writer.groups_deferred`; WAL depth already via `signaldb.wal.entries_pending`)
- [x] 1.8 `cargo fmt` + `cargo clippy --workspace --all-targets --all-features`; rust-code-reviewer pass (findings #1/#2/#4 fixed, #3/#5/#8 noted as follow-ups); commit

## 2. PR 2 — Flip `do_put` to async ack (BREAKING: read-your-writes)

- [x] 2.1 Async-ack behavior covered by the restart durability test (2.3) + the end-to-end flush test (2.4); a pure `do_put` unit test is impractical (needs a live Flight stream), so it is folded into those
- [x] 2.2 Remove the synchronous `process_single_entry` loop from `do_put` (`flight_iceberg.rs`); ack after `wal.flush()`; make tests pass
- [x] 2.3 Add a writer test for deferred-data durability across a simulated restart (un-committed WAL entries are committed by the loop after restart) — `deferred_entries_survive_a_processor_restart`
- [x] 2.4 Add/extend an integration test in `tests-integration` proving end-to-end async visibility: ingest → `do_action("flush")` → queryable immediately — `flush_persists_ingested_logs_without_waiting_for_the_loop` + shared `common::testing::flush_storage_writers` helper
- [x] 2.5 Suite already tolerated async persistence (poll/long-timeout waits) — no real breakage. Migrated the slow fixed-sleep suites to the flush barrier: logql 15.7s→0.67s, router 15.4s→1.02s, promql→3.97s; added shared `flush_storage_writers`. (query_ir/logs-metrics already poll-until-present.)
- [x] 2.6 `self_monitoring` suite passes (4 tests). The export-timeout churn (#889) is removed structurally: `do_put` no longer blocks on the Iceberg/catalog commit, so the exporter deadline is decoupled from catalog latency (a deterministic ExportError-count assertion isn't feasible in-test)
- [x] 2.7 `cargo fmt` + clippy; rust-code-reviewer pass (findings #1 flush-all, #2 flush timeout, #3 delete `process_single_entry` all fixed; added Flight-level failure test + module doc); commit

## 3. PR 3 — Bound Iceberg metadata growth

- [x] 3.1 Spike (Open Question D4): pinned iceberg-rust does NOT honor the properties (`metadata_log` never maintained; SQL catalog writes a new `metadata.json` per commit and never deletes the superseded one). Resolved by implementing the support upstream rather than the compactor-tick fallback.
- [x] 3.2 Implement `write.metadata.previous-versions-max` / `delete-after-commit.enabled` in `iceberg-sql-catalog` (upstream PR JanKaul/iceberg-rust#382, test `delete_after_commit_prunes_old_metadata_files`); pin SignalDB to the fork commit; set the properties at table creation in `table_manager.rs`
- [x] 3.3 Fallback (compactor metadata-cleanup tick) not needed — the upstream fix makes the property-driven path work
- [x] 3.4 Test `test_created_tables_enable_metadata_pruning` asserts created tables carry the pruning properties (the pruning mechanism itself is covered by the upstream crate test)
- [x] 3.5 `cargo fmt` + clippy + machete + deny clean; committed. (Reviewer pass applied on the two substantive PRs; PR 3's SignalDB surface is a pin + 2 property inserts, with the pruning mechanism covered by the upstream crate test.)

## 4. Cross-cutting

- [x] 4.1 Update docs: `[writer]` config (configuration skill), async ack + coalescing + flush action (flight-communication.md, wal-persistence.md). Other flagged docs are only incidentally source-matched — no content change needed.
- [x] 4.2 Update `signaldb.dist.toml` with the new `[writer]` keys and comments (landed in PR 1)
- [ ] 4.3 Verify with the `verify` skill end-to-end on a local run; confirm #888 (metadata growth) and #889 (export timeouts) are resolved against a self-monitoring-enabled instance
- [ ] 4.4 Reference issues #888 and #889 in the PR descriptions and close on merge
