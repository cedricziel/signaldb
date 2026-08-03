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

- [ ] 2.1 Write a failing writer test asserting `do_put` returns after WAL flush without an Iceberg commit, and that the background loop commits the entry afterward (`cargo test -p writer`)
- [ ] 2.2 Remove the synchronous `process_single_entry` loop from `do_put` (`flight_iceberg.rs`); ack after `wal.flush()`; make tests pass
- [ ] 2.3 Add a writer test for deferred-data durability across a simulated restart (un-committed WAL entries are committed by the loop after restart)
- [ ] 2.4 Add/extend an integration test in `tests-integration` proving end-to-end async visibility: ingest → data not yet queryable → `do_action("flush")` → queryable
- [ ] 2.5 Migrate existing ingest-then-query tests (promql_queries, logql_queries, router_tempo_endpoints, query_ir_e2e, end_to_end_logs_metrics_tests, writer/querier suites) from `sleep`-based waits to the force-commit primitive (`cargo test -p tests-integration`)
- [ ] 2.6 Verify `_system._monitoring` self-export no longer times out: run the `self_monitoring` integration test; assert no `BatchSpanProcessor.ExportError` churn (`cargo test -p tests-integration self_monitoring`)
- [ ] 2.7 `cargo fmt` + clippy; rust-code-reviewer pass; commit

## 3. PR 3 — Bound Iceberg metadata growth

- [ ] 3.1 Spike (Open Question D4): confirm whether the pinned iceberg-rust honors `write.metadata.previous-versions-max` / `write.metadata.delete-after-commit.enabled`; record the finding
- [ ] 3.2 If honored: set the metadata-pruning table properties at table creation in the writer/catalog path, with a test asserting bounded metadata versions after many commits
- [ ] 3.3 If not honored: add a short-interval snapshot-expiration + metadata-cleanup tick covering the tables; test bounded metadata-version count under continuous ingestion
- [ ] 3.4 Add an integration test simulating sustained writes and asserting the retained table-metadata-version window stays bounded (`cargo test -p tests-integration`)
- [ ] 3.5 `cargo fmt` + clippy; rust-code-reviewer pass; commit

## 4. Cross-cutting

- [ ] 4.1 Update docs via the docs skill: write-path commit model, new `[writer]` config, eventual-visibility semantics, and the flush escape hatch (configuration + architecture pages)
- [ ] 4.2 Update `signaldb.dist.toml` with the new `[writer]` keys and comments
- [ ] 4.3 Verify with the `verify` skill end-to-end on a local run; confirm #888 (metadata growth) and #889 (export timeouts) are resolved against a self-monitoring-enabled instance
- [ ] 4.4 Reference issues #888 and #889 in the PR descriptions and close on merge
