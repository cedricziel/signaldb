# Tasks: Declared Sort Orders End-to-End

Fork work first (everything downstream pins it), then declaration, producers, engine, benchmarks. Groups ≈ PRs. TDD throughout.

## 1. Fork: sorting_columns writer support (D4a)

- [x] 1.1 Fork test: writer properties derived from a table with SortOrder carry `sorting_columns` matching the order; footer of a written file attests it
- [x] 1.2 Implement `set_sorting_columns` derivation in the fork's writer-properties builder (mirror #379's bloom plumbing); PR upstream to JanKaul/iceberg-rust; pin fork rev in workspace

## 2. Fork: provider output_ordering (D4b)

- [x] 2.1 Fork test: scan over footer-attested files reports the table SortOrder as output ordering; scan including one unattested file downgrades the claim (Inexact — explicit sort retained)
- [x] 2.2 Map table SortOrder → `with_output_ordering` in `datafusion_iceberg`, gating per-file claims on footer attestation; PR upstream; pin fork rev

## 3. Table metadata: declare canonical sort orders (D2)

- [x] 3.1 Tests: new signal tables carry the canonical per-signal SortOrder (incl. profiles `(timestamp, service_name)`); startup add-sort-order on a pre-existing table is idempotent
- [x] 3.2 Add `with_sort_order` to table creation in `table_manager.rs` with per-signal keys; add startup metadata upgrade for existing tables; remove the profiles no-sort warning path in the compactor by defining its key
- [x] 3.3 Release-note the BREAKING metadata addition per post-1.0 policy

## 4. Ingest-path sorting (D3)

- [x] 4.1 Tests: out-of-order batch group persists as a file sorted by the declared key with footer attestation; feature-gated sortedness assertion fires on an unsorted write in test builds (D6)
- [x] 4.2 Columnar sort (lexsort + take) per commit group in the writer persist path before `write_parquet_partitioned` (write-path benchmark: numbers pending nightly run, see 6.4)

## 5. Compaction attribution guard

- [x] 5.1 Regression test: compacted output of a partition with legacy unattested files is fully attested and sorted (convergence; no attribution regression)
- [x] 5.2 Ensure compactor rewrite consumes the declared SortOrder (not its own hardcoded key list) so D2 stays single-source-of-truth

## 6. Engine enablement + correctness gate (D5, D6)

- [x] 6.1 Integration test: `ORDER BY timestamp DESC LIMIT n` over a deliberately mixed (attested + legacy) table equals optimization-disabled results exactly — permanent regression test
- [x] 6.2 Plan-shape test: fully attested range elides the redundant sort; mixed range retains it
- [x] 6.3 Enable `split_file_groups_by_statistics` in the querier session config (already on: `[querier.datafusion]` defaults it to `true`, applied in `flight.rs::session_config_from`; verified rather than changed, and covered by 6.1's both-ways assertions)
- [x] 6.4 Benchmark gates: `querier_read_paths.rs` (`declared_ordering` group: recent-first TopK, oldest-first TopK, full ordered scan; attested vs unattested vs `split_file_groups_by_statistics` off, with files reached/pruned/read, row groups, bytes, and sort elision printed from the scan's own metrics) + `trace_read_analysis.rs` (EXPLAIN ANALYZE of both TopK directions), plus `iceberg_benchmarks.rs`'s `ingest_sort` for the write-path cost of 4.2. Numbers recorded in `docs/contributing/benchmarking.md`. Finding: the recent-first `DESC` shape keeps its TopK sort in DataFusion 54 regardless of attestation — its file skipping (56–59 of 60 files pruned) is the TopK dynamic filter on statistics, available to unattested files too; attestation elides the sort for `ASC` requests (oldest-first TopK reaches 4 files instead of 60; a full ordered scan skips the sort of every row). The delta spec's recent-first scenario is corrected accordingly, and `docs/operations/query-ordering.md` no longer tells operators to expect a missing `SortExec` on a `DESC` plan (#1317)

## 7. Close-out

- [x] 7.1 Full workspace lint/format/machete; integration suites green — verified by #1313's CI run (`Check & Lint` = workspace clippy `--all-targets --all-features` plus `cargo fmt --check` and `cargo machete`, `Check (macOS)`, `Test Suite (stable)`, `Generated code up-to-date`, `Semconv live-check`, docs freshness). Recorded against CI rather than local runs: the fleet's disk constraint ruled out a workspace build on the build host, and a `-p <crate> --lib` run does not compile the integration test targets — which is exactly how a clippy lint in a new test reached CI
- [x] 7.2 Update GitHub: fork-delta rationale corrected on #950 and #955 (the whole carried delta had merged upstream; branch rebased, delta is now exactly JanKaul/iceberg-rust#391); plan-shape results cross-linked on epic #953. #936 stays open until 6.4 lands
