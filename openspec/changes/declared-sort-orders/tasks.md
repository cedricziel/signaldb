# Tasks: Declared Sort Orders End-to-End

Fork work first (everything downstream pins it), then declaration, producers, engine, benchmarks. Groups ≈ PRs. TDD throughout.

## 1. Fork: sorting_columns writer support (D4a)

- [ ] 1.1 Fork test: writer properties derived from a table with SortOrder carry `sorting_columns` matching the order; footer of a written file attests it
- [ ] 1.2 Implement `set_sorting_columns` derivation in the fork's writer-properties builder (mirror #379's bloom plumbing); PR upstream to JanKaul/iceberg-rust; pin fork rev in workspace

## 2. Fork: provider output_ordering (D4b)

- [ ] 2.1 Fork test: scan over footer-attested files reports the table SortOrder as output ordering; scan including one unattested file downgrades the claim (Inexact — explicit sort retained)
- [ ] 2.2 Map table SortOrder → `with_output_ordering` in `datafusion_iceberg`, gating per-file claims on footer attestation; PR upstream; pin fork rev

## 3. Table metadata: declare canonical sort orders (D2)

- [ ] 3.1 Tests: new signal tables carry the canonical per-signal SortOrder (incl. profiles `(timestamp, service_name)`); startup add-sort-order on a pre-existing table is idempotent
- [ ] 3.2 Add `with_sort_order` to table creation in `table_manager.rs` with per-signal keys; add startup metadata upgrade for existing tables; remove the profiles no-sort warning path in the compactor by defining its key
- [ ] 3.3 Release-note the BREAKING metadata addition per post-1.0 policy

## 4. Ingest-path sorting (D3)

- [ ] 4.1 Tests: out-of-order batch group persists as a file sorted by the declared key with footer attestation; feature-gated sortedness assertion fires on an unsorted write in test builds (D6)
- [ ] 4.2 Columnar sort (lexsort + take) per commit group in the writer persist path before `write_parquet_partitioned`; write-path benchmark before/after

## 5. Compaction attribution guard

- [ ] 5.1 Regression test: compacted output of a partition with legacy unattested files is fully attested and sorted (convergence; no attribution regression)
- [ ] 5.2 Ensure compactor rewrite consumes the declared SortOrder (not its own hardcoded key list) so D2 stays single-source-of-truth

## 6. Engine enablement + correctness gate (D5, D6)

- [ ] 6.1 Integration test: `ORDER BY timestamp DESC LIMIT n` over a deliberately mixed (attested + legacy) table equals optimization-disabled results exactly — permanent regression test
- [ ] 6.2 Plan-shape test: fully attested range elides the redundant sort; mixed range retains it
- [ ] 6.3 Enable `split_file_groups_by_statistics` in the querier session config
- [ ] 6.4 Benchmark gates: `querier_read_paths.rs` + `trace_read_analysis.rs` before/after, including a recent-first TopK scenario showing files/row-groups skipped

## 7. Close-out

- [ ] 7.1 Full workspace lint/format/machete; integration suites green
- [ ] 7.2 Update GitHub: close #936 via PRs; note fork-delta additions on #950; cross-link plan-shape results on epic #953
