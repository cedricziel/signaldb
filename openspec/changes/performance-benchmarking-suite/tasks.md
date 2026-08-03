## 1. Read-path benchmark scaffolding

Read benches live in `tests-integration` (not a querier feature-seam): it already depends on `writer` (to seed), `querier`, `common/testing`, `datafusion_iceberg`, and the trace/log/metric generators, so no new dep edges or exposing private querier internals are needed. Read paths are exercised as DataFusion SQL over the registered `DataFusionTable`, mirroring `tests/querier/trace_bloom_pruning.rs`.

- [x] 1.1 Add a `benchmarks` feature (`dep:criterion`) + `[[bench]]` entry (`harness = false`, `required-features = ["benchmarks"]`) to `tests-integration/Cargo.toml`.

## 2. Shared benchmark dataset generation

- [x] 2.1 Reuse the existing `tests_integration::generators` (`generate_traces` / `generate_trace_files_with_ids`, plus `generate_logs` / `generate_metrics`) to seed data via the in-memory Iceberg write path — no new generator needed.

## 3. Querier read-path benchmarks

- [x] 3.1 Write single-trace lookup benchmarks: `trace_lookup_by_id_windowed` (bounded time range, the real Tempo path) → **~11 ms**, and `trace_lookup_by_id_unbounded` (no time filter, regression guard) → **~50 ms**. Diagnosed via `benches/trace_read_analysis.rs`: cost is ~100% `time_elapsed_opening` (file footers), not scanning; time-window partition pruning is the dominant lever (see conversation experiment).
- [x] 3.2 Write the trace-search / groups benchmark (`DISTINCT trace_id` scan). → **~28 ms** median.
- [x] EXPERIMENT (id-lookup speedup): `trace_lookup_by_id_via_index` — a `trace_id → {hour buckets}` point index turns an id-only lookup (no time input) into a bounded, partition-pruned scan. Measured on a SPREAD trace (spans in 3 hours across ~2 days): full-scan 24 ms (complete), ±1h window 3.2 ms (**incomplete: 1/3 spans**), via-index 10 ms (**complete: 3/3**). Bench asserts completeness.
- [x] EXPERIMENT (index self-scaling): `trace_index_scaling` — REAL Parquet index, Hive-partitioned by `trace_id` prefix (256 shards) + bloom on `trace_id`. Lookup vs size: 10k→333µs, 100k→342µs, 1M→367µs (**flat: +10% over 100× growth**, sub-ms). Validates prefix-shard pruning keeps a point lookup ~O(1). Caveat: at 1B, shard files grow (~4M rows/shard at 256 shards) → shard finer (more prefix bytes) to stay flat.
- [ ] 3.3 Write a PromQL metric query benchmark and a LogQL log query benchmark. NOTE: faithful versions need the querier's private PromQL/LogQL engine (a `benchmarks`-gated re-export of `MetricsService`/`LogsService`); raw SQL over the metrics/logs table is only a scan-cost proxy. Decide seam vs proxy before writing.
- [x] 3.4 Run `cargo bench -p tests-integration --features benchmarks --bench querier_read_paths` and confirm both benches execute and report timing.

## 4. Acceptor OTLP-decode benchmark

The decode + OTLP→Arrow conversion code lives in `common` (`common::flight::conversion::otlp_traces_to_arrow`), not the acceptor server, so the bench lives in `common/benches/ingest_and_wal.rs` — no acceptor server/seeding needed.

- [x] 4.1 Add a `benchmarks` feature + `[[bench]]` entry (in `src/common/Cargo.toml`, where the conversion code lives).
- [x] 4.2 Build a fixed 1k-span OTLP request and measure protobuf decode + Arrow conversion, with no WAL/object-store write in the closure. → `otlp_decode_and_convert` **~1.36 ms / 1k spans**; `otlp_convert_only` **~0.80 ms** (decode adds ~0.56 ms).
- [x] 4.3 Run `cargo bench -p common --features benchmarks --bench ingest_and_wal` and confirm timing is reported. NOTE: traces only for now; logs/metrics decode benches are a trivial follow-up using `otlp_logs_to_arrow` / `otlp_metrics_to_arrow`.

## 5. Compaction throughput benchmark

Lives in `tests-integration` (needs compactor + writer + generators to seed). Compaction mutates the table, so the bench uses `iter_custom` to re-seed per iteration outside the timed region and measures only `execute_candidate`.

- [x] 5.1 Add the `compaction` `[[bench]]` entry to `tests-integration/Cargo.toml` (shares the `benchmarks` feature).
- [x] 5.2 Seed `NUM_FILES` small files via `generate_traces`, plan a candidate, time the executor rewrite. → `compactor/rewrite_6_files` **~7.2 ms** (6 files / 600 rows).
- [x] 5.3 Run `cargo bench -p tests-integration --features benchmarks --bench compaction` — timing reported.

## 6. WAL round-trip benchmark

- [x] 6.1 #868 (part of the #865 fix stack) is the rebase base, so WAL framing is settled; the bench targets `record_batch_to_bytes` / `bytes_to_record_batch` as they now stand.
- [x] 6.2 Added the `benchmarks` feature + `[[bench]]` entry to `src/common/Cargo.toml` (shared with the ingest bench).
- [x] 6.3 `wal/record_batch_roundtrip` (serialize + replay of a 1k-row batch) → **~100 µs**. Runs via `cargo bench -p common --features benchmarks --bench ingest_and_wal`.

## 7. Per-PR compile guard

- [ ] 7.1 Add a CI step running `cargo check --benches --features benchmarks` for each crate that actually has benches: `writer` (already has `benchmarks`), `common` (ingest + WAL), and `tests-integration` (querier read + compaction). Acceptor/compactor have no bench targets (their hot-path code is benched from `common`/`tests-integration`).
- [ ] 7.2 Confirm the step fails when a bench target is deliberately broken, then revert the break.

## 8. Nightly run + trend tracking

- [ ] 8.1 Add a nightly-scheduled workflow that runs the full Criterion suite across all bench crates in release mode and emits Criterion JSON output.
- [ ] 8.2 Wire `benchmark-action/github-action-benchmark` to append results to a trend data branch (`GITHUB_TOKEN` with `contents: write` scoped to the job) and render the trend.
- [ ] 8.3 Configure `alert-threshold` (start generous per design Open Question) with regression reporting; leave `fail-on-alert` behind a variance-observation period.

## 9. CI cleanup

- [ ] 9.1 Rename or remove the `performance-benchmark` matrix job in `.github/workflows/test-matrix.yml` so CI naming reflects that it runs ignored load tests, not Criterion.

## 10. Documentation

- [ ] 10.1 Document the local baseline/compare workflow (`--save-baseline` / `--baseline`) and how to read the nightly trend, including the caveat that in-memory benches measure relative regression, not production latency.

## 11. Validation

- [ ] 11.1 Run `openspec validate performance-benchmarking-suite --strict` and resolve any findings.
