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
- [x] 3.3 LogQL/PromQL read benches in `tests-integration/benches/signal_read_paths.rs` (raw-SQL scan/aggregation PROXIES, not the private query engine — labeled as such): `logs_filter_line_proxy` **~35 ms**, `metrics_range_aggregation_proxy` **~28 ms**. SUPERSEDED by `tests-integration/benches/querier_service_read_paths.rs`, which drives the public `QuerierFlightService::do_get` in-process with the router's ticket formats — no private seam needed: `find_trace_by_id` (bloom-only pruning) **~33 ms**, `find_trace_by_id_hinted` **~5 ms**, `search_traces_recent` **~56 ms**, `promql_range_avg_by_service` **~67 ms**, `logql_line_filter` **~70 ms** through the real engines. The SQL proxies were removed. NOTE: faithful versions need the querier's private PromQL/LogQL engine (a `benchmarks`-gated re-export of `MetricsService`/`LogsService`); raw SQL over the metrics/logs table is only a scan-cost proxy. Decide seam vs proxy before writing.
- [x] 3.4 Run `cargo bench -p tests-integration --features benchmarks --bench querier_read_paths` and confirm both benches execute and report timing.

## 4. Acceptor OTLP-decode benchmark

The decode + OTLP→Arrow conversion code lives in `common` (`common::flight::conversion::otlp_traces_to_arrow`), not the acceptor server, so the bench lives in `common/benches/ingest_and_wal.rs` — no acceptor server/seeding needed.

- [x] 4.1 Add a `benchmarks` feature + `[[bench]]` entry (in `src/common/Cargo.toml`, where the conversion code lives).
- [x] 4.2 Build a fixed 1k-span OTLP request and measure protobuf decode + Arrow conversion, with no WAL/object-store write in the closure. → `otlp_decode_and_convert` **~1.36 ms / 1k spans**; `otlp_convert_only` **~0.80 ms** (decode adds ~0.56 ms).
- [x] 4.3 Run `cargo bench -p common --features benchmarks --bench ingest_and_wal` and confirm timing is reported. Logs/metrics decode now covered in `src/common/benches/signal_decode.rs`: logs decode+convert **~1.18 ms/1k**, metrics **~1.14 ms/1k** (convert-only 521 µs / 745 µs).

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

- [x] 7.1 No new step needed: `ci.yml`'s `cargo clippy --workspace --all-targets --all-features -- -D warnings` already type-checks every `[[bench]]` target behind the per-crate `benchmarks` feature (`common`, `writer`, `tests-integration`). Documented on the step itself. Acceptor/compactor have no bench targets (their hot-path code is benched from `common`/`tests-integration`).
- [x] 7.2 Confirmed: a deliberately broken `common/benches/ingest_and_wal.rs` fails `cargo clippy -p common --all-targets --all-features -- -D warnings` with `could not compile common (bench "ingest_and_wal")`; break reverted.

## 8. Nightly run + trend tracking

- [x] 8.1 `.github/workflows/benchmarks.yml`: nightly (`17 3 * * *`) + `workflow_dispatch`, runs every `[[bench]]` target via `scripts/run-benches.sh -- --output-format bencher` (targets discovered through `cargo metadata`), uploads the raw output as an artifact.
- [x] 8.2 `benchmark-action/github-action-benchmark` (`tool: cargo`) appends to the `benchmark-data` branch under `dev/bench/` (`contents: write`, auto-push; the branch is seeded on first run). `docs.yml` re-publishes on `workflow_run` of Benchmarks: copies `dev/bench` into the site at `/benchmarks/` and regenerates a "latest results" table into `docs/contributing/benchmarking.md` via `scripts/render-bench-summary.py`.
- [x] 8.3 `alert-threshold: 150%`, `summary-always`, `fail-on-alert: false` / `comment-on-alert: false` until variance has been observed.

## 9. CI cleanup

- [x] 9.1 Renamed the `performance-benchmark` matrix job in `.github/workflows/test-matrix.yml` to `load-tests` ("Ignored load tests") with a comment pointing at the real Criterion entry points.

## 10. Documentation

- [x] 10.1 `docs/contributing/benchmarking.md` (in the mkdocs nav under Contributing): what is benchmarked, `scripts/run-benches.sh`, `--save-baseline` / `--baseline` workflow, nightly trend + alert semantics, the in-memory/relative-only caveat, and how to add a bench. README's "no published benchmarks" line now links to it.

## 11. Validation

- [x] 11.1 `openspec validate performance-benchmarking-suite --strict` passes.
