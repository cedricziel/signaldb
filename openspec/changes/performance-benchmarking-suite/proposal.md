## Why

SignalDB ships two Criterion benchmark suites in the `writer` crate, but nothing runs, gates, or tracks them: CI's `performance-benchmark` job runs `cargo test --ignored` (not `cargo bench`) and only on manual dispatch, so the Criterion benches have no baseline, no regression signal, and can silently stop compiling. Meanwhile the active performance work — trace-lookup scan reduction, bloom filters, label materialization — lives on the **query** path, which has zero benchmark coverage. We need a benchmark harness that actually measures the hot paths we are changing and fails loudly when they regress.

## What Changes

- Cover the critical **read** paths the UI exercises with a querier Criterion suite: single-trace lookup, trace search (the trace-groups list), and a PromQL metric and LogQL log query, so UI-perceived query latency has hard numbers.
- Cover the critical **write** paths per component: the acceptor OTLP protobuf → Arrow decode/convert step (hottest ingest step, currently uncovered), the writer Iceberg append (already benched), a WAL serialize/replay round-trip, and the compactor Parquet-rewrite.
- Keep the existing `writer` benches alive: add a per-PR CI step that compiles all benches under `--features benchmarks` so they cannot bit-rot.
- Add a nightly CI job that runs the full Criterion suite (release, single-threaded) and feeds results to `benchmark-action/github-action-benchmark`, tracking the trend over time and alerting when a metric regresses beyond a configured threshold.
- Standardize the baseline/compare developer workflow (`--save-baseline` / `--baseline`) and document it so contributors can check a branch against `main` locally.
- Replace or rename the misleading `performance-benchmark` job in `test-matrix.yml` (it benchmarks nothing via Criterion) so CI naming reflects what actually runs.

## Capabilities

### New Capabilities

- `performance-benchmarking`: The benchmark harness contract — the critical write/read path of each component that must have Criterion coverage (acceptor OTLP decode, writer append, WAL round-trip, querier trace-lookup/search/PromQL/LogQL, compaction throughput), how benches are guarded in CI (per-PR compile check), how they are tracked over time (nightly run + trend storage + regression alert threshold), and the local baseline/compare workflow.

### Modified Capabilities

<!-- None. No OTLP ingest, query surface, Flight schema, or storage-layout behavior changes. -->

## Impact

- **Crates**: `querier`, `acceptor`, `compactor`, and `common` (WAL) each gain a `benches/` dir + `benchmarks` feature gate mirroring `writer`; `writer` (unchanged benches, now compile-checked); workspace `Cargo.toml` (Criterion already declared).
- **CI**: `.github/workflows/` — new per-PR bench compile step, new nightly bench workflow, removal/rename of the current `performance-benchmark` matrix job in `test-matrix.yml`.
- **Docs**: contributor-facing benchmarking workflow (baseline/compare, how to read trend results).
- **Runtime/product code**: none. No OTLP ingest, Tempo/LogQL/PromQL surface, Flight wire schema, or on-disk Iceberg/WAL layout is touched — this is test/tooling infrastructure only.
