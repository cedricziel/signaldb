---
audience: contributor
type: how-to
status: living
sources:
  - src/common/benches/**
  - src/writer/benches/**
  - tests-integration/benches/**
  - scripts/run-benches.sh
  - scripts/render-bench-summary.py
  - .github/workflows/benchmarks.yml
---

# Benchmarking

SignalDB ships a set of [Criterion](https://bheisler.github.io/criterion.rs/book/)
micro-benchmarks covering the hot paths that performance work keeps
touching: OTLP decode, WAL encoding, the Iceberg append, schema
materialization, compaction, and the querier read paths (both raw scan cost
and the full querier service). This page is how to
run them, how to compare a branch against `main`, and where the nightly trend
lives.

> **What the numbers mean.** Every bench runs against in-memory catalogs and
> object stores on a single machine. They measure CPU, planning, encoding,
> and commit-protocol cost — not S3 latency, not network, not a loaded
> cluster. Treat them as a **relative regression signal** ("did this change
> make it worse?"), never as sizing guidance or a production latency claim.

## Latest nightly results

The nightly workflow appends every run to the
[`benchmark-data`](https://github.com/cedricziel/signaldb/tree/benchmark-data)
branch and this table is regenerated from it when the docs site is built.
Full per-benchmark trend charts: [cedricziel.github.io/signaldb/benchmarks](https://cedricziel.github.io/signaldb/benchmarks/).

<!-- bench-summary:start -->

No nightly results have been published yet.

<!-- bench-summary:end -->

## What is benchmarked

All benches live behind a per-crate `benchmarks` feature (`harness = false`
Criterion targets) so they never enter a normal build. CI's clippy step on core PRs
(`--all-targets --all-features`) compiles them, so a bench that stops
building fails CI.

| Crate               | Bench target                  | Measures                                                                                                                                          |
| ------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `common`            | `ingest_and_wal`              | Acceptor CPU per OTLP trace request: protobuf decode + OTLP→Arrow; WAL `record_batch_to_bytes`/`bytes_to_record_batch` round-trip                 |
| `common`            | `signal_decode`               | Same decode + convert for logs and metrics requests                                                                                               |
| `writer`            | `schema_transform_benchmarks` | `transform_trace_v1_to_v2` — the wire→storage materialization plan                                                                                |
| `writer`            | `iceberg_benchmarks`          | `IcebergTableWriter::append_batches_with_marker` across batch sizes, multi-batch commits, and concurrent tenants; writer creation cost separately |
| `tests-integration` | `querier_read_paths`          | Trace lookup by id (unbounded, time-windowed, via a point index, and with/without the Parquet footer cache) and the trace-groups listing over a seeded Iceberg table |
| `tests-integration` | `querier_service_read_paths` | The real querier (`QuerierFlightService::do_get` with router ticket formats): bloom-pruned `find_trace` with and without a time hint, `search_traces`, a PromQL range query and a LogQL line filter through the actual engines |
| `tests-integration` | `compaction`                  | `CompactionExecutor::execute_candidate` rewriting a set of small files                                                                            |
| `tests-integration` | `trace_index_scaling`         | Point lookup against a prefix-sharded, bloom-filtered Parquet index at 10k → 1M traces                                                            |

The inputs come from shared fixtures: `common::testing::sample_trace_request`
and friends for OTLP payloads, and `tests_integration::generators` for seeded
Iceberg tables. Reuse those rather than hand-building data in a new bench.

## Running locally

```bash
# Everything, with Criterion's HTML report under target/criterion/report/
scripts/run-benches.sh

# One crate
scripts/run-benches.sh -p writer

# One target directly
cargo bench -p common --features benchmarks --bench ingest_and_wal
```

`scripts/run-benches.sh` discovers `[[bench]]` targets through
`cargo metadata`, so a new bench only needs its `Cargo.toml` entry. Anything
after `--` is passed to Criterion; a quick smoke run looks like

```bash
scripts/run-benches.sh -p common -- --warm-up-time 0.5 --measurement-time 1 --sample-size 10
```

Close other heavy processes first: the writer and querier benches are
sensitive to CPU contention, and Criterion will happily report a 30% "change"
that is your IDE indexing.

## Comparing a branch against main

Criterion baselines make an A/B comparison two commands:

```bash
git switch main
scripts/run-benches.sh -- --save-baseline main

git switch my-branch
scripts/run-benches.sh -- --baseline main
```

The second run prints each benchmark's change against the saved baseline
with a significance verdict (`No change in performance detected` /
`Performance has improved` / `Performance has regressed`). Baselines are
stored under `target/criterion/<bench>/<baseline>/` and survive `cargo
clean`-free rebuilds; use `--save-baseline` with a branch name so several can
coexist.

For a PR that claims a performance win, paste the relevant `--baseline main`
lines into the description — see #1245 for the pattern.

## Nightly trend

`.github/workflows/benchmarks.yml` runs the full suite once a night (and on
`workflow_dispatch`) in release mode on a single runner, feeds Criterion's
`--output-format bencher` output to
[github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark),
and pushes the accumulated series to the `benchmark-data` branch under
`dev/bench/`. The docs build copies that directory into the site as
[cedricziel.github.io/signaldb/benchmarks](https://cedricziel.github.io/signaldb/benchmarks/) and regenerates the table above with
`scripts/render-bench-summary.py`.

The workflow points `CRITERION_HOME` at the runner's temp directory so
Criterion's own data never lands in the cached `target/`. rust-cache strips
the files but not the directories from `target/criterion/` before saving, and
a restored tree of empty `base/` directories makes Criterion attempt a
baseline comparison, fail, and print the error into the bencher output the
next step parses.

An `alert-threshold` of 150% flags a benchmark whose mean is 1.5× the
previous run in the job summary. It does not fail the workflow yet: shared
runners are noisy, and the threshold needs a few weeks of observed variance
before it becomes a gate.

## Adding a bench

1. Put the file under the owning crate's `benches/` (or `tests-integration/benches/`
   when it needs the writer to seed a table plus the querier or compactor).
2. Add a `[[bench]]` entry with `harness = false` and
   `required-features = ["benchmarks"]`.
3. Use `Throughput::Elements` for per-record work, `iter_custom` when the
   measured operation mutates state that must be rebuilt outside the timed
   region (see `tests-integration/benches/compaction.rs`), and assert on the
   result once up front so the bench cannot silently time an empty scan.
4. Keep the header comment honest about what is and is not measured.
5. Add a row to the table on this page.
