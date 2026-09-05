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
| `writer`            | `iceberg_benchmarks`          | `IcebergTableWriter::append_batches_with_marker` across batch sizes, multi-batch commits, and concurrent tenants; writer creation cost separately; `ingest_sort` isolates the per-commit-group sort by the declared key on in-order and shuffled input |
| `tests-integration` | `querier_read_paths`          | Trace lookup by id (unbounded, time-windowed, via a point index, and with/without the Parquet footer cache) and the trace-groups listing over a seeded Iceberg table; `recent_first_topk` times `ORDER BY timestamp DESC LIMIT n` over sequential files in every attestation state and prints what each scan opened and pruned (see [Declared sort orders](#declared-sort-orders-what-the-benchmark-shows)) |
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

## Declared sort orders: what the benchmark shows

`querier_read_paths`'s `declared_ordering` group is the measurement gate of
the declared-sort-orders work (#936, #1317). It seeds one traces table of 60
sequential ingest files (1,000 spans each, two minutes apart, one row group
per file, 2.5 MB) twice — once through ingest, so every file attests the
declared `(timestamp, trace_id)` order, and once through the plain write path,
so none does — and runs three ordered shapes against the querier's real
session. Before timing, it prints what each scan did, from DataFusion's own
metrics, because wall-clock alone cannot tell an elided sort from a quiet
machine. These are the numbers from the run that closed #1317 (in-memory
object store, warm footer cache, 4 vCPUs):

| Shape                                      | Files               | Reached | Pruned | Read | Bytes   | Sort   | Time    |
| ------------------------------------------ | ------------------- | ------: | -----: | ---: | ------: | ------ | ------: |
| `ORDER BY timestamp DESC LIMIT 20`         | attested            |      60 |     56 |    4 |  23,641 | kept   | 13.3 ms |
|                                            | attested, split off |      60 |     58 |    2 |  11,820 | kept   | 13.2 ms |
|                                            | unattested          |      60 |     59 |    2 |  11,820 | kept   | 12.0 ms |
| `ORDER BY timestamp ASC LIMIT 20`          | attested            |       4 |      0 |    4 |  23,641 | elided | 10.6 ms |
|                                            | attested, split off |       2 |      0 |    2 |  11,824 | elided | 10.2 ms |
|                                            | unattested          |      60 |     58 |    2 |  11,817 | kept   | 12.1 ms |
| `ORDER BY timestamp ASC` (all 60,000 rows) | attested            |      60 |      0 |   60 | 354,626 | elided | 20.9 ms |
|                                            | attested, split off |      60 |      0 |   60 | 354,626 | elided | 26.7 ms |
|                                            | unattested          |      60 |      0 |   60 | 354,626 | kept   | 25.4 ms |

"Reached" counts files the scan got as far as preparing to open; "pruned"
counts the reached files it then skipped whole on their statistics without
reading them; "read" is the rest, one row group each in this layout; "split
off" is `[querier.datafusion].split_file_groups_by_statistics = false`. The
pruned/read split of a TopK arm moves by one file between runs: the dynamic
filter tightens as partitions race to fill the heap.

What the mechanism columns say:

- **Recent-first (`DESC`) gains nothing from attestation.** DataFusion 54
  cannot serve the reverse of a declared order without a sort, so every arm
  keeps a `SortExec: TopK`. What makes the shape cheap is the TopK's dynamic
  filter: the scan reads files newest-first, fills the heap from the first
  file per group, and prunes the other 56–59 files on statistics. That works
  on any file with statistics, attested or not — which is why the unattested
  arm is not slower.
- **Oldest-first (`ASC`) is where attestation pays.** The request matches the
  declared order, so the attested scan declares it, the sort is elided, and
  the limit stops the scan after the first file of each group: 4 files reached
  instead of 60. On an object store each of those 56 unreached footers is a
  round-trip the query never makes; in memory it is the ~1.5 ms between the
  arms.
- **A full ordered scan elides the sort outright.** Attested: rows stream out
  in file order. Unattested: 60,000 rows are sorted first. The difference is
  the sort itself (~4.5 ms here, and the sort's memory).
- **Every timing includes ~10 ms of planning** — the Iceberg provider reads
  manifests and builds the scan on every query — which is why the ratios
  look small next to the I/O ratios. The rows are the same across arms; the
  bench asserts it.

The write-path cost of the sort that makes attestation possible is
`iceberg_benchmarks`'s `ingest_sort` group: the columnar sort of one commit
group by the table's key, on the same `metrics_gauge` batches
`single_batch_writes` appends. Ingest's usual input arrives close to time
order, which is the cheap case; a shuffled group is the expensive one.

| Rows    | Append (`single_batch_writes`) | Sort, in order | Sort, shuffled |
| ------: | -----------------------------: | -------------: | -------------: |
|   1,000 |                         4.6 ms |          75 µs |         111 µs |
|  10,000 |                        15.3 ms |         748 µs |        1.45 ms |
| 100,000 |                       125.6 ms |        20.0 ms |        35.6 ms |

The sort is 2–5% of the append for the commit groups ingest actually forms
(thousands of rows) and reaches 16% (in order) to 28% (shuffled) only at
100,000-row groups, where the append is already 125 ms. It is bounded by the
group, not the table.

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
