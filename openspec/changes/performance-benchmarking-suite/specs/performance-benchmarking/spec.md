## Purpose

Defines the benchmark harness contract for SignalDB: the critical write and read paths of each component (acceptor ingest decode, writer append, WAL round-trip, querier UI read paths, compaction) that must carry Criterion coverage, how those benches are guarded against bit-rot in CI, how their results are tracked over time to catch regressions, and the workflow a contributor uses to compare a branch against a baseline.

## ADDED Requirements

### Requirement: Write-path benchmark coverage

The benchmark suite SHALL provide Criterion benchmarks for the writer's Iceberg append path, covering single-batch writes across a range of batch sizes and concurrent writes across a range of writer counts.

#### Scenario: Writer append benchmarks are runnable

- **WHEN** a developer runs the writer benchmark suite with the `benchmarks` feature enabled
- **THEN** Criterion executes benchmarks for single-batch append and concurrent append and reports per-benchmark timing and throughput

### Requirement: Query-path benchmark coverage

The benchmark suite SHALL provide Criterion benchmarks for the querier read paths that the UI exercises, covering single-trace lookup by trace ID, trace search (the trace-groups list over a time window), and a representative query for each metric/log surface (PromQL, LogQL).

#### Scenario: Trace-lookup benchmark is runnable

- **WHEN** a developer runs the querier benchmark suite with the `benchmarks` feature enabled against a fixed, generated dataset
- **THEN** Criterion reports timing for single-trace lookup by trace ID

#### Scenario: Trace-search benchmark is runnable

- **WHEN** the querier benchmark suite runs a trace search over a time window against the generated dataset
- **THEN** Criterion reports timing for returning the matching trace groups

#### Scenario: Metric and log query benchmarks are runnable

- **WHEN** the querier benchmark suite runs
- **THEN** it reports timing for at least one PromQL metric query and one LogQL log query against the generated dataset

### Requirement: OTLP ingest decode benchmark

The benchmark suite SHALL provide a Criterion benchmark for the acceptor's OTLP protobuf decode and Arrow conversion step, over representative traces/logs/metrics payloads, independent of downstream WAL and Iceberg writes.

#### Scenario: OTLP decode benchmark is runnable

- **WHEN** a developer runs the acceptor benchmark suite with the `benchmarks` feature enabled against a fixed OTLP payload
- **THEN** Criterion reports timing for decoding the protobuf payload and converting it to Arrow record batches

### Requirement: WAL round-trip benchmark

The benchmark suite SHALL provide a Criterion benchmark for the WAL serialize-then-replay round trip over a representative batch, measuring durability-path encode and read-back cost.

#### Scenario: WAL round-trip benchmark is runnable

- **WHEN** a developer runs the WAL benchmark with the `benchmarks` feature enabled
- **THEN** Criterion reports timing for serializing entries to the WAL and replaying them back into record batches

### Requirement: Compaction throughput benchmark

The benchmark suite SHALL provide a Criterion benchmark for the compactor's Parquet-rewrite step over a set of input files, so compaction throughput can be tracked over time.

#### Scenario: Compaction benchmark is runnable

- **WHEN** a developer runs the compactor benchmark with the `benchmarks` feature enabled against a fixed set of generated input files
- **THEN** Criterion reports timing and throughput for rewriting them into compacted output

### Requirement: Benchmarks are compile-guarded in CI

CI SHALL compile every benchmark target under its `benchmarks` feature on each pull request, and the check SHALL fail when any benchmark target does not compile.

#### Scenario: A broken benchmark fails the PR

- **WHEN** a pull request changes code so that a benchmark target no longer compiles under the `benchmarks` feature
- **THEN** the CI benchmark compile check fails and the pull request is not reported as passing

### Requirement: Nightly benchmark run with trend tracking

CI SHALL run the full Criterion suite on a nightly schedule in release mode, store each run's results as a historical series keyed to the commit, and expose the trend over time.

#### Scenario: Nightly run records a data point

- **WHEN** the nightly benchmark workflow completes on the main branch
- **THEN** the run's benchmark results are appended to the tracked historical series for that commit

### Requirement: Regression alerting threshold

The nightly benchmark tracking SHALL flag a benchmark whose measured time regresses beyond a configured threshold relative to the recorded baseline, and SHALL surface the regression rather than passing silently.

#### Scenario: A slowdown beyond threshold is flagged

- **WHEN** a nightly benchmark result is slower than its baseline by more than the configured threshold
- **THEN** the tracking step marks the run as regressed and reports which benchmark regressed and by how much

#### Scenario: Within-threshold variation does not alert

- **WHEN** a nightly benchmark result differs from its baseline by less than the configured threshold
- **THEN** no regression is flagged for that benchmark

### Requirement: Local baseline and compare workflow

The project SHALL document a repeatable workflow for saving a named Criterion baseline and comparing a later run against it, so a contributor can measure a branch against `main` locally without CI.

#### Scenario: Contributor compares a branch to a saved baseline

- **WHEN** a contributor follows the documented workflow to save a baseline on one revision and compare a second revision against it
- **THEN** Criterion reports the per-benchmark change between the two revisions
