# Unflushed Data Visibility (LSM Stage 2)

## Why

Today acknowledged data becomes queryable only after its coalesced Iceberg
commit: worst case `commit_interval` (default 5s) plus the background
loop's tick (~5s) — a 5–10s read-your-writes lag. That lag is what pins
`commit_interval` low; raising it to 30–60s would deliver the file-size and
snapshot-count payoff of the `lsm-writer-memtable` change but make the lag
user-visible and unacceptable. This change decouples visibility from commit
cadence: queriers union the writer's resident (memtable) data with the
committed Iceberg scan, so data is queryable at ack and the commit interval
becomes a pure storage-shape tuning knob. It also fixes the first-run
experience — a new tenant's data is currently unqueryable until the first
commit creates the Iceberg table.

Note: there is no query-driven force-commit in production today
(`do_action("flush")` is called only by the test-gated
`common::testing::flush_storage_writers`); this change migrates those
integration tests to real visibility semantics and keeps the action as an
operational primitive.

## What Changes

- **Per-group commit watermark.** The writer assigns each memtable insert a
  per-`(writer, tenant, dataset, table)` monotonic sequence; every Iceberg
  commit records the committed high-water sequence in table properties in
  the same atomic transaction as the data (alongside the existing
  idempotency marker, which is unchanged).
- **Writer hot-scan surface.** A Flight `do_get` streams a group's resident
  batches — coerced to the table's Arrow schema, tagged with writer id and
  sequence — with mandatory time bounds, subject to the same
  internal-service authentication as `do_put`, tenant-matched to the
  caller's scope.
- **Querier hot/cold union at one chokepoint.** The querier's Iceberg
  schema provider returns a hybrid table provider: scan hot first, then
  resolve the cold snapshot and its watermarks, drop hot batches at or
  below the watermark. This ordering provably yields no duplication and no
  omission across the flush boundary. When the Iceberg table does not yet
  exist but hot data does, a hot-only provider serves it (first-run
  read-your-writes).
- **Degrade, observably.** Writer unreachable or boundary unresolvable →
  serve committed data, drop hot rows, surface degradation via metrics and
  span attributes (plus the standard `warnings` field on the PromQL path);
  no invented markers on Tempo/Loki response formats.
- **Test migration.** The five integration-test suites using the
  force-commit barrier move to memtable visibility; `do_action("flush")`
  stays for operational use.

## Capabilities

### New Capabilities

- `unflushed-data-visibility`: the watermark protocol, the writer's
  authenticated hot-scan surface, the querier's hot-first union with
  no-duplication and no-omission guarantees, first-run hot-only serving,
  and observable degradation.

### Modified Capabilities

- `writer-commit-coalescing`: the acknowledgement requirement's visibility
  scenario changes — acknowledged data is queryable before its commit via
  the unflushed-data path; force-commit is explicitly an operational
  primitive that query execution does not depend on.

## Impact

- **writer**: sequence assignment in the memtable, watermark property in
  the commit transaction (`storage/iceberg.rs`), `do_get` hot-scan handler
  with auth + time bounds (`flight_iceberg.rs`).
- **querier**: hybrid table provider returned from the Iceberg schema
  chokepoint (`flight.rs` `LiveIcebergSchema::table`), shared schema
  coercion from `common`, cached Storage-writer discovery, hot-only
  provider for missing tables, degradation metrics/span attributes.
- **common**: hot-scan Flight ticket types, discovery-cache helper,
  self-monitoring metrics for hot scans and degradation.
- **compactor**: regression test that compaction / snapshot-expiration
  commits preserve the watermark table properties.
- **router**: PromQL `warnings` plumbing for degraded results.
- **tests-integration**: watermark round-trip, flush-race
  (no-dup/no-omission), first-run visibility, degrade-not-fail, and the
  five migrated flush-barrier suites.
- Depends on the `lsm-writer-memtable` change (stage 1) being implemented.
- Not breaking: additive Flight surface; WAL format, Iceberg layout, and
  ingest surfaces unchanged.
