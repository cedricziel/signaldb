# Trace-ID Point Index

## Why

Looking up a trace by a bare id (a trace id pasted from a log line or an
error report, with no time range) is the slowest and the least reliable read
path for traces (#880). `find_by_id_with_tenant` filters `traces` on
`trace_id` and lets bloom filters (#826) and the Parquet footer cache (#1310)
prune. Pruning is already near perfect, but on a cold cache the lookup still
opens a footer for every file in the dataset. On a seeded dataset that was
~73 files and ~53 ms of `time_elapsed_opening` against 0.9 ms of scanning.
On object storage each open is a network round trip, and the cost grows with
retention.

A time window is not a substitute. Callers without one cannot use it, and a
fixed window is incomplete for traces that span hours. In the #879 bench, a
trace with spans in 3 hours across 2 days returned 1 of 3 spans for a ±1h
window.

The cheaper levers from #880 are covered elsewhere. Footer caching shipped in
#1310, and sort-by-`trace_id` belongs to #936 / `declared-data-ordering`. This
change designs the remaining lever: a companion point index that maps a trace
id to the hours it touches. A bare-id lookup then opens only those hours'
files, cold cache included, and stays complete for traces spread across days.

## What Changes

- **New per-dataset Iceberg table `trace_index`**, next to `traces` in the
  tenant namespace. It holds one row per `(trace_id, hour)` that the dataset
  holds spans for. It is partitioned by `truncate(1, trace_id)` (16 shards by
  the first hex digit), sorted by `trace_id`, with a bloom filter on
  `trace_id`. This is a **new table, not a change to `traces`**: the on-disk
  layout of existing tables is unchanged, and nothing breaks for older
  binaries, which ignore the table.
- **The writer appends index rows before it commits the span data they
  describe** (index first). A failure between the two commits leaves only a
  harmless false positive, so the index is always a superset of what
  `traces` holds and a lookup never misses spans because of the index.
- **The compactor dedups and compacts each index shard** into a few large
  sorted files. In the same rewrite it drops rows older than the dataset's
  trace retention.
- **A coverage watermark** (`signaldb.trace_index.complete_from`, an index
  table property) marks the earliest hour the index is known complete for.
  On activation it is set to the first whole hour after every writer is
  known to be indexing. A seed step covers spans committed earlier that
  still fall in later hours, and a compactor backfill job indexes older hours
  from existing data and lowers the watermark. Activation is a table
  property, so all writers agree on it and an off/on toggle cannot leave an
  unindexed hour that lookups trust.
- **The querier's bare-id lookup consults the index.** For hours the index
  covers, it scans only the hours the index lists for that id. For the recent
  hot window, and for any hours older than the watermark, it scans `traces`
  directly as it does today. A missing index table, or an index read error,
  falls back to today's scan. Lookups that already carry a time range are
  unchanged.
- **Config**: a `[trace_index]` section.
  - `enabled` decides whether the reconciler activates the index for
    datasets. It defaults to `false` until the write-cost bench in design.md
    D3 passes.
  - `hot_window` defaults to `2h`.
  - `refresh_interval` is how often writers re-read the activation state. It
    defaults to `1m`.
- **Table lifecycle**: the writer's signal-table reconciler provisions
  `trace_index` together with `traces`, and tenant/dataset deletion removes it.

Affected crates: `common` (index schema, config), `writer` (index append,
reconciler), `compactor` (shard compaction, retention, backfill), `querier`
(lookup planner), `router` / OpenAPI only if the Query IR gains a bare-id
lookup (see design.md, Open Questions).

## Capabilities

### New Capabilities

- `trace-id-lookup`: a trace looked up by id alone returns every span the
  dataset holds for it, without the caller supplying a time range, and
  at a cost bounded by the hours the trace touches rather than by retention.

### Modified Capabilities

<!-- None. The index is an internal acceleration structure; `otlp-traces-ingestion`,
`compaction` and `dataset-table-provisioning` behaviour visible to users is
unchanged apart from what `trace-id-lookup` states. -->

## Impact

- **Storage**: one small table per dataset. Each row is a 32-character id
  plus an hour. Before dedup there are roughly (distinct traces × hours
  touched) rows, and almost every trace touches a single hour.
- **Write path**: one extra Iceberg commit per writer commit cycle for the
  `trace_index` table, up to 16 small files per commit before compaction.
  This is the main cost; design.md D3 bounds it.
- **Query path**: bare-id lookups open the index shard plus only the listed
  hours' files.
- **Rollback**: drop or ignore the table. Disabling the index stops reads and
  writes, and lookups fall back to today's behaviour.
- **Related**: #880 (this), #936 (sort by `trace_id`, separate), #1310 (footer
  cache, shipped), #879 (benches the measurements come from).
