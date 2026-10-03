# Design: Trace-ID Point Index

## Context

See proposal.md, Why. Code facts as of `main` on 2026-10-03:

- `find_by_id_with_tenant` (`src/querier/src/query/trace.rs:97`) loads
  `traces`, filters `trace_id = ?`, and applies the optional Tempo
  `start`/`end` hints as both a row filter and a widened `timestamp`
  predicate, because `traces` is partitioned `Hour(timestamp)`
  (`schemas.toml`, `partition_by = ["timestamp"]`). Without hints, every
  file's footer is opened. Bloom filters prune row groups, and the footer
  cache (#1310) only helps on a repeat lookup.
- The writer already maintains a companion table next to a signal table:
  `metric_exemplars` is committed beside `metrics` in the same commit cycle
  (`src/writer/src/processor.rs`, `commit_metric_exemplars_for_chunk`).
  `trace_index` follows that pattern.
- The compactor works on closed partitions and commits deltas that tolerate
  concurrent ingest (spec `compaction`). Retention lives in
  `src/compactor/src/retention`.
- The writer's reconciler (`src/writer/src/reconcile.rs`) ensures a table per
  enabled signal per dataset.
- Iceberg has no multi-table transactions, so a `traces` commit and a
  `trace_index` commit are always two commits.
- The #879 benches (`tests-integration/benches/trace_index_scaling.rs`) show a
  prefix-sharded, sorted, bloomed Parquet index answering in 333 µs at 10k
  traces and 367 µs at 1M.

FDAP alignment: the index is an ordinary Iceberg table read through
DataFusion, with no side store and no custom file format. Everything the
querier does with it is a DataFusion plan over Iceberg scans, so footer
caching, bloom pruning and snapshot pinning apply to it unchanged.

## Goals / Non-Goals

**Goals:**

- A bare-id lookup is complete across retention and bounded by the hours the
  trace touches.
- No false negatives from the index, ever. A wrong index may cost extra
  scans, never missing spans.
- Graceful degradation: no table, a stale watermark or a read error gives
  today's behaviour.

**Non-Goals:**

- Sorting or clustering `traces` by `trace_id` (#936, `declared-data-ordering`).
- Attribute search indexes (the warm index already covers that path).
- Indexing logs or profiles by trace id. The same table shape could serve
  them later, but that is out of scope here.
- Changing lookups that carry a time range.

## Decisions

### D1: Table shape: one row per (trace_id, hour)

```
trace_index
  trace_id   string        required   -- hex, as in traces.trace_id
  hour       timestamp_ns  required   -- span start truncated to the hour (UTC)
partition:   truncate(1, trace_id)    -- 16 shards by first hex digit
sort order:  trace_id, hour
bloom:       trace_id
```

The grain is a posting list of the partitions a trace actually touches,
which matches the `Hour(timestamp)` partitioning of `traces`. The `hour`
value is the same hour the span's row lands in, so `hour IN (...)` maps
directly to `timestamp` partition pruning.

`truncate(1, trace_id)` is a native Iceberg transform, so no computed column
is needed. Shard count can grow later through partition-spec evolution, for
example to `truncate(2)` for 256 shards, without rewriting old files. The
default is 16 rather than the 256 the bench used, because the writer writes
one file per shard per commit (D3). At 16 shards a low-volume dataset ends up
with 16 files after compaction rather than 256.

Rejected: span counts or min/max timestamps per row. They make dedup a merge
instead of a distinct, and the lookup never needs them.

Rejected: partitioning by `day(hour)` as well. Retention could then drop
partitions directly, but a lookup would reopen one file per shard per day,
which brings back the problem this index exists to fix.

### D2: Index first, so the index is always a superset

For each commit cycle that carries spans, the writer:

1. derives the distinct `(trace_id, hour)` pairs from the batches it is about
   to commit;
2. appends them to `trace_index` and commits;
3. commits the span data to `traces`.

If step 3 fails, the index lists an hour with no spans for that trace. The
lookup scans that hour and finds nothing extra, so the only cost is one
wasted scan. WAL replay retries step 3 and re-appends the same index rows,
and duplicates are removed at compaction.

The reverse order (data first) would make a step-2 failure a false negative,
which would silently drop spans.

If step 2 fails, the cycle fails before any data is committed and WAL replay
retries it, the same as any other commit failure. An index outage therefore
stalls trace ingest instead of degrading it. `[trace_index].enabled = false`
is the escape hatch, and turning it off also stops lookups from trusting the
index (D5).

### D3: Bounding the write cost

The index commit doubles Iceberg commits for traces and adds up to 16 small
files per cycle. Mitigations:

- The commit coalescer (spec `writer-commit-coalescing`) already batches
  traces commits per dataset, and the index commit rides the same cadence.
- Index rows are tiny: about 40 bytes before encoding, and most batches touch
  one hour.
- The compactor treats `trace_index` shards as small-file candidates like any
  other table (D4).

To measure in task 1.x before turning this on by default: writer commit
latency and throughput with and without the index on the ingest bench. The
acceptance bar is no more than 10% regression on sustained trace ingest.

### D4: Compactor maintenance: dedup, compact, retention

When a shard reaches the small-file threshold, the compactor rewrites it as
`SELECT DISTINCT trace_id, hour ... WHERE hour >= retention_cutoff ORDER BY
trace_id, hour`. That is one rewrite doing dedup, compaction and retention.
It commits as a delta that replaces only the files it read (spec
`compaction`), so concurrent writer appends stay.

Between compactions, rows for hours already dropped from `traces` are
harmless false positives, because the scan finds no files for those hours.

The index has no closed partitions of its own, since every shard receives
appends continuously. The rewrite therefore takes only files older than a
settle age (default 10 minutes), so a rewrite does not race the files the
writer is still producing.

### D5: Querier: hot window, indexed range, unindexed range

For a bare-id lookup at time `now`:

```
complete_from = trace_index property signaldb.trace_index.complete_from
hot_from      = now - [trace_index].hot_window          (default 2h)

indexed hours = SELECT DISTINCT hour FROM trace_index
                WHERE trace_id = ? AND hour >= complete_from AND hour < hot_from

traces scan   = trace_id = ?
                AND ( timestamp IN indexed hours        -- widened to hour bounds
                   OR timestamp >= hot_from
                   OR timestamp <  complete_from )      -- omitted when complete_from <= oldest data
```

- **Hot window.** The newest hours are where the index has the most small,
  uncompacted files, and where the `traces` files are fewest and most likely
  already cached. Scanning them directly avoids merge-on-read over many small
  index files. This is the hot/cold split #880 suggests.
- **Unindexed range.** For hours older than `complete_from` (data written
  before the index existed and not yet backfilled), the lookup falls back to
  today's bloom scan.
- **Fallback.** A missing table, a disabled index, or any error reading the
  index makes the querier drop the index branch and run today's unbounded
  scan. It logs at `warn` with the tenant/dataset and increments a counter.
  This never surfaces as a user error.
- **Pushdown.** The `IN` list is turned into `timestamp` range predicates per
  hour, so Iceberg prunes partitions. The `trace_id` filter keeps the row
  filter exact.

Explicit `start`/`end` hints keep today's path (spec: lookups with a time
range are unchanged).

### D6: Coverage watermark and backfill

`signaldb.trace_index.complete_from` is a table property set to the creation
hour when the reconciler creates `trace_index`. Every span committed after
that point went through D2, so the index is complete from that hour on.

Backfill is a compactor job. It walks closed `traces` hour partitions older
than `complete_from`, newest first. For each partition it appends the
partition's distinct `(trace_id, hour)` pairs, then lowers `complete_from` to
that hour in the same `trace_index` commit, using the property update and the
append as one Iceberg transaction on one table. A crash between hours leaves
the watermark at the last fully indexed hour.

Backfill stops at the retention cutoff. It is opt-in per run (`compact_run`
style trigger) in the first release, and becomes automatic once the write
cost (D3) is measured.

Late spans need no special case. A span arriving days late is still written
through D2, its `(trace_id, hour)` row lands in its event hour, and the
posting list grows.

### Migration and rollback

- **Migration**: purely additive. A new table per dataset, created by the
  reconciler, and no change to `traces`, the WAL or the Flight schemas.
  Existing data is served by the unindexed-range fallback until backfilled.
- **Rollback**: older binaries ignore `trace_index`. To stop paying the
  write cost, set `[trace_index].enabled = false`. To reclaim space, drop the
  table. Nothing else references it.

## Risks / Trade-offs

- **Ingest stalls on an index outage** (D2). This is accepted in exchange for
  never having a false negative. The kill switch turns it off.
- **Write amplification** (D3). This has to be measured before the index is
  enabled by default, and the default stays `false` until then.
- **Hot-window misconfiguration.** A hot window shorter than the compactor's
  lag makes lookups open more small index files. This affects speed only,
  never correctness.
- **Small datasets.** For a dataset with a handful of hour files, the index
  adds a scan without saving much. A later refinement could skip the index
  when `traces` has fewer files than shards. It is not part of this change.

## Open Questions

1. **Query IR surface.** IR documents require `range`, so first-party
   surfaces (Explore UI, MCP `get_trace`, CLI) cannot ask for a bare-id
   lookup today. Proposed: allow `range` to be omitted when the pipeline's
   only filter is `trace_id = <literal>` and the result is `trace`, and route
   that to the indexed lookup. The alternative is to keep IR range-bound and
   only accelerate the Tempo path. This needs Cedric's call, because it
   changes the IR contract.
2. **Default on or off.** Proposed: off until the D3 bench passes, then on.
3. **Shard count.** Proposed: 16 by default, with partition-spec evolution
   to 256 as the documented scaling step. Should this be a config knob or
   operator-run spec evolution?
