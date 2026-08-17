---
audience: operator
type: how-to
status: living
sources:
  - src/common/src/iceberg/sort.rs
  - src/querier/src/flight.rs
---

# Ordered Queries and Sort-Order Attestation

Every signal table declares a time-leading sort order, and every file producer
sorts its rows by it and records that fact on the files it writes. The querier
uses those recordings to skip sorting data that is already in order — which is
where "order by time, take the most recent _n_", the most common query shape in
observability, gets faster.

This page is for operators: what changes when you upgrade, how to tell whether
it is working, and how to turn it off. For the design, see
[Storage Layout](../architecture/storage-layout.md#declared-sort-order).

## What changes when you upgrade

**Nothing you have to do.** But something does change in how queries execute,
and it is worth understanding before you see it.

The engine option this depends on, `[querier.datafusion].split_file_groups_by_statistics`,
has been enabled by default for some time. It was **inert**: no file recorded
whether its rows were sorted, so there was nothing for the option to act on and
every ordered query sorted its input.

Once a version that records the ordering is running, newly written files carry
that record, and ordered queries over them **stop performing sorts they used to
perform**. The option did not change. What changed is that it finally has
something to act on.

Files written by older versions carry no record. They are read as unsorted,
which is correct rather than merely safe: a single unrecorded file in the range
a query touches withdraws the ordering claim for that whole range, so the query
sorts exactly as it did before. Those files converge as compaction rewrites
their partitions. There is no backfill job and nothing to run by hand.

## Why this is worth watching

The saving comes from the engine trusting what a file says about itself. A file
that claims to be sorted when it is not does not make a query slow — it makes
it **wrong**, silently: the engine drops a sort it believed was redundant and
returns rows in the wrong order, with no error anywhere.

The system is built so that cannot happen — a producer that cannot guarantee
the order writes its files without the claim, and debug builds re-check the
sort before claiming it — and there is a permanent regression test that
compares ordered results against an independently computed answer. But it is
the reason to treat a suspicious ordering result as a serious bug report rather
than a performance question.

## How to tell it is working

Ask for an execution plan on a time-ordered query over a recent range:

```sql
EXPLAIN SELECT timestamp, trace_id FROM traces
WHERE timestamp > now() - INTERVAL '1 hour'
ORDER BY timestamp DESC LIMIT 20;
```

- The scan reports `output_ordering=[...]` → the files in range carry the
  record.
- No `SortExec` above the scan → the sort was skipped. This is the win.
- A `SortExec` is present → some file in range carries no record (expected for
  data written before the upgrade), or the files' time ranges overlap so
  reading them in sequence would not be ordered. Both are correct behavior, not
  a fault.

Compacting the partitions in question converges the first case.

## Turning it off

```toml
[querier.datafusion]
split_file_groups_by_statistics = false
```

This is the whole rollback. The sort orders in table metadata and the records
in Parquet footers are inert without the querier acting on them — they cost
nothing to leave in place, and queries return to sorting their input as before.
No data is rewritten and nothing needs to be undone.

Note that this option also affects file grouping generally, not only ordering,
so leave it on unless you are specifically backing out of ordered-scan
behavior.

## Related

- [Storage Layout — declared sort order](../architecture/storage-layout.md#declared-sort-order)
  — the sort keys per signal and how attestation travels with a file.
- [Compactor operations](compactor/operations.md) — how compaction converges
  files written before the upgrade.
- [Compactor troubleshooting](compactor/troubleshooting.md#sort-order-and-ordering-attestation)
  — the log lines a partition emits when it has no declared order to honor.
