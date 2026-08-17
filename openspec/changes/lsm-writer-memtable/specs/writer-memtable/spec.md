# writer-memtable Delta Specification

## Purpose

Defines the writer's in-memory cache (memtable) of acknowledged,
not-yet-committed data: when batches enter and leave it, how it reconciles
with the WAL so failure handling keeps self-healing, bounded startup replay,
the soft/hard memory budget, and the observability operators need.

## ADDED Requirements

### Requirement: Admission precedes durability; durability precedes residency

Ingest SHALL follow one ordering: capacity admission (the hard-ceiling
check, accounting for concurrent in-flight puts via reservations) runs
before the WAL append; the WAL append and durable flush run next; the
memtable insert runs after the flush succeeds and before the
acknowledgement is returned. A rejection or failure before durability
leaves no WAL entry and no memtable entry, so upstream redelivery cannot
create duplicate writer-WAL entries. After durability, the ingest SHALL be
acknowledged even if a post-durability step (e.g. schema coercion) fails —
such entries follow the poison-entry path via reconciliation instead of
erroring the acknowledgement. Durability never depends on the memtable,
and a WAL entry is marked processed only after the Iceberg commit covering
it succeeds.

#### Scenario: Rejected ingest leaves no durable entry

- **WHEN** an ingest is rejected by the hard-ceiling admission check
- **THEN** no WAL entry and no memtable entry exist for it, and the
  upstream retry redelivers without creating duplicates

#### Scenario: WAL flush failure leaves no resident batch

- **WHEN** the durable WAL flush for an ingested batch fails
- **THEN** the ingest is rejected as retryable and the memtable contains no
  entry for that batch

#### Scenario: Post-durability failure does not error the acknowledgement

- **WHEN** a batch is durably in the WAL but a subsequent step (such as
  coercion or insertion) fails
- **THEN** the ingest is still acknowledged and the entry is handled by
  the poison/reconciliation path, so the upstream sender does not
  redeliver a durable entry

#### Scenario: Acknowledged batches are resident before the ack returns

- **WHEN** `do_put` acknowledges a batch during normal operation
- **THEN** the batch is already resident in the memtable (or already
  routed to poison handling) at the time the acknowledgement is returned

#### Scenario: Memtable entries always correspond to durable WAL entries

- **WHEN** a batch is resident in the memtable
- **THEN** its covering WAL entries exist durably on disk and are not yet
  marked processed

### Requirement: The memtable is a reconciled cache over the WAL's unprocessed suffix

The steady-state commit path SHALL drain resident groups without reading
WAL payloads back from disk. The writer SHALL nevertheless reconcile the
memtable against each WAL's unprocessed entry set on a regular cadence
using entry metadata only (no payload reads), lazily loading payloads for
any unprocessed entry not resident. Batches SHALL leave the memtable only when
their entries are marked processed or dead-lettered — a failed Iceberg
commit therefore retries from memory, and failure handling (retry counting,
poison-entry dead-lettering) keeps functioning without per-tick payload
re-reads.

#### Scenario: Commit drains from memory, not the WAL

- **WHEN** the background loop commits a pending group during normal
  operation (no restart since the data was acknowledged)
- **THEN** the committed data comes from the memtable and no WAL payload is
  read back from disk to serve the commit

#### Scenario: Failed commit retries without restart

- **WHEN** a group's Iceberg commit fails after the group was drained
- **THEN** the group's batches remain (or are restored) in the memtable and
  are retried on a subsequent cycle without requiring a writer restart

#### Scenario: Deferred groups are decoded once

- **WHEN** a group is deferred by the coalescing floor for several cycles
  before committing
- **THEN** its batches are decoded once at ingest and remain resident until
  the commit, rather than being re-read and re-decoded each cycle

#### Scenario: Dead-lettering evicts the resident copy

- **WHEN** an entry exhausts its failure budget and is dead-lettered
- **THEN** its batches are removed from the memtable and its bytes are
  released from the memory accounting

#### Scenario: Two WALs feeding one table never share a commit

- **WHEN** two WALs (a tenant's own and an adopted legacy root WAL) both
  hold unprocessed entries that route to the same
  `(tenant, dataset, table)`
- **THEN** each WAL's entries are committed and marked separately, so the
  per-WAL idempotency marker a commit writes covers exactly the entries of
  the WAL it names and never those of the other

### Requirement: Crash recovery rebuilds the memtable within the memory budget

On startup, the writer SHALL replay unprocessed WAL entries into the
memtable, preserving at-least-once delivery. Replay SHALL be incremental:
when resident bytes reach the budget, the writer commits drained groups
and continues, so replay memory is bounded by the configured budget
regardless of backlog size. Replay runs concurrently with live ingest:
`do_put` remains available, and replay loads and live inserts share the
same budget accounting and admission check. A replay chunk whose commit or
mark-processed fails is retained and retried under the normal failure
budget without halting replay progress. Undeserializable or unroutable
entries follow the existing poison-entry handling. Routing during replay
SHALL agree with routing at ingest: the same batch lands in the same
`(tenant, dataset, table)` before and after a restart.

#### Scenario: Restart with un-committed data loses nothing

- **WHEN** the writer restarts after acknowledging batches whose Iceberg
  commit had not yet run
- **THEN** replay rebuilds those batches into the memtable and the
  background loop commits them, and the data becomes queryable from storage

#### Scenario: Replay of a backlog larger than the budget does not exhaust memory

- **WHEN** the unprocessed WAL backlog exceeds the configured memory budget
  at startup
- **THEN** the writer alternates loading and committing so peak memtable
  memory stays bounded by the budget and the full backlog is eventually
  committed

#### Scenario: Ingest during replay shares the budget

- **WHEN** live ingest arrives while startup replay is still loading a
  backlog
- **THEN** the ingest is admitted against the same accounting as replay
  loads, combined residency stays within the configured bounds, and both
  the replayed and the live batches are eventually committed

#### Scenario: Replay chunk failure does not halt replay

- **WHEN** a commit or mark-processed operation for a replay chunk fails
- **THEN** the chunk is retained for retry under the normal failure
  handling and replay continues with subsequent entries

### Requirement: Soft budget signals, hard ceiling rejects

The writer SHALL enforce a configurable soft memory budget and a
configurable hard ceiling on total memtable memory, both defined over
accounted Arrow batch bytes (including in-flight admission reservations);
sizing guidance directs operators to leave headroom for unaccounted
bookkeeping and allocator overhead. Crossing the soft
budget SHALL cause the commit loop to flush the largest group first, ahead
of its coalescing schedule; pressure flushing MUST NOT run inline on the
ingest path, so ingest acknowledgement latency never couples to catalog
latency. At the hard ceiling the writer SHALL reject further ingest with a
retryable error rather than growing without bound — under sustained commit
failure, memtable memory stays bounded while upstream WAL retry preserves
the data. Accounting SHALL be tracked per group — that is, per
`(WAL identity, tenant, dataset, table)` — so the per-group byte ceiling
governs exactly the unit that commits; reported attribution MAY aggregate
groups up to `(tenant, dataset, table)`.

#### Scenario: Soft-budget breach flushes the largest group

- **WHEN** resident bytes exceed the soft budget
- **THEN** the commit loop commits the largest pending group ahead of its
  schedule while ingest continues to be acknowledged normally

#### Scenario: One noisy tenant does not evict everyone

- **WHEN** one tenant's group dominates memtable memory while other tenants
  hold small pending groups
- **THEN** pressure flushes target the dominating group; the small groups
  keep coalescing on their normal interval

#### Scenario: Sustained commit failure does not grow memory without bound

- **WHEN** Iceberg commits fail repeatedly (e.g. catalog outage) while
  ingest continues
- **THEN** once the hard ceiling is reached the writer rejects ingest with
  a retryable error, resident memory stays at or below the ceiling, and
  ingest resumes when commits recover

#### Scenario: Pressure never blocks the ingest ack

- **WHEN** an ingest arrives while the writer is over the soft budget
- **THEN** the acknowledgement does not wait for any Iceberg commit or
  catalog operation triggered by the pressure condition

### Requirement: Memtable observability

The writer SHALL expose self-monitoring metrics for memtable operation: at
minimum resident bytes (total, with bounded-cardinality group attribution),
pressure-flush occurrences, hard-ceiling rejections, startup replay volume,
and WAL payload reads labeled by reason (recovery, reconcile, dead-letter)
— making the "no payload reads on the steady-state path" behavior
observable and assertable.

#### Scenario: Operator can see memory pressure

- **WHEN** pressure flushes or hard-ceiling rejections occur because the
  budget is undersized for the ingest volume
- **THEN** the writer's metrics surface the pressure-flush and rejection
  rates and resident bytes so the operator can size the budget

#### Scenario: Steady-state payload reads are visible as zero

- **WHEN** the writer operates normally with no restarts, reconciliation
  gaps, or dead-lettering
- **THEN** the payload-read metric records no reads, and any nonzero
  reading is attributable to its reason label
