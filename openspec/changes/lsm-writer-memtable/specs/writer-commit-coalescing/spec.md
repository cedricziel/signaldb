# writer-commit-coalescing Delta Specification

## ADDED Requirements

### Requirement: Memory-pressure commit trigger

In addition to the commit interval and the row ceiling, the writer SHALL
commit a group ahead of its schedule when memtable memory exceeds the
configured soft budget, selecting the largest group first, and SHALL
enforce a per-group byte ceiling alongside the existing per-group row
ceiling. Pressure commits SHALL NOT alter the coalescing schedule of groups
that were not flushed, SHALL be executed by the background commit loop
(never inline on the ingest path), and SHALL be observable via
self-monitoring metrics so sustained pressure churn is visible to
operators.

#### Scenario: Budget breach commits ahead of schedule

- **WHEN** pending memory exceeds the soft budget before any group's
  interval or ceiling is reached
- **THEN** the commit loop commits the largest group immediately and the
  remaining groups keep their normal coalescing schedule

#### Scenario: Group hitting its byte ceiling commits early

- **WHEN** a single group's resident bytes reach the per-group byte ceiling
  before its interval elapses or its row ceiling is reached
- **THEN** the writer commits that group immediately, keeping the global
  budget a safety net rather than the steady-state trigger

## MODIFIED Requirements

### Requirement: Commit coalescing per tenant, dataset, and table

The writer SHALL coalesce pending entries for each `(tenant, dataset, table)` into
a single Iceberg commit, and SHALL commit a group when either a configured commit
interval has elapsed since that group's last commit OR the group's pending row
count reaches a configured ceiling OR the group's pending bytes reach a configured
byte ceiling OR a memory-pressure flush selects the group — whichever occurs
first. The row and byte ceilings are upper bounds that trigger an earlier commit
for bursts; they SHALL NOT be treated as minimums that delay commits of
low-volume groups.

#### Scenario: Low-volume group commits on the interval

- **WHEN** a group accumulates only a few rows and the commit interval elapses
- **THEN** the writer commits those rows in a single Iceberg commit without
  waiting for the row ceiling

#### Scenario: Burst commits early on the row ceiling

- **WHEN** a group accumulates rows up to the configured ceiling before the commit
  interval elapses
- **THEN** the writer commits that group immediately rather than waiting for the
  remainder of the interval

#### Scenario: High-frequency producer does not amplify commits

- **WHEN** a producer sends many small batches to the same table within one commit
  interval, without reaching a row or byte ceiling or triggering memory
  pressure
- **THEN** the writer produces at most one Iceberg commit for that table per commit
  interval (plus at most one additional commit per ceiling-sized burst or
  pressure flush)

### Requirement: Asynchronous commit acknowledgement

The writer SHALL acknowledge an ingest (Flight `do_put`) once the data is durably
persisted to the writer's write-ahead log, and MUST NOT block the acknowledgement
on the Iceberg commit. The Iceberg commit is performed asynchronously by the
writer's background processing loop, draining the in-memory pending buffer.

#### Scenario: Ack does not wait for the Iceberg commit

- **WHEN** a batch is sent to the writer via `do_put`
- **THEN** the writer returns success after the batch is durably flushed to the
  writer WAL, without waiting for an Iceberg snapshot to be committed

#### Scenario: Data becomes queryable after asynchronous commit

- **WHEN** a batch has been acknowledged but not yet committed
- **THEN** the data is not required to be queryable until the background loop
  commits it, and it SHALL become queryable once committed

#### Scenario: Deferred data survives writer restart

- **WHEN** the writer restarts after acknowledging batches whose Iceberg commit
  had not yet run
- **THEN** the un-committed entries remain in the writer WAL, are replayed into
  the pending buffer, and are committed by the background loop after restart
  (at-least-once delivery is preserved)
