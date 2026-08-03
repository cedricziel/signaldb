# writer-commit-coalescing Specification

## Purpose

Defines the writer's Iceberg commit model: ingested data is committed to storage
asynchronously and in coalesced batches per `(tenant, dataset, table)`, decoupling
commit rate from ingest rate and export latency from catalog latency, while keeping
Iceberg metadata growth bounded and offering an on-demand, tenant-scoped
force-commit for read-your-writes.

## Requirements

### Requirement: Asynchronous commit acknowledgement

The writer SHALL acknowledge an ingest (Flight `do_put`) once the data is durably
persisted to the writer's write-ahead log, and MUST NOT block the acknowledgement
on the Iceberg commit. The Iceberg commit is performed asynchronously by the
writer's background processing loop.

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
- **THEN** the un-committed entries remain in the writer WAL and are committed by
  the background loop after restart (at-least-once delivery is preserved)

### Requirement: Commit coalescing per tenant, dataset, and table

The writer SHALL coalesce pending entries for each `(tenant, dataset, table)` into
a single Iceberg commit, and SHALL commit a group when either a configured commit
interval has elapsed since that group's last commit OR the group's pending row
count reaches a configured ceiling — whichever occurs first. The row ceiling is an
upper bound that triggers an earlier commit for bursts; it SHALL NOT be treated as
a minimum that delays commits of low-volume groups.

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
  interval
- **THEN** the writer produces at most one Iceberg commit for that table per commit
  interval (plus at most one additional commit per ceiling-sized burst)

### Requirement: On-demand, tenant-scoped force-commit

The writer SHALL provide a force-commit operation that immediately commits the
pending groups for a requested tenant (optionally a single dataset within it),
ignoring the coalescing interval and row ceiling for that scope only. Groups
outside the scope SHALL continue to coalesce normally, so a force-commit for one
tenant does not bypass coalescing for others. The scope SHALL be taken from the
request's tenant identity, and a force-commit request without a tenant SHALL be
rejected.

#### Scenario: Force-commit drains the requested tenant's pending data

- **WHEN** a force-commit is requested for a tenant that has pending, un-committed
  entries
- **THEN** the writer commits that tenant's pending groups to Iceberg before the
  force-commit completes, making the data queryable

#### Scenario: Force-commit leaves other tenants coalescing

- **WHEN** a force-commit is requested for one tenant while another tenant also has
  pending entries below the coalescing floor
- **THEN** only the requested tenant's groups are committed; the other tenant's
  groups remain deferred under normal coalescing

#### Scenario: Unscoped force-commit is rejected

- **WHEN** a force-commit is requested without a tenant scope
- **THEN** the writer rejects the request rather than committing every tenant's
  pending groups

#### Scenario: Force-commit with nothing pending is a no-op

- **WHEN** a force-commit is requested for a tenant with no pending entries
- **THEN** the writer performs no Iceberg commit and reports success

### Requirement: Bounded Iceberg metadata growth

The writer's commit model SHALL keep the Iceberg metadata chain for each table
bounded over time, so that continuous ingestion does not accumulate unbounded
table-metadata versions on the catalog and object store.

#### Scenario: Continuous ingestion does not grow metadata without bound

- **WHEN** a table is written continuously over a long period
- **THEN** the number of retained table-metadata versions for that table stays
  within a bounded window rather than growing monotonically with commit count
