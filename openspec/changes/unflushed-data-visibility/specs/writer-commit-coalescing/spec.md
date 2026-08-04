# writer-commit-coalescing Delta Specification

## MODIFIED Requirements

### Requirement: Asynchronous commit acknowledgement

The writer SHALL acknowledge an ingest (Flight `do_put`) once the data is durably
persisted to the writer's write-ahead log, and MUST NOT block the acknowledgement
on the Iceberg commit. The Iceberg commit is performed asynchronously by the
writer's background processing loop, draining the in-memory pending buffer.

#### Scenario: Ack does not wait for the Iceberg commit

- **WHEN** a batch is sent to the writer via `do_put`
- **THEN** the writer returns success after the batch is durably flushed to the
  writer WAL, without waiting for an Iceberg snapshot to be committed

#### Scenario: Acknowledged data is queryable before the commit

- **WHEN** a batch has been acknowledged but not yet committed
- **THEN** the data is queryable through the unflushed-data path (see
  `unflushed-data-visibility`) without an early Iceberg commit, and it
  SHALL also be queryable from storage once committed

#### Scenario: Deferred data survives writer restart

- **WHEN** the writer restarts after acknowledging batches whose Iceberg commit
  had not yet run
- **THEN** the un-committed entries remain in the writer WAL, are replayed into
  the pending buffer, and are committed by the background loop after restart
  (at-least-once delivery is preserved)

### Requirement: On-demand, tenant-scoped force-commit

The writer SHALL provide a force-commit operation that immediately commits the
pending groups for a requested tenant (optionally a single dataset within it),
ignoring the coalescing interval and row ceiling for that scope only. Groups
outside the scope SHALL continue to coalesce normally, so a force-commit for one
tenant does not bypass coalescing for others. The scope SHALL be taken from the
request's tenant identity, and a force-commit request without a tenant SHALL be
rejected. Force-commit is an operational primitive; query execution MUST NOT
depend on it for read-your-writes (see `unflushed-data-visibility`).

#### Scenario: Force-commit drains the requested tenant's pending data

- **WHEN** a force-commit is requested for a tenant that has pending, un-committed
  entries
- **THEN** the writer commits that tenant's pending groups to Iceberg before the
  force-commit completes, making the data queryable from storage

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
