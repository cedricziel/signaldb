# unflushed-data-visibility Delta Specification

## Purpose

Defines how acknowledged-but-not-yet-committed data becomes queryable: the
per-group commit watermark that makes the hot/cold boundary resolvable, the
writer's authenticated hot-scan surface, the querier's hot-first union with
its no-duplication and no-omission guarantees, and observable degradation
when hot data cannot be served.

## ADDED Requirements

### Requirement: Commit watermark recorded atomically with the data

The writer SHALL assign each resident batch a monotonically increasing
sequence per `(writer, tenant, dataset, table)` group, and every Iceberg
commit SHALL record the group's committed high-water sequence in the
table's metadata within the same atomic commit transaction as the data
files. Watermarks are namespaced per writer so multiple writers never
contend on one key, and lifecycle operations on the table (compaction,
snapshot expiration) MUST preserve them.

#### Scenario: Watermark and data commit together

- **WHEN** a group's pending batches are committed to Iceberg
- **THEN** the same commit records the group's committed high-water
  sequence, and no snapshot exists in which the data is present but the
  covering watermark is not

#### Scenario: Chunked commits advance the watermark contiguously

- **WHEN** a group drains in multiple chunks and a later chunk fails
- **THEN** the recorded watermark covers only the contiguously committed
  prefix, so uncommitted batches remain above the watermark

#### Scenario: Compaction preserves watermarks

- **WHEN** the compactor rewrites files or expires snapshots for a table
- **THEN** the per-writer watermark properties survive unchanged

### Requirement: Writer exposes authenticated, bounded hot scans

The writer SHALL serve a Flight scan over a group's resident batches,
returned in the table's exact Arrow storage schema and tagged with the
writer identity and each batch's sequence. The scan SHALL require the same
internal-service authentication as ingest, and the requested tenant SHALL
be validated against the authenticated caller's scope — a scan without a
tenant scope, or for a tenant outside the caller's scope, is rejected.
Requests SHALL carry time bounds, and the writer SHALL skip batches that do
not overlap them; responses SHALL be bounded in size.

#### Scenario: Scan returns only the requested tenant's pending data

- **WHEN** two tenants have resident data and an authorized scan requests
  one tenant's table
- **THEN** the response contains only that tenant's batches for that table

#### Scenario: Unauthenticated or unscoped scans are rejected

- **WHEN** a scan arrives without valid internal-service authentication,
  without a tenant scope, or for a tenant outside the caller's scope
- **THEN** the writer rejects the request and serves no data

#### Scenario: Time bounds prune the scan

- **WHEN** a scan requests a time range that overlaps only some resident
  batches
- **THEN** non-overlapping batches are not transferred

#### Scenario: Hot batches match the cold schema

- **WHEN** a hot batch is returned for a table that also has committed data
- **THEN** its Arrow schema is identical to the committed side's resolved
  schema — including field types, timestamp units, nullability, and derived
  columns — so filters plan and evaluate identically on both sides

### Requirement: Queries see acknowledged data without commits

The querier SHALL union resident data from Storage-capable writers with the
committed Iceberg scan, so acknowledged data is queryable without waiting
for — or triggering — an Iceberg commit. Query execution MUST NOT invoke
the writer's force-commit operation. All query surfaces SHALL acquire the
union through the querier's single table-resolution point, so no surface
silently lacks hot data. When the Iceberg table for a group does not yet
exist but the group has resident data, the querier SHALL serve the hot data
alone using the table's canonical schema.

#### Scenario: Acknowledged data is queryable before its commit

- **WHEN** a batch has been acknowledged but its coalesced commit has not
  yet run
- **THEN** a query over the covering time range returns the batch's rows,
  merged with committed data

#### Scenario: First data for a new table is queryable before the table exists

- **WHEN** a new `(tenant, dataset, table)` has acknowledged data and no
  Iceberg table has been created yet
- **THEN** a query returns the resident data rather than a table-not-found
  error

#### Scenario: Queries do not trigger commits

- **WHEN** clients query continuously while groups are coalescing
- **THEN** no force-commit is invoked by query execution and commit cadence
  follows the writer's coalescing and memory policies alone

### Requirement: No duplication and no omission across the flush boundary

For rows ingested through a single writer entry, the hot/cold union SHALL
NOT introduce duplication — a row present both in a hot scan and in the
resolved committed snapshot is returned once — and SHALL NOT omit a row
acknowledged before query execution began because it moved from the
memtable to Iceberg during the query. To guarantee both, the querier SHALL
obtain hot results first and only then resolve the committed snapshot and
its watermarks, discarding hot batches whose sequence is at or below the
resolved watermark for their writer; batches SHALL remain scannable on the
writer until their WAL entries are marked processed. (Duplicates arising
from upstream at-least-once redelivery are out of scope — they exist
independently of this capability.)

#### Scenario: Query concurrent with a flush counts rows once

- **WHEN** a query executes while one of its target groups is being
  committed from the memtable to Iceberg
- **THEN** each ingested row appears exactly once in the result, whether it
  was served from the hot scan or from the newly committed snapshot

#### Scenario: Commit between hot scan and cold resolution loses nothing

- **WHEN** a group's commit completes after the hot scan returned its
  batches but before the committed snapshot is resolved
- **THEN** the affected hot batches are discarded by the watermark filter
  and the same rows are returned from the committed snapshot

### Requirement: Unresolvable hot data degrades observably

When hot data cannot be served or its boundary cannot be resolved — a
writer is unreachable, still replaying after restart, or returns batches
whose watermark cannot be determined — the querier SHALL serve committed
data, SHALL drop the affected hot data rather than risk duplication, and
SHALL surface the degradation through self-monitoring metrics and span
attributes. On query surfaces with a standard warning mechanism the
degradation SHALL additionally be reported in the response; surfaces
without one are not extended with non-standard markers.

#### Scenario: Writer outage still serves committed data

- **WHEN** every Storage-capable writer is unreachable at query time
- **THEN** the query succeeds over committed Iceberg data and the
  degradation is recorded in the querier's metrics and the query's span

#### Scenario: Boundary unresolvable means hot rows are dropped, not duplicated

- **WHEN** a hot scan returns batches but the committed watermark for that
  writer cannot be resolved
- **THEN** those hot batches are excluded from the result rather than
  merged unfiltered
