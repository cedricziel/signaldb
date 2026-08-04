# Query Execution Contract Delta Spec

## Purpose

Defines the querier's observable execution semantics: how and when results are delivered, how one deadline governs a query across all hops, what snapshot consistency a result carries, and what resource-fairness guarantees hold between tenants.

## ADDED Requirements

### Requirement: Results are delivered incrementally with bounded memory

The querier SHALL produce query results as an incremental stream of record batches; neither the full decoded result set nor a full encoded copy of it SHALL be resident in querier memory at once, and the router SHALL forward/consume results incrementally rather than buffering complete responses. Result-size guards (row caps) SHALL continue to apply.

#### Scenario: Large result does not spike memory

- **WHEN** a query's result approaches the configured row cap (e.g. hundreds of thousands of rows)
- **THEN** querier peak memory during delivery is bounded by a small number of batches plus encoding buffers, not by the full result size, and the first batch arrives before the last batch is produced

#### Scenario: Mid-stream failure is reported, not silent

- **WHEN** execution fails after some batches were already delivered (e.g. deadline exceeded, memory exhausted)
- **THEN** the stream terminates with an attributable error that the router surfaces to the HTTP caller with a failure reason (never a silently truncated success)

### Requirement: One deadline governs a query across all hops

Each query SHALL have exactly one deadline, taken from the querier's per-query timeout budget; every downstream/transport request deadline involved in serving that query SHALL be derived from it (with a bounded margin), never configured as an independent value. Connection establishment SHALL have its own separate connect-timeout. When the deadline is exceeded, the querier SHALL cancel execution — releasing CPU, memory reservations, and concurrency permits — and the caller SHALL receive a timeout error attributing the deadline.

#### Scenario: Long-running query is not killed early by a transport setting

- **WHEN** a query legitimately runs longer than any transport-level default but within the query timeout budget
- **THEN** it completes successfully; no intermediate hop aborts it before the budget elapses

#### Scenario: Deadline cancels server-side work

- **WHEN** a query exceeds its deadline
- **THEN** its execution stops on the querier (verifiable: CPU/permits released) and the caller receives a timeout error, not a connection reset or bodyless failure

### Requirement: A query executes against pinned table snapshots

For each table a query touches, the querier SHALL resolve the table exactly once at query start and execute the entire query against that snapshot. Results SHALL be single-snapshot-consistent per table: data committed after resolution is not visible, and data visible at resolution does not disappear mid-query. Query correctness MUST NOT depend on snapshot-retention sizing.

#### Scenario: Concurrent commit does not leak into a running query

- **WHEN** ingest commits new data to a table while a query over that table is executing
- **THEN** the query's result reflects only the snapshot resolved at query start

#### Scenario: Multiple references, one snapshot

- **WHEN** a query plan reads the same table more than once (e.g. self-join or multi-stage plan)
- **THEN** every read observes the same snapshot

### Requirement: Tenants get fair, bounded execution resources by default

The querier SHALL run with bounded defaults: a memory budget shared fairly across concurrent queries (a single query cannot monopolize the pool; spill-capable operators spill under pressure) and a per-tenant concurrency limit greater than zero. Exceeding limits SHALL yield attributable errors (resource-exhausted or admission rejection), never process death. Unlimited operation SHALL require explicit configuration.

#### Scenario: Heavy query does not starve other tenants

- **WHEN** one tenant runs a memory-hungry aggregation while another tenant issues a small query concurrently
- **THEN** the small query completes without waiting for the heavy one to finish or fail, and the heavy query spills or fails attributably rather than taking the whole pool

#### Scenario: Defaults are bounded out of the box

- **WHEN** a deployment runs with no explicit querier resource configuration
- **THEN** memory and per-tenant concurrency limits are in force with documented default values, observable via startup logs/metrics
