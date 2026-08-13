## ADDED Requirements

### Requirement: Database client spans for query execution

DataFusion query execution SHALL produce CLIENT spans following the stable
database semantic conventions, across every query surface the querier
serves — raw SQL, PromQL, LogQL, TraceQL, and the native query-IR —
carrying `db.system.name` (a value identifying DataFusion as the query
engine), `db.operation.name` (specific to the query surface, e.g. the SQL
verb or the protocol name), and `db.namespace` (the tenant/dataset scope
the query executed against). When the query surface exposes literal text
(SQL, PromQL, LogQL, TraceQL), the span SHALL carry `db.query.text`
sanitized per the DB-semconv rules: literals replaced with placeholders,
parameterized values never inlined.

#### Scenario: Every query surface produces a DB client span

- **WHEN** a query executes through any of the SQL, PromQL, LogQL, TraceQL,
  or query-IR surfaces
- **THEN** the trace contains a CLIENT span with `db.system.name`,
  `db.operation.name`, and `db.namespace` set for that query

#### Scenario: A query surface with no prior span coverage is now covered

- **WHEN** a PromQL, LogQL, or TraceQL query executes with self-monitoring
  enabled
- **THEN** the trace contains a DB CLIENT span for that query, where
  previously no span existed around its execution

#### Scenario: Query text is sanitized on the DB client span

- **WHEN** a query containing string or numeric literals executes and its
  surface exposes query text
- **THEN** the CLIENT span's `db.query.text` carries placeholders in place
  of the literal values, never the raw literals

## MODIFIED Requirements

### Requirement: Query execution stage spans

Query execution in the querier SHALL emit INTERNAL child spans for its
major stages (planning, table/Iceberg scan, execution, result encoding)
nested under the DataFusion query CLIENT span (itself nested under the
Flight SERVER span), carrying result-size attributes (row/byte counts) in
the `signaldb.*` namespace. Any recorded query text SHALL follow the
DB-semconv sanitization rules: literals replaced with placeholders before
recording, and parameterized values never inlined.

#### Scenario: Slow query is attributable to a stage

- **WHEN** a query executes with self-monitoring enabled
- **THEN** its trace decomposes the DataFusion query CLIENT span into
  per-stage child spans such that the dominant time contributor is
  identifiable

#### Scenario: Query literals do not leak into telemetry

- **WHEN** a query containing string or numeric literals is recorded on a
  span
- **THEN** the recorded query text carries placeholders in place of the
  literal values
