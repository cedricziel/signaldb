## ADDED Requirements

### Requirement: Database client spans for query execution

DataFusion query execution SHALL produce CLIENT spans following the stable
database semantic conventions, across every query surface the querier
serves — raw SQL, PromQL, LogQL, TraceQL, and the native query-IR —
carrying `db.system.name` (a value identifying DataFusion as the query
engine) and `db.namespace` (the tenant/dataset scope the query executed
against). `db.operation.name` SHALL be one fixed literal per query surface
— `"query"` for raw SQL, `"promql_query"`, `"logql_query"`,
`"traceql_query"`, `"query_ir"` for the other four — never derived by
parsing the submitted query text, so the attribute's value set is exactly
these five literals regardless of what a client submits. When the query
surface exposes literal text (SQL, PromQL, LogQL, TraceQL), the span SHALL
carry `db.query.text` sanitized per the DB-semconv rules: literals replaced
with placeholders, parameterized values never inlined, using the same
sanitized value recorded on any other span attribute that also carries
that query's text (e.g. `signaldb.query.text`) — never a second,
independently-derived copy. If a surface's literal shape cannot be safely
sanitized, `db.query.text` SHALL be omitted for that surface rather than
carrying unsanitized text.

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

#### Scenario: `db.operation.name` never grows beyond the fixed set

- **WHEN** a raw SQL query of any statement shape (`SELECT`, a query with
  CTEs, or any other text the client submits) executes
- **THEN** the CLIENT span's `db.operation.name` is exactly `"query"`, not
  a value parsed from the submitted text

#### Scenario: Query text is sanitized on the DB client span

- **WHEN** a query containing string or numeric literals executes and its
  surface exposes query text
- **THEN** the CLIENT span's `db.query.text` carries placeholders in place
  of the literal values, never the raw literals

#### Scenario: Every recorded copy of query text is sanitized identically

- **WHEN** a query's text is recorded on both the DB client span's
  `db.query.text` and any other span attribute for the same query
- **THEN** both attributes carry the same sanitized text — neither carries
  a raw literal the other has scrubbed

### Requirement: Query-execution metric attributes are a fixed low-cardinality allowlist

Metrics recorded for DataFusion query execution (`signaldb.query.duration`,
`signaldb.query.errors`, `signaldb.query.rows_returned`) SHALL carry only
`db.system.name` and `db.operation.name` from the DB-semconv attribute set,
alongside the pre-existing `query_type` attribute. `db.namespace` and
`db.query.text` SHALL NOT be recorded as metric attributes.

#### Scenario: Metric attributes exclude tenant scope and query text

- **WHEN** a query execution metric is recorded
- **THEN** its attributes include `db.system.name`, `db.operation.name`,
  and `query_type`, and do not include `db.namespace` or `db.query.text`

## MODIFIED Requirements

### Requirement: Query execution stage spans

Query execution in the querier SHALL emit INTERNAL child spans for its two
stages — planning (`signaldb.query.plan`) and execution
(`signaldb.query.execute`, which covers scan and result encoding as part
of the same stage) — nested under the DataFusion query CLIENT span (itself
nested under the Flight SERVER span), carrying result-size attributes
(row/byte counts) in the `signaldb.*` namespace. Any recorded query text
SHALL follow the DB-semconv sanitization rules: literals replaced with
placeholders before recording, and parameterized values never inlined,
using the same sanitized value as the DB client span's `db.query.text` for
the same query.

#### Scenario: Slow query is attributable to a stage

- **WHEN** a query executes with self-monitoring enabled
- **THEN** its trace decomposes the DataFusion query CLIENT span into a
  planning child span and an execution child span such that the dominant
  time contributor between the two is identifiable

#### Scenario: Query literals do not leak into telemetry

- **WHEN** a query containing string or numeric literals is recorded on a
  span
- **THEN** the recorded query text carries placeholders in place of the
  literal values
