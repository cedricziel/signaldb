## MODIFIED Requirements

### Requirement: Database client spans for catalog access

Operations against the SQL catalog (SQLite or PostgreSQL) SHALL produce
CLIENT spans following the stable database semantic conventions:
`db.system.name` (`sqlite` or `postgresql`), `db.operation.name`,
`db.namespace`, and sanitized `db.query.text` (literals replaced with
placeholders before recording; bound/parameterized values never inlined),
named per the DB span-naming precedence (never raw SQL as the span name).

#### Scenario: Catalog query is visible as a DB client span

- **WHEN** a service performs a catalog read while serving a traced request
- **THEN** the trace contains a CLIENT span with `db.system.name` and
  `db.operation.name` beneath the serving span

#### Scenario: Catalog statement text is captured and sanitized

- **WHEN** a service issues a catalog register, heartbeat, list, or
  deregister ingester operation
- **THEN** the resulting CLIENT span carries `db.query.text` with any
  literal values replaced by placeholders
