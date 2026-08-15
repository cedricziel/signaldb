## Purpose

Guarantees that a signal's declared schema and what its ingest/query code
actually does agree with each other, so a field can never be exposed as
queryable while silently carrying no real data, or persisted while
invisible to query.

## ADDED Requirements

### Requirement: Every declared field is backed by real conversion behavior

For every signal, every non-computed field declared in that signal's
current schema definition SHALL have a corresponding read path (from the
signal's native ingest format into storage) and, where the signal supports
export, a corresponding write path back out. A field that is declared but
has no such path SHALL be treated as a defect surfaced before or at
deployment, never as a silently always-empty or always-default value at
query time.

#### Scenario: A newly declared field without a conversion path fails loudly

- **WHEN** a schema definition declares a new physical field for a signal
  but the signal's conversion code has no path that reads or writes it
- **THEN** this is caught as a failure before the mismatch can reach a
  running deployment, rather than surfacing later as a query that always
  returns an empty or default result

#### Scenario: A query-registered field matches real data

- **WHEN** a field is resolvable through the query/registry layer for a
  signal
- **THEN** that field has a real physical column that the signal's ingest
  path actually populates, so querying it reflects genuine ingested data

### Requirement: Generated schema representations are behaviorally identical to their hand-written predecessors

Where a schema representation (wire format, physical layout, or
query-registry entry) is produced by resolving a signal's schema
definition instead of being hand-written, the generated representation
SHALL be indistinguishable in behavior from what existed before generation
replaced hand-authoring: same fields, same types, same nullability, same
query results for existing data.

#### Scenario: Generated wire schema matches the previous hand-written one

- **WHEN** a signal's Flight wire schema is produced by resolving its
  schema definition
- **THEN** it declares exactly the same fields, types, and nullability as
  the hand-written schema it replaces
