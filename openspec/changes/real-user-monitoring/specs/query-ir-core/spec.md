## ADDED Requirements

### Requirement: Approximate distinct-count aggregate

The `aggregate` stage SHALL accept `fn: "count_distinct"` with an `of` field
of any type, returning an integer estimate of the number of distinct
non-null values of that field in the group. The estimate SHALL be computed
with a bounded-memory sketch (DataFusion `approx_distinct`, HyperLogLog), so
its cost does not grow with the number of distinct values, and the IR
documentation SHALL state it is approximate. It SHALL accept a scope
predicate like every other aggregate. Introducing it SHALL bump the IR
version; documents at earlier versions that use it SHALL be rejected at
validation.

#### Scenario: Counting sessions

- **WHEN** a logs query aggregates
  `{"fn": "count_distinct", "of": "session.id", "as": "sessions"}` over
  records carrying 1,000 distinct `session.id` values
- **THEN** `sessions` is within 2% of 1,000

#### Scenario: Scoped distinct count

- **WHEN** the same query also declares `count_distinct` of `session.id`
  scoped to `event.name = exception`
- **THEN** that column counts only sessions with at least one exception
  record, and groups without one report zero

#### Scenario: Nulls are not a value

- **WHEN** some records in a group have no `user.id`
- **THEN** `count_distinct` of `user.id` counts only the records' present
  values

#### Scenario: Version gate

- **WHEN** a document declaring the previous IR version uses `count_distinct`
- **THEN** validation rejects it naming the version that introduced it
