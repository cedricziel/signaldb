## ADDED Requirements

### Requirement: Aggregates may be scoped by a predicate

An aggregate SHALL accept an optional predicate scoping which records it
consumes, so that a single grouped query can report both an overall measure and a
measure over a subset of the same groups. The predicate SHALL use the same
grammar and logical field names as the `where` stage, and SHALL be evaluated
against the records reaching the aggregate stage — the stage's grouping and any
earlier stage's filtering apply first.

Scoping SHALL be per-aggregate: aggregates within one stage may carry different
predicates, or none. An unscoped aggregate SHALL consume every record in its
group, unchanged from today.

#### Scenario: A scoped and an unscoped aggregate share one grouping

- **WHEN** a query groups records and declares both an unscoped count and a count
  scoped to a predicate
- **THEN** each group reports the total for the group and the count of only the
  records satisfying the predicate
- **AND** the grouping is performed once, not once per aggregate

#### Scenario: A group where no record satisfies the predicate

- **WHEN** a group contains no record satisfying a scoped aggregate's predicate
- **THEN** the group is still returned, with the scoped count reported as zero
- **AND** the group is not dropped from the result

#### Scenario: Scoping does not change the group set

- **WHEN** the same query is issued with and without a scoping predicate on one
  of its aggregates
- **THEN** both return the same groups in the same order under the same `order`
  stage, differing only in the scoped aggregate's values

#### Scenario: Scoped non-count aggregates measure only matching records

- **WHEN** a quantile or sum aggregate carries a scoping predicate
- **THEN** its value is computed over only the records in the group satisfying
  that predicate

#### Scenario: An invalid scoping predicate is rejected at validation

- **WHEN** a scoping predicate references a field that cannot be resolved, or
  uses an operator the predicate grammar does not define
- **THEN** the query is rejected at validation identifying the offending
  predicate, rather than executing with the scope ignored

#### Scenario: The scoping predicate is a structured operand

- **WHEN** a client supplies an aggregate's scope
- **THEN** it is expressed with the structured predicate grammar, and a scope
  supplied as an unparsed expression string is rejected
