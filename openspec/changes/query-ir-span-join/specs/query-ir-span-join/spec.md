## Purpose

Lets a Query IR pipeline over `traces` join each span to its parent span in the same trace, so callers can ask which service called which and aggregate over caller/callee pairs.

## ADDED Requirements

### Requirement: Span-to-parent correlate stage

The Query IR SHALL accept a `correlate` stage, from IR version 8, that joins each row of a `traces` relation to the span in the same trace whose `span_id` equals the row's `parent_span_id`. The stage SHALL take a join kind of `inner` or `left`. A document at IR version 7 or earlier that contains `correlate` SHALL be rejected with a validation error naming the minimum version.

#### Scenario: Inner join pairs child and parent spans

- **WHEN** a client submits a version 8 document over `traces` with a `correlate` stage of kind `inner`, and a trace holds span B whose parent is span A
- **THEN** the result holds a row for B carrying B's fields and A's fields
- **AND** the trace's root span, having no parent, produces no row

#### Scenario: Left join keeps spans without a parent

- **WHEN** the same query uses kind `left`
- **THEN** the root span produces a row whose parent fields are all null

#### Scenario: Older IR version is rejected

- **WHEN** a document declares IR version 7 and contains a `correlate` stage
- **THEN** the server returns a validation error stating that `correlate` requires IR version 8

### Requirement: Parent fields are addressed under a fixed prefix

After a `correlate` stage, every field of the parent span SHALL be addressable as `parent.<field>` — including attribute scopes (`parent.span.<key>`, `parent.resource.<key>`) — and the child span's fields SHALL keep their unprefixed names. Later `where` and `aggregate` stages SHALL accept fields from both sides.

#### Scenario: Group by caller and callee service

- **WHEN** a pipeline runs `correlate` (inner), then `where parent.service_name != service_name`, then `aggregate count() by parent.service_name, service_name`
- **THEN** the result holds one row per distinct caller/callee service pair with its call count

#### Scenario: Unknown parent field is rejected

- **WHEN** a stage after `correlate` references `parent.no_such_field`
- **THEN** validation fails and names the unresolved field, as it does for an unknown unprefixed field

### Requirement: Correlate placement rules

The server SHALL reject a document where `correlate` is used on a source other than `traces`, appears more than once, or appears after an `aggregate` stage. Each rejection SHALL be a validation error that names the rule broken.

#### Scenario: Correlate on logs is rejected

- **WHEN** a document over `logs` contains a `correlate` stage
- **THEN** validation fails stating that span correlation requires the `traces` source

#### Scenario: Correlate after aggregate is rejected

- **WHEN** `correlate` follows an `aggregate` stage
- **THEN** validation fails stating that `correlate` must precede `aggregate`

### Requirement: Correlate respects the query window and a row bound

Both sides of the join SHALL be read from the query's time range only. A parent span that starts outside the window SHALL be treated as missing: under `inner` its child is dropped, under `left` the parent fields are null. The number of joined rows SHALL be capped by a server-side limit; when the cap is reached the result SHALL carry a warning stating that it was truncated, and the query SHALL NOT fail.

#### Scenario: Parent outside the window

- **WHEN** a child span falls inside the query window but its parent starts before it
- **THEN** a `left` join returns the child with null parent fields and an `inner` join omits it

#### Scenario: Row bound reached

- **WHEN** the joined relation exceeds the server's row limit
- **THEN** the query succeeds with the rows up to the limit and a warning stating the result was truncated by the correlate row limit

### Requirement: Tenant isolation holds across the join

Both sides of a `correlate` SHALL be read from the caller's tenant and dataset only. A parent span stored under another tenant or dataset SHALL be treated as missing.

#### Scenario: Cross-tenant parent is not joined

- **WHEN** a span in tenant A names a parent span ID that only exists in tenant B
- **THEN** a query by tenant A treats the parent as missing
