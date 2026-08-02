## Purpose

Defines the UI-facing surfaces around core IR execution: build-side **discovery**
(queryable signals, fields, values, relationship hints over the same logical,
registry-mediated namespace as `query-ir-core`) and delivery-side **live tail +
pagination** for query results. Stub scope; requirements below are the headline
guarantees to be expanded when the change is picked up.

## ADDED Requirements

### Requirement: Field discovery over the logical namespace

SignalDB SHALL expose, per signal source and tenant, the set of queryable fields
as logical dotted OTel-native names with their canonical types, such that any
field returned by discovery is valid to reference in a `query-ir-core` predicate.
Discovery SHALL NOT expose physical column names or storage details, and SHALL
NOT distinguish promoted from unpromoted fields in a way that changes which names
are valid.

#### Scenario: Discovered fields are queryable

- **WHEN** a client requests the queryable fields for a signal source over a time
  range
- **THEN** each returned field is a logical name with a canonical type that can be
  used directly in an IR predicate, with no physical/storage reference

### Requirement: Scoped value suggestions

SignalDB SHALL provide value suggestions for a field, scoped to a time range and
optionally to the predicates chosen so far, so the builder can offer "what can I
add next" rather than a static catalog.

#### Scenario: Values narrow with context

- **WHEN** a client requests values for a field given a partial set of filters
  and a time range
- **THEN** the suggested values reflect that scope, bounded by documented
  cardinality limits

### Requirement: Live tail of an IR query

SignalDB SHALL provide a streaming channel that delivers new records matching a
submitted IR query as they arrive, using the same IR document as the unary query
surface. The stream SHALL be tenant-scoped and authenticated identically to the
unary surface.

#### Scenario: New matching rows stream as they arrive

- **WHEN** a client opens a tail on an IR query and matching data is subsequently
  ingested
- **THEN** the new matching records are delivered over the stream without the
  client re-submitting the query

### Requirement: Pagination of large results

SignalDB SHALL support walking a large `rows`/`trace` result in bounded pages via
an opaque continuation token, so a client can retrieve the full result set
without a single unbounded response.

#### Scenario: A large result is walked in pages

- **WHEN** a query result exceeds a single page and the client requests the next
  page with the returned continuation token
- **THEN** the subsequent page continues from where the previous one ended,
  within documented page-size and total-scan bounds
