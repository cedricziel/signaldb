## Purpose

Requires the OTLP ingest path to route attribute values through the logical
schema and registry so that each value's canonical type is asserted and stored
at write time, rather than reconstructed by casting a stringified value at read.

## ADDED Requirements

### Requirement: Ingest routes by the canonical authority, never rewriting the sender's value

The ingest path SHALL resolve each attribute through the registry using the same
canonical authority as reads — precedence **config → semconv hint → observed
`AnyValue`** (see `attribute-type-authority`) — and route the value to the field's
canonical typed home when its sent type matches, or to the structured residue when
it does not. Ingest SHALL store the value **as sent**; it SHALL NOT coerce or
rewrite the sender's value, and SHALL NOT persist attributes as an untyped string
map requiring read-time reconstruction.

#### Scenario: Matching-type value is stored in the canonical home

- **WHEN** a record's attribute matches the field's canonical type
- **THEN** the value is written to the typed home and a later read returns it typed
  without a read-time cast

#### Scenario: Off-type value is routed to the residue, not coerced

- **WHEN** a sender transmits a key under a different `AnyValue` type than the
  field's canonical type (e.g. a semconv-integer key sent as a string)
- **THEN** ingest retains the value **as sent** in the residue (not coerced to the
  canonical type), and the occurrence is recorded as an off-type mismatch

### Requirement: Ingest never drops records on type mismatch

The ingest path SHALL NOT reject or silently discard a record because an
attribute's sent type does not match the canonical type. The value SHALL be
retained losslessly (in the residue), and the condition SHALL be observable.

#### Scenario: Off-type value is retained, not dropped

- **WHEN** an attribute value does not match the field's canonical type
- **THEN** the record is still ingested, the value is retained without loss in the
  residue, and the mismatch is surfaced (metric/log) rather than dropped

### Requirement: Ingest wire format compatibility during migration

Enforcing types at write SHALL NOT require a breaking change to the ingest wire
or the write-ahead log in the first phase; a JSON-carried attribute
representation on the wire SHALL remain acceptable and be typed at the storage
boundary.

#### Scenario: Existing OTLP clients keep working

- **WHEN** an OTLP client sends attributes exactly as it does today
- **THEN** ingestion succeeds and the values are stored typed, with no client-side
  change required
