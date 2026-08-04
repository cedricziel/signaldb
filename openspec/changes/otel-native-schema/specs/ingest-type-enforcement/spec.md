## Purpose

Requires the OTLP ingest path to route attribute values through the logical
schema and registry so that each value's canonical type is asserted and stored
at write time, rather than reconstructed by casting a stringified value at read.

## ADDED Requirements

### Requirement: Ingest resolves and stores the canonical type at write

The ingest path SHALL resolve each attribute's canonical type through the
registry (semconv → `AnyValue` → config) and encode the value into the typed
physical substrate under that type before persistence. Ingest SHALL NOT persist
attribute values as an untyped string map that requires read-time type
reconstruction.

#### Scenario: Typed value is stored typed

- **WHEN** a record with a typed attribute is ingested
- **THEN** the value is written into the typed substrate under its canonical
  type, and a later read returns it typed without a read-time cast

#### Scenario: Semconv-typed key is coerced at write

- **WHEN** a sender transmits a semantic-convention key under a different
  `AnyValue` type than the convention declares (e.g. a numeric status code as a
  string)
- **THEN** ingest coerces the value to the registry's canonical type where the
  coercion is lossless, and records the value typed

### Requirement: Ingest never drops records on type conflict or coercion failure

The ingest path SHALL NOT reject or silently discard a record because an
attribute's sent type conflicts with the canonical type or cannot be losslessly
coerced. The value SHALL be retained (under its sent type's home or the residue),
and the condition SHALL be observable.

#### Scenario: Lossy coercion falls back to retention

- **WHEN** an attribute value cannot be losslessly coerced to the canonical type
- **THEN** the record is still ingested, the value is retained without loss, and
  the event is surfaced (metric/log) rather than dropped

### Requirement: Ingest wire format compatibility during migration

Enforcing types at write SHALL NOT require a breaking change to the ingest wire
or the write-ahead log in the first phase; a JSON-carried attribute
representation on the wire SHALL remain acceptable and be typed at the storage
boundary.

#### Scenario: Existing OTLP clients keep working

- **WHEN** an OTLP client sends attributes exactly as it does today
- **THEN** ingestion succeeds and the values are stored typed, with no client-side
  change required
