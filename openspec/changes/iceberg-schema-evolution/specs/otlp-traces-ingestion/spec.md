## MODIFIED Requirements

### Requirement: Span data preservation

The acceptor SHALL preserve OpenTelemetry span fields required for
Tempo-compatible querying, including trace and span identifiers, parent
linkage, name, kind, start/end timestamps, status, and attributes, along
with resource and scope attributes. Span events and span exceptions SHALL be
preserved so they can be surfaced on the trace view.

Span kind and status code are OTel numeric enumerations. The stored span
SHALL preserve the original numeric value for each, in addition to any
derived display string; the display string SHALL be computed from the
numeric value, never the other way around, so a defect in the string
mapping cannot destroy the original value. Dropped-attribute, dropped-event,
and dropped-link counts on a span SHALL be preserved rather than discarded;
a query against these counts SHALL reflect the original span, never a
default placeholder for data that was actually present.

#### Scenario: Spans with events and exceptions are retained

- **WHEN** an accepted span carries events and recorded exceptions
- **THEN** the stored span retains those events and exceptions for later
  query

#### Scenario: Span kind survives a display-string defect

- **WHEN** a span is ingested with a given OTel `kind` value
- **THEN** the stored numeric `kind` matches the original value regardless
  of what the derived display string computed from it happens to be

#### Scenario: Dropped counts are queryable and accurate

- **WHEN** an accepted span reports a nonzero dropped-attributes,
  dropped-events, or dropped-links count
- **THEN** querying that count on the stored span returns the original
  nonzero value, not zero
