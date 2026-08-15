# otlp-traces-ingestion Specification

## Purpose
Defines how SignalDB accepts OpenTelemetry trace exports over OTLP/gRPC and
OTLP/HTTP, including transport endpoints, wire encodings, response semantics,
and which span data is preserved for querying. Inherits authentication,
durability, and rate-limit/quota behavior from the shared ingest
capabilities.
## Requirements
### Requirement: OTLP trace export endpoints

The acceptor SHALL accept OpenTelemetry trace exports on both transports:
OTLP/gRPC via the `TraceService/Export` RPC (default port `4317`) and
OTLP/HTTP via `POST /v1/traces` (default port `4318`).

#### Scenario: gRPC trace export is accepted

- **WHEN** an authorized client calls `TraceService/Export` with a valid
  `ExportTraceServiceRequest`
- **THEN** the acceptor durably accepts the spans and returns an
  `ExportTraceServiceResponse`

#### Scenario: HTTP trace export is accepted

- **WHEN** an authorized client sends `POST /v1/traces` with a valid trace
  payload
- **THEN** the acceptor durably accepts the spans and returns `200 OK`

### Requirement: OTLP/HTTP trace encodings

The acceptor SHALL accept OTLP/HTTP trace bodies encoded as protobuf
(`application/x-protobuf`, the default when no content type is present) and
as OTLP/JSON (`application/json`, protojson with hex-encoded trace and span
ids). The success response body SHALL be an `ExportTraceServiceResponse`
encoded to match the request's encoding.

#### Scenario: Protobuf request yields protobuf response

- **WHEN** a `POST /v1/traces` request uses `application/x-protobuf` (or
  omits the content type)
- **THEN** the acceptor decodes protobuf and responds with a protobuf
  `ExportTraceServiceResponse`

#### Scenario: JSON request yields JSON response

- **WHEN** a `POST /v1/traces` request uses `application/json`
- **THEN** the acceptor decodes protojson and responds with a JSON body

#### Scenario: Malformed payload is rejected

- **WHEN** a trace request body cannot be decoded for its declared encoding
- **THEN** the acceptor responds `400 Bad Request` and ingests no data

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

