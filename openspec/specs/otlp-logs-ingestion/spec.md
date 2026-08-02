# otlp-logs-ingestion Specification

## Purpose
Defines how SignalDB accepts OpenTelemetry log exports over OTLP/gRPC and
OTLP/HTTP, including transport endpoints, wire encodings, response semantics,
and which log record data is preserved for querying. Inherits
authentication, durability, and rate-limit/quota behavior from the shared
ingest capabilities.
## Requirements
### Requirement: OTLP log export endpoints

The acceptor SHALL accept OpenTelemetry log exports on both transports:
OTLP/gRPC via the `LogsService/Export` RPC (default port `4317`) and
OTLP/HTTP via `POST /v1/logs` (default port `4318`).

#### Scenario: gRPC log export is accepted

- **WHEN** an authorized client calls `LogsService/Export` with a valid
  `ExportLogsServiceRequest`
- **THEN** the acceptor durably accepts the log records and returns an
  `ExportLogsServiceResponse`

#### Scenario: HTTP log export is accepted

- **WHEN** an authorized client sends `POST /v1/logs` with a valid log
  payload
- **THEN** the acceptor durably accepts the log records and returns `200 OK`

### Requirement: OTLP/HTTP log encodings

The acceptor SHALL accept OTLP/HTTP log bodies encoded as protobuf
(`application/x-protobuf`, the default when no content type is present) and
as OTLP/JSON (`application/json`, protojson). The success response body SHALL
be an `ExportLogsServiceResponse` encoded to match the request's encoding.

#### Scenario: Protobuf request yields protobuf response

- **WHEN** a `POST /v1/logs` request uses `application/x-protobuf` (or omits
  the content type)
- **THEN** the acceptor decodes protobuf and responds with a protobuf
  `ExportLogsServiceResponse`

#### Scenario: JSON request yields JSON response

- **WHEN** a `POST /v1/logs` request uses `application/json`
- **THEN** the acceptor decodes protojson and responds with a JSON body

#### Scenario: Malformed payload is rejected

- **WHEN** a log request body cannot be decoded for its declared encoding
- **THEN** the acceptor responds `400 Bad Request` and ingests no data

### Requirement: Log record data preservation

The acceptor SHALL preserve OpenTelemetry log record fields required for
LogQL-compatible querying, including timestamp and observed timestamp,
severity, body, attributes, and any trace/span correlation identifiers,
along with resource and scope attributes.

#### Scenario: Trace-correlated log record is retained

- **WHEN** an accepted log record carries a trace id and span id
- **THEN** the stored log record retains that correlation for later query

