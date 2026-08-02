## Purpose

Defines how SignalDB accepts OpenTelemetry profiling data over the OTLP
profiles signal, which is at development (`v1development`) maturity. Covers
the transport endpoints, encodings, and response semantics. Inherits
authentication, durability, and rate-limit/quota behavior from the shared
ingest capabilities.

## ADDED Requirements

### Requirement: OTLP profiles export endpoints

The acceptor SHALL accept OpenTelemetry profile exports at development
maturity on both transports: OTLP/gRPC via the development
`ProfilesService/Export` RPC (default port `4317`) and OTLP/HTTP via
`POST /v1development/profiles` (default port `4318`).

#### Scenario: gRPC profiles export is accepted

- **WHEN** an authorized client calls the development `ProfilesService/Export`
  RPC with a valid `ExportProfilesServiceRequest`
- **THEN** the acceptor durably accepts the profiles and returns an export
  response

#### Scenario: HTTP profiles export is accepted

- **WHEN** an authorized client sends `POST /v1development/profiles` with a
  valid profiles payload
- **THEN** the acceptor durably accepts the profiles and returns `200 OK`

### Requirement: OTLP/HTTP profiles encodings

The acceptor SHALL accept OTLP/HTTP profiles bodies encoded as protobuf
(`application/x-protobuf`, the default when no content type is present) and
as OTLP/JSON (`application/json`, protojson).

#### Scenario: Protobuf and JSON payloads are accepted

- **WHEN** a `POST /v1development/profiles` request uses
  `application/x-protobuf` (or omits the content type) or `application/json`
- **THEN** the acceptor decodes the body accordingly and durably accepts the
  profiles

#### Scenario: Malformed payload is rejected

- **WHEN** a profiles request body cannot be decoded for its declared
  encoding
- **THEN** the acceptor responds `400 Bad Request` and ingests no data

### Requirement: Development-maturity signal

The acceptor SHALL expose the profiles signal at the OpenTelemetry
`v1development` maturity level, reflecting that the OTLP profiles
specification is not yet stable and its wire shape may change.

#### Scenario: Development endpoint path is used

- **WHEN** a client integrates profile ingestion
- **THEN** it targets the `v1development` profiles endpoint rather than a
  stable `v1` path
