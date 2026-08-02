## Purpose

Defines how SignalDB accepts OpenTelemetry metric exports over OTLP/gRPC and
OTLP/HTTP, including transport endpoints, wire encodings, response semantics,
and which metric types and data are preserved for querying. Inherits
authentication, durability, and rate-limit/quota behavior from the shared
ingest capabilities.

## ADDED Requirements

### Requirement: OTLP metric export endpoints

The acceptor SHALL accept OpenTelemetry metric exports on both transports:
OTLP/gRPC via the `MetricsService/Export` RPC (default port `4317`) and
OTLP/HTTP via `POST /v1/metrics` (default port `4318`).

#### Scenario: gRPC metric export is accepted

- **WHEN** an authorized client calls `MetricsService/Export` with a valid
  `ExportMetricsServiceRequest`
- **THEN** the acceptor durably accepts the metrics and returns an
  `ExportMetricsServiceResponse`

#### Scenario: HTTP metric export is accepted

- **WHEN** an authorized client sends `POST /v1/metrics` with a valid metric
  payload
- **THEN** the acceptor durably accepts the metrics and returns `200 OK`

### Requirement: OTLP/HTTP metric encodings

The acceptor SHALL accept OTLP/HTTP metric bodies encoded as protobuf
(`application/x-protobuf`, the default when no content type is present) and
as OTLP/JSON (`application/json`, protojson). The success response body SHALL
be an `ExportMetricsServiceResponse` encoded to match the request's
encoding.

#### Scenario: Protobuf request yields protobuf response

- **WHEN** a `POST /v1/metrics` request uses `application/x-protobuf` (or
  omits the content type)
- **THEN** the acceptor decodes protobuf and responds with a protobuf
  `ExportMetricsServiceResponse`

#### Scenario: JSON request yields JSON response

- **WHEN** a `POST /v1/metrics` request uses `application/json`
- **THEN** the acceptor decodes protojson and responds with a JSON body

#### Scenario: Malformed payload is rejected

- **WHEN** a metric request body cannot be decoded for its declared encoding
- **THEN** the acceptor responds `400 Bad Request` and ingests no data

### Requirement: Supported metric types

The acceptor SHALL accept all OpenTelemetry metric data types — Gauge, Sum,
Histogram, Exponential Histogram, and Summary — preserving their data points,
attributes, timestamps, and aggregation semantics (temporality and
monotonicity where applicable) for PromQL-compatible querying.

#### Scenario: Each metric type is accepted and preserved

- **WHEN** an export contains Gauge, Sum, Histogram, Exponential Histogram,
  or Summary metrics
- **THEN** the acceptor durably accepts each type and preserves its data
  points and aggregation semantics
