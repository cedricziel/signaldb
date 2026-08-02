# prometheus-remote-write Specification

## Purpose
Defines how SignalDB accepts Prometheus `remote_write` data at
`POST /api/v1/write` and stores it as metrics. This is not an OTLP signal,
but it rides the same acceptor and inherits authentication, durability, and
rate-limit/quota behavior from the shared ingest capabilities.
## Requirements
### Requirement: Prometheus remote_write endpoint

The acceptor SHALL accept Prometheus `remote_write` requests at
`POST /api/v1/write` (default port `4318`), supporting both the v1 and v2
remote_write protocol versions. Request bodies are snappy-compressed
protobuf (block format).

#### Scenario: Valid remote_write request is accepted

- **WHEN** an authorized Prometheus-compatible agent sends a valid
  snappy-compressed `remote_write` request to `POST /api/v1/write`
- **THEN** the acceptor durably accepts the samples and returns a success
  status

#### Scenario: Empty remote_write request is a no-op

- **WHEN** a decoded `remote_write` request contains no time series
- **THEN** the acceptor accepts it without storing any data

#### Scenario: Undecodable body is rejected

- **WHEN** a `remote_write` body cannot be snappy-decompressed or
  protobuf-decoded
- **THEN** the acceptor responds with a client error (`400`) and ingests no
  data

### Requirement: Conversion to the metrics store

The acceptor SHALL convert Prometheus `remote_write` time series into the
same metrics representation used for OTLP metrics, so that ingested
Prometheus samples are queryable alongside OTLP-ingested metrics.

#### Scenario: Samples are stored as metrics

- **WHEN** a `remote_write` request carries labelled time series with samples
- **THEN** the acceptor converts them to metrics and durably stores them for
  PromQL-compatible querying

