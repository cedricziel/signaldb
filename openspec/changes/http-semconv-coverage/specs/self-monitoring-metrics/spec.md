## ADDED Requirements

### Requirement: HTTP server metrics on every HTTP surface

Every HTTP surface SignalDB serves — router query/API, acceptor OTLP/HTTP
and remote-write, compactor observability endpoints (including `/metrics`,
`/health`, and `/status`), and the MCP server (including its public
discovery document) — SHALL record the HTTP server metrics defined by the
OpenTelemetry HTTP semantic conventions: request duration, in-flight
requests, and request/response body sizes.

Request duration SHALL carry `http.request.method`, a `url.scheme` derived
from the request rather than a fixed literal, `http.response.status_code`,
`http.route` whenever a route template matched, and `error.type` whenever
the request failed. It SHALL NOT carry tenant identity.

#### Scenario: Latency is attributable to an endpoint

- **WHEN** requests are served on two different routes of the same service
- **THEN** their durations are recorded under distinct `http.route` values,
  so one slow endpoint is distinguishable from another

#### Scenario: Scheme reflects the request

- **WHEN** a request arrives over TLS
- **THEN** the recorded `url.scheme` is `https`, not a hardcoded `http`

#### Scenario: A previously uninstrumented surface is measured

- **WHEN** the compactor's `/metrics`, `/status`, or `/health` endpoint is
  requested
- **THEN** the request is recorded in the HTTP server metrics like any other
  route

#### Scenario: Unmatched requests are still counted

- **WHEN** a request matches no route and receives a 404
- **THEN** its duration is still recorded, without an `http.route`
  attribute, so unmatched traffic is visible rather than silently dropped

#### Scenario: Failures are classified

- **WHEN** a request fails with a server error
- **THEN** the recorded duration carries `error.type`

### Requirement: Server metric and span attributes agree

The HTTP server metrics and the HTTP server span for one request SHALL be
derived from a single attribute set, so that the values they share —
`http.route`, `http.request.method`, `url.scheme`,
`http.response.status_code`, `error.type` — are identical for that request.

#### Scenario: A metric series pivots to its explaining spans

- **WHEN** an operator finds a slow series for one `http.route` and queries
  spans filtered by the same route value
- **THEN** the spans returned are the requests that produced that series

### Requirement: HTTP client metrics for object storage

Every HTTP request SignalDB issues to object storage SHALL be recorded in
the HTTP client metrics defined by the OpenTelemetry HTTP semantic
conventions — request duration, in-flight requests, and request/response
body sizes — carrying `http.request.method`, `server.address`,
`server.port`, `http.response.status_code` when a response was received, and
`error.type` when the attempt failed. Each transport attempt SHALL be
recorded individually, so retried requests are visible as separate
measurements.

#### Scenario: Storage latency is separable from query latency

- **WHEN** a query executes against data in object storage
- **THEN** the time spent on object-storage HTTP requests is recorded
  independently of the query's total duration

#### Scenario: Retried storage requests are visible

- **WHEN** an object-storage request fails and is retried
- **THEN** each attempt is recorded, with the failing attempts carrying
  `error.type`

#### Scenario: Local and in-memory stores emit no HTTP client metrics

- **WHEN** SignalDB is configured with a filesystem or in-memory store
- **THEN** no HTTP client metrics are recorded for storage access

### Requirement: HTTP client metrics for the client SDK

The SignalDB client SDK SHALL record HTTP client metrics for every request
it issues, carrying `http.request.method`, `server.address`, `server.port`,
`http.response.status_code` when a response was received, `error.type` on
failure, `url.template` identifying the API operation, and
`http.request.resend_count` when the request was retried. Recording SHALL be
inert when the embedding application has installed no meter provider.

#### Scenario: Retry behavior is measurable by the caller

- **WHEN** an SDK operation is retried after a throttling response
- **THEN** the recorded request carries a non-zero
  `http.request.resend_count`

#### Scenario: Operation identity is low-cardinality

- **WHEN** SDK requests are recorded
- **THEN** they are grouped by `url.template` (the API operation), never by
  a URL containing path parameters

#### Scenario: No telemetry without a provider

- **WHEN** an application uses the SDK without installing an OpenTelemetry
  meter provider
- **THEN** the SDK records nothing and incurs no export
