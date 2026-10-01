## MODIFIED Requirements

### Requirement: HTTP server spans on every HTTP surface

Every HTTP surface SignalDB serves (router query/API, acceptor OTLP/HTTP
and remote-write, compactor observability endpoints, MCP server HTTP
including its public discovery document) SHALL produce one SERVER span per
request following the stable HTTP semantic conventions: name
`{method} {http.route}` (falling back to `{method}` when no route template
exists, never the raw path), the required `http.request.method`,
`url.path`, `url.scheme` attributes, `http.route` and
`http.response.status_code` when available, and W3C trace-context parent
adoption from inbound headers. Server spans SHALL set status Error only for
5xx or transport failure — never for 4xx alone — and SHALL then carry
`error.type`.

Instrumentation SHALL be a property of serving HTTP, not an opt-in step per
surface: a surface SHALL NOT be servable without it, so that adding a new
HTTP endpoint cannot silently produce unmeasured traffic.

#### Scenario: Ingest request over OTLP/HTTP joins the caller's trace

- **WHEN** a client sends `POST /v1/traces` to the acceptor with a
  `traceparent` header
- **THEN** the acceptor's exported SERVER span is a child of the caller's
  span and is named `POST /v1/traces`

#### Scenario: Client error does not mark the server span failed

- **WHEN** a request is rejected with a 4xx status
- **THEN** the SERVER span records `http.response.status_code` but its
  status is not Error

#### Scenario: Compactor observability endpoints produce spans

- **WHEN** the compactor's `/metrics`, `/status`, or `/health` endpoint is
  requested
- **THEN** a SERVER span is exported for that request, named by its route

#### Scenario: A new HTTP surface cannot ship uninstrumented

- **WHEN** a service is written to serve HTTP without going through the
  shared instrumented serving path
- **THEN** CI rejects it

## ADDED Requirements

### Requirement: HTTP client spans for outbound requests

Every HTTP request SignalDB issues — to object storage, and from the client
SDK — SHALL produce a CLIENT span following the HTTP semantic conventions,
named by the request method (optionally disambiguated by a low-cardinality
operation identifier, never the raw URL), carrying `http.request.method`,
`server.address`, `server.port`, `http.response.status_code` when a response
was received, and `error.type` on failure. Trace context SHALL be propagated
on outbound requests so the callee can join the caller's trace.

#### Scenario: Object-storage access appears in the query trace

- **WHEN** a query reads data from object storage
- **THEN** the exported trace contains CLIENT spans for the storage requests
  it issued, as descendants of the query's span

#### Scenario: SDK calls join the server trace

- **WHEN** a CLI or MCP-server operation calls SignalDB through the SDK
- **THEN** the SDK's CLIENT span is the parent of the server's SERVER span
  for that request

### Requirement: Outbound URLs are sanitized before recording

When a CLIENT span records a request URL, credentials and signing material
SHALL be redacted first, so that presigned object-storage URLs and any
query-string credentials never appear in exported telemetry.

#### Scenario: Presigned storage URL is redacted

- **WHEN** a CLIENT span is recorded for a presigned object-storage request
- **THEN** the recorded URL carries no signature, credential, or token
  parameter values
