## Purpose

Defines how every SignalDB ingest request is authenticated and bound to a
tenant and dataset, so that ingested telemetry is isolated per tenant and
only accepted from callers holding a valid API key with the required write
scope. Shared by all OTLP signals and the Prometheus `remote_write` path.

## ADDED Requirements

### Requirement: Bearer API-key authentication

The acceptor SHALL authenticate every ingest request using an API key
presented as an HTTP `Authorization: Bearer <api-key>` header (OTLP/HTTP and
Prometheus `remote_write`) or the equivalent gRPC `authorization` metadata
(OTLP/gRPC). The same credential model and validators apply across both
transports.

#### Scenario: Valid API key is accepted

- **WHEN** a request presents `Authorization: Bearer <valid-key>` together
  with a valid tenant identifier
- **THEN** the acceptor resolves an authenticated tenant context and
  proceeds to ingest

#### Scenario: Missing or non-Bearer credentials are rejected

- **WHEN** a request omits the `Authorization` header or uses a scheme other
  than `Bearer`
- **THEN** the acceptor rejects the request as unauthenticated (OTLP/HTTP
  `401`; OTLP/gRPC `UNAUTHENTICATED`) and does not ingest any data

#### Scenario: Unknown or invalid API key is rejected

- **WHEN** a request presents a Bearer token that does not resolve to a
  known API key for the given tenant
- **THEN** the acceptor rejects the request as unauthenticated and ingests
  no data

### Requirement: Tenant and dataset resolution

The acceptor SHALL require a tenant identifier on every ingest request via
the `x-tenant-id` header (or gRPC metadata) and SHALL accept an optional
`x-dataset-id`; when the dataset is omitted the tenant's default dataset is
used. Both identifiers are validated before use, and the resolved
`(tenant, dataset)` pair scopes all downstream storage and isolation.

#### Scenario: Missing tenant identifier is rejected

- **WHEN** an authenticated request omits `x-tenant-id`
- **THEN** the acceptor rejects the request (OTLP/gRPC `UNAUTHENTICATED`,
  OTLP/HTTP `401`) and ingests no data

#### Scenario: Invalid tenant or dataset identifier is rejected

- **WHEN** a request supplies an `x-tenant-id` or `x-dataset-id` that fails
  identifier validation
- **THEN** the acceptor rejects the request as a bad request and ingests no
  data

#### Scenario: Dataset defaults when omitted

- **WHEN** an authenticated request supplies a valid `x-tenant-id` but no
  `x-dataset-id`
- **THEN** the acceptor resolves the tenant's default dataset and ingests
  into it

### Requirement: Per-signal write scopes

When an API key carries explicit scopes, the acceptor SHALL require the
`<signal>:write` scope matching the signal being ingested (`traces:write`,
`logs:write`, `metrics:write`, `profiles:write`). An API key with no
explicit scopes is treated as a legacy unrestricted key and MAY ingest any
signal.

#### Scenario: Key with matching write scope may ingest

- **WHEN** a scoped API key holding `traces:write` exports traces
- **THEN** the acceptor accepts the export

#### Scenario: Key lacking the required write scope is rejected

- **WHEN** a scoped API key that does not hold `metrics:write` exports
  metrics
- **THEN** the acceptor rejects the request as permission denied (OTLP/gRPC
  `PERMISSION_DENIED`) and ingests no data

#### Scenario: Unscoped legacy key may ingest any signal

- **WHEN** an API key with no explicit scopes exports any supported signal
- **THEN** the acceptor accepts the export

### Requirement: Self-monitoring tenant isolation

The acceptor SHALL treat the reserved self-monitoring (`_system`) tenant's
own telemetry as non-recursive: processing `_system` ingest MUST NOT emit
further self-monitoring telemetry, and `_system` traffic MUST NOT be counted
in tenant-facing ingest metrics.

#### Scenario: Self-monitoring ingest does not feed back

- **WHEN** telemetry for the `_system` tenant is ingested
- **THEN** the acceptor suppresses self-telemetry generated while handling it
  and excludes it from tenant ingest counters
