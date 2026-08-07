## Purpose

Defines the published OpenAPI contract for SignalDB's Loki-, Prometheus-, and
Pyroscope-compatible query and metadata endpoints, so it can be relied on to
generate faithful, typed clients the way the admin/management contract
already does.

## ADDED Requirements

### Requirement: Query-compat metadata endpoints are documented

The OpenAPI document SHALL describe every Loki-, Prometheus-, and
Pyroscope-compatible metadata endpoint the router serves: `GET
/loki/api/v1/labels`, `GET /loki/api/v1/label/{name}/values`, `GET
/loki/api/v1/series`, `GET /loki/api/v1/detected_fields`, `GET
/prometheus/api/v1/labels`, `GET
/prometheus/api/v1/label/{name}/values`, `GET
/prometheus/api/v1/label_stats`, `GET /pyroscope/render`, `GET
/pyroscope/render-diff`, `GET /pyroscope/label-names`, `GET
/pyroscope/label-values`, `GET /pyroscope/profile-types`, and `GET
/api/profiles/trace/{trace_id}` — with each operation's method,
path, query parameters, and response schema.

#### Scenario: Metadata endpoint appears in the document

- **WHEN** the published OpenAPI document is inspected for
  `GET /loki/api/v1/labels`
- **THEN** the operation is present with its `start`/`end` query parameters
  and response schema documented

#### Scenario: Pyroscope surface appears in the document

- **WHEN** the published OpenAPI document is inspected for the Pyroscope
  endpoints
- **THEN** `render`, `render-diff`, `label-names`, `label-values`,
  `profile-types`, and `profiles-by-trace` are all present, not only a subset

### Requirement: Query-compat response bodies use real, typed schemas

Every Loki-, Prometheus-, and Pyroscope-compatible operation the router
serves — including `logql::query`, `logql::query_range`, `promql::query`, and
`promql::query_range`, all of which predate this capability and all of which
currently declare an untyped `serde_json::Value` body — SHALL declare its
actual response DTO as the response schema, unconditionally. An operation
SHALL NOT declare an untyped `serde_json::Value`/arbitrary-JSON schema when a
typed response DTO exists for what the handler returns.

#### Scenario: Loki instant query has a typed response

- **WHEN** the published document is inspected for `GET /loki/api/v1/query`
- **THEN** the response schema is the Loki query-response DTO's generated
  schema, not an untyped/arbitrary-object schema

#### Scenario: Prometheus range query has a typed response

- **WHEN** the published document is inspected for
  `GET /prometheus/api/v1/query_range`
- **THEN** the response schema is the Prometheus query-response DTO's
  generated schema, not an untyped/arbitrary-object schema

#### Scenario: A generated client reflects the real shape

- **WHEN** a TypeScript or Rust client is generated from the document for any
  query-compat operation
- **THEN** the generated return type reflects the operation's actual fields,
  not an opaque `unknown`/`any`/generic-JSON type

### Requirement: Query-compat document stays in sync with implementation

The published document SHALL faithfully describe the query-compat endpoints
the router actually serves, with no drift between the spec and the
implementation.

#### Scenario: Handler changes without spec regeneration

- **WHEN** a query-compat handler's route, parameters, or response DTO changes
  without the published spec being regenerated
- **THEN** the project's golden-file check fails, requiring the spec to be
  brought back in sync before the change can merge
