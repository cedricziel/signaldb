## Purpose

Defines how a request to any SignalDB entry point — router HTTP, OTLP/HTTP,
OTLP/gRPC, and inter-service Flight — is authenticated: which credential kinds
exist, how they are parsed and prioritised, which credentials are reserved for
the instance itself (self-monitoring, browser telemetry, the service mesh), and
how rejections are reported so clients can react correctly.

## ADDED Requirements

### Requirement: One credential-parsing contract for every entry point

Every entry point that accepts a bearer credential — the router's HTTP API,
the acceptor's OTLP/HTTP and Prometheus `remote_write` endpoints, the
acceptor's OTLP/gRPC endpoint, and the Flight services — SHALL parse the
`Authorization` header (or `authorization` gRPC metadata) by one shared
contract: the `Bearer` scheme is matched case-insensitively, surrounding
whitespace is tolerated, and an empty token, a non-Bearer scheme, or a
malformed header is rejected with the same classification on every entry
point (HTTP `401` with a generic message, gRPC/Flight `UNAUTHENTICATED`).
Tenant and dataset identifiers SHALL be validated by the same rules on every
entry point. A credential accepted by one entry point SHALL NOT be rejected by
another for reasons of formatting alone.

#### Scenario: Lower-case scheme is accepted on the router

- **WHEN** a client calls `/api/v1/whoami` with `Authorization: bearer <valid-key>`
  and a valid `X-Tenant-ID`
- **THEN** the router authenticates the request exactly as it would for
  `Bearer <valid-key>`

#### Scenario: Malformed header is classified identically everywhere

- **WHEN** a request presents `Authorization: Token abc` to the router, to
  OTLP/HTTP, to OTLP/gRPC, and to a Flight service
- **THEN** each rejects it as unauthenticated (HTTP `401`, gRPC/Flight
  `UNAUTHENTICATED`) with a message that names neither the scheme it saw nor
  the token

#### Scenario: Identifier validation does not differ by entry point

- **WHEN** a request names a tenant id containing `/` or longer than the
  documented maximum on any entry point
- **THEN** every entry point rejects it with the same classification
  (HTTP `400`, gRPC `INVALID_ARGUMENT`) before consulting the credential store

### Requirement: The router accepts three credential kinds with fixed precedence

On the router's authenticated routes the system SHALL accept exactly three
credential kinds: a tenant API key (bearer), an OAuth access token (bearer,
recognisable by its prefix), and a browser session cookie. A bearer
credential, when present, SHALL take precedence over a cookie. An API key or
session MUST be accompanied by `X-Tenant-ID`; an OAuth token carries its own
tenant and any `X-Tenant-ID` header SHALL be ignored. Requests presenting no
credential of any kind SHALL be rejected as unauthenticated. The OAuth
access-token prefix SHALL be reserved: an API key that begins with it SHALL be
refused when created through any surface and when loaded from configuration,
so that classifying a bearer value by its prefix is deterministic and never
misroutes a valid key. The resolved principal (tenant, dataset, scopes or
role, user identity when a human) SHALL be the only input to downstream
authorization, regardless of which credential kind produced it.

#### Scenario: Bearer wins over cookie

- **WHEN** a browser request carries both a valid session cookie for user U
  and `Authorization: Bearer <tenant-key>`
- **THEN** the request is authenticated as the API key's principal, not as U

#### Scenario: Session without tenant header is rejected

- **WHEN** a request carries a valid session cookie but no `X-Tenant-ID`
- **THEN** the router rejects it with `400` naming the missing tenant header

#### Scenario: OAuth token ignores the tenant header

- **WHEN** a request carries an OAuth access token bound to tenant A and
  `X-Tenant-ID: B`
- **THEN** the request is authenticated for tenant A and the header has no
  effect

#### Scenario: API key with the reserved OAuth prefix is refused

- **WHEN** an API key whose secret begins with the OAuth access-token prefix
  is submitted to key creation on any surface, or appears under
  `[[auth.tenants]]` in configuration
- **THEN** creation is rejected with a validation error naming the reserved
  prefix, and configuration load fails naming the tenant and key

### Requirement: Flight mesh authentication

When `[auth].internal_service_key` is configured, every Flight service SHALL
require each call to present either that key or a valid tenant API key as a
bearer credential; the writer and compactor SHALL accept only the internal key.
The internal-key comparison SHALL be constant-time. When the key is not
configured the Flight services SHALL accept calls that present no
authorization metadata, and every service process SHALL log a warning at
startup stating that the Flight ports are unauthenticated. A credential that
is supplied while the key is not configured SHALL still be parsed by the
shared contract — a malformed header is rejected with `UNAUTHENTICATED` — but
SHALL NOT be verified. The presence or absence of Flight authentication SHALL
NOT change the authentication behaviour of the HTTP or OTLP entry points.

#### Scenario: Tenant key cannot reach an internal-only service

- **WHEN** a caller presents a valid tenant API key to the writer's Flight port
  on an instance with `internal_service_key` set
- **THEN** the call is rejected with `UNAUTHENTICATED` and no data is written

#### Scenario: Internal key is accepted by every service

- **WHEN** a caller presents the configured `internal_service_key` to the
  querier, writer, or compactor Flight port
- **THEN** the call is authenticated as the internal service principal

#### Scenario: Missing mesh key is announced

- **WHEN** a service starts without `internal_service_key` configured
- **THEN** its startup log contains a warning that its Flight port accepts
  unauthenticated calls

#### Scenario: Malformed header is rejected even without a mesh key

- **WHEN** `internal_service_key` is not configured and a caller presents
  `authorization: Token abc` to a Flight port
- **THEN** the call is rejected with `UNAUTHENTICATED`, while a call with no
  authorization metadata is accepted

### Requirement: Self-monitoring authenticates with a dedicated credential

The self-monitoring tenant (`_system` by default) SHALL authenticate its own
telemetry with a dedicated API key configured under `[self_monitoring]`, or
generated and persisted at first boot when not configured. The instance
`admin_api_key` SHALL NOT be a valid tenant credential for the self-monitoring
tenant or for any other tenant; it authorizes only the admin and operations
APIs. Operators SHALL be able to rotate the self-monitoring key like any other
tenant key without touching `admin_api_key`. When no key is configured and
the catalog cannot persist a generated one, startup SHALL fail with an error
naming the missing credential rather than enable self-monitoring without a
durable key.

#### Scenario: Admin key is refused on tenant routes

- **WHEN** a client presents `admin_api_key` as a bearer with
  `X-Tenant-ID: _system` to the query API or to OTLP ingest
- **THEN** the request is rejected as unauthenticated

#### Scenario: Self-monitoring export uses its own key

- **WHEN** self-monitoring is enabled and the process exports its telemetry
- **THEN** the export authenticates with the `[self_monitoring]` key and is
  accepted for the `_system` tenant

#### Scenario: Key is generated when absent

- **WHEN** self-monitoring is enabled, no `[self_monitoring].api_key` is set,
  and the instance boots for the first time
- **THEN** a self-monitoring key is created, persisted in the catalog, and the
  startup log states that it was generated (without printing the secret
  beyond the first boot)

#### Scenario: Key cannot be persisted

- **WHEN** self-monitoring is enabled, no `[self_monitoring].api_key` is set,
  and the catalog is read-only or the key cannot be written
- **THEN** the process fails startup with an error naming
  `[self_monitoring].api_key`, and no telemetry export is started

### Requirement: The browser-published telemetry key is narrow

When browser telemetry is enabled the router publishes an ingest credential to
anonymous clients through the UI runtime configuration. That credential SHALL
carry only write scopes, SHALL be bound to the configured frontend tenant and
dataset, and SHALL NOT be the admin key, the self-monitoring key, or a key
with read, schema, or management scopes. The router SHALL validate this at
startup and refuse to serve the runtime configuration — logging the reason —
when the configured key does not satisfy these bounds. The runtime
configuration response SHALL be marked non-cacheable.

#### Scenario: Over-privileged frontend key is refused at startup

- **WHEN** `[self_monitoring.frontend].api_key` names a key that carries
  `traces:read`
- **THEN** the router fails startup validation with a message naming the
  offending scope, and no runtime configuration embedding that key is served

#### Scenario: Key bound elsewhere is refused at startup

- **WHEN** `[self_monitoring.frontend].api_key` names a write-only key that
  belongs to a tenant other than the configured frontend tenant, or is
  restricted to a dataset other than the configured frontend dataset
- **THEN** the router fails startup validation with a message naming the
  mismatched tenant or dataset, and no runtime configuration embedding that
  key is served

#### Scenario: Conforming key is published

- **WHEN** the frontend key carries only `traces:write` and `logs:write` for
  the frontend tenant/dataset
- **THEN** `/ui/runtime-config.js` embeds it with `Cache-Control: no-store`

### Requirement: Unauthenticated and forbidden are distinguishable

Authenticated routes SHALL answer `401` only when no acceptable credential was
presented or the presented credential is invalid, expired, or revoked, and
`403` when the credential is valid but lacks the scope, role, tenant, or
dataset the operation requires. The web UI SHALL treat `401` as a signal to
re-establish the session (login gate) and `403` as a forbidden state shown in
place, without discarding the current session.

#### Scenario: Scope denial is forbidden, not unauthenticated

- **WHEN** an ingest-only key queries `/api/v1/query`
- **THEN** the response is `403` and, if the call originated in the UI, the
  user sees a "not permitted" state and remains signed in

#### Scenario: Revoked session is unauthenticated

- **WHEN** a browser request carries a session cookie that was revoked
- **THEN** the response is `401` and the UI returns to the login gate
