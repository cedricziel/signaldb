## Purpose

Defines the published contract for SignalDB's admin (`/api/v1/admin/*`) and
tenant-management (`/api/v1/manage/*`) HTTP APIs: an OpenAPI document derived
from the implementation, served by the router, guaranteed not to drift from the
code, and used as the integration surface for SignalDB's own clients (CLI and
web UI).

## ADDED Requirements

### Requirement: Published OpenAPI document for the admin & management API

SignalDB SHALL serve an OpenAPI document describing its admin and
tenant-management HTTP endpoints at `GET /api/v1/openapi.json`. The document
SHALL cover the admin surface (`/api/v1/admin/*`: tenants, API keys, datasets)
and the management surface (`/api/v1/manage/*`: tenants, datasets, API keys,
memberships), with each operation's method, path parameters, request body, and
response schemas.

#### Scenario: Spec is retrievable

- **WHEN** a client requests `GET /api/v1/openapi.json`
- **THEN** it receives a valid OpenAPI document that includes the
  `/api/v1/admin/*` and `/api/v1/manage/*` paths and their component schemas

#### Scenario: Management surface is documented

- **WHEN** the published document is inspected for the tenant-management surface
- **THEN** the membership, dataset, API-key, and tenant management operations
  under `/api/v1/manage/*` are present, not only the admin surface

### Requirement: The published spec matches the served API

The published document SHALL faithfully describe the endpoints the router
actually serves — there SHALL be no drift between the spec and the
implementation. Every documented operation SHALL correspond to a served route,
and the documented request/response schemas SHALL match the wire format the
handlers produce and accept.

#### Scenario: Spec is regenerated from the code

- **WHEN** the API's handlers or DTOs change without the published spec being
  regenerated
- **THEN** the project's checks fail, requiring the spec to be brought back in
  sync before the change can merge

#### Scenario: Documented responses include error and quota outcomes

- **WHEN** an operation can return an error the handler actually emits — for
  example a quota rejection on admin key/dataset creation, or an internal
  error on a management operation
- **THEN** that response status is declared in the operation's contract, not
  only the success response

#### Scenario: Authentication is documented

- **WHEN** any admin or management operation is inspected in the document
- **THEN** it declares that authentication is required (a bearer security
  scheme), rather than appearing unauthenticated

### Requirement: First-party clients consume the generated contract

SignalDB's own consumers SHALL interact with the admin and management APIs
through clients generated from the published OpenAPI document, rather than
hand-written HTTP calls. Regenerating the document SHALL regenerate those
clients, keeping consumers in lockstep with the contract.

#### Scenario: The web UI uses the generated client

- **WHEN** the web UI performs a tenant-management operation
- **THEN** it does so through the generated TypeScript client derived from the
  spec, issuing no raw HTTP request against `/api/v1/manage/*`

#### Scenario: The CLI uses the generated SDK

- **WHEN** the CLI performs an admin operation (e.g. listing or creating
  tenants)
- **THEN** it does so through the generated Rust SDK derived from the spec
