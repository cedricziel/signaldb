## MODIFIED Requirements

### Requirement: SDK covers the full API surface

`signaldb-sdk` SHALL expose every capability the SignalDB API offers, spanning
its HTTP surface (admin/management, tenant self-management including tables
and schemas, operational control, and the PromQL/LogQL/TraceQL/Pyroscope
query-compat endpoints including trace-to-profile correlation) and its Arrow
Flight surface (SQL query). A capability reachable through the API but absent
from the SDK is a defect. Every HTTP endpoint the router serves to tenants or
administrators SHALL be declared in the OpenAPI document, because that document
is the SDK's only source; an endpoint outside it is invisible to every client
and is therefore a parity defect. The route drift guard's allowlist SHALL name
only routes that are genuinely not part of the client contract (health, the
OpenAPI document itself, browser session and OAuth flows, static UI); a
compat query route SHALL NOT be allowlisted.

#### Scenario: A new API capability lacks SDK coverage

- **WHEN** the API exposes an operation that has no corresponding `signaldb-sdk`
  method
- **THEN** the parity check fails and identifies the uncovered operation

#### Scenario: A router endpoint is missing from the OpenAPI document

- **WHEN** the router registers a tenant- or admin-facing HTTP route that has no
  OpenAPI operation
- **THEN** the OpenAPI drift check fails and names the route

#### Scenario: Pyroscope compat endpoints are in the contract

- **WHEN** the OpenAPI document is inspected
- **THEN** it declares operations for `/pyroscope/render`, `/render-diff`,
  `/label-names`, `/label-values`, `/profile-types`, and
  `/api/profiles/trace/{trace_id}`, and the generated Rust SDK and TypeScript
  client expose them
