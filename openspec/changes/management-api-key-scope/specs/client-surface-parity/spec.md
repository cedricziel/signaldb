## MODIFIED Requirements

### Requirement: Query-surface parity is enforced

An automated check SHALL assert capability parity across the CLI and the MCP
server for **every** operation the SDK exposes — derived from the SDK's own
operation list, not a hand-maintained subset — against a reviewed exclusion
list of operations that are intentionally single-surface. That list SHALL be
limited to operations that are inherently tied to a browser or a signed-in
human: the OAuth 2.1 consent endpoints (session-cookie authenticated) and the
human self-serve tenant creation performed by an instance administrator
(API-key clients create tenants through the admin API instead). Every other
operation — including the whole tenant management API, which an API key with
`tenant:manage` may call — SHALL have both a CLI command and an MCP tool.
Query languages keep their specific rules: every language SHALL be reachable
through a CLI `query` flag; every language served over the router's **HTTP**
surface (TraceQL, LogQL, PromQL, Query IR) SHALL additionally be reachable
through an MCP tool; **SQL** is served over Arrow Flight (gRPC) and, because
the MCP server is an HTTP forwarder that holds no Flight client, SQL is
intentionally CLI-only, asserted explicitly. The check SHALL fail and name the
surface and operation whenever a non-excluded SDK operation lacks a CLI command
or an MCP tool, and SHALL fail when the exclusion list names an operation that
no longer exists.

#### Scenario: An HTTP query language is missing from the CLI or MCP

- **WHEN** a TraceQL/LogQL/PromQL/Query-IR capability lacks either a CLI `query`
  flag or an MCP tool
- **THEN** the parity check fails and names the missing surface

#### Scenario: SQL is CLI-only by design

- **WHEN** the parity check evaluates SQL
- **THEN** it requires a CLI `--sql` flag and requires that no MCP tool claims
  SQL, matching the HTTP-forwarder boundary

#### Scenario: A management operation lacks a surface

- **WHEN** the SDK gains an operation (for example a new tenant-management call)
  and either the CLI or the MCP server does not expose it and it is not on the
  exclusion list
- **THEN** the parity check fails and names the operation and the missing
  surface

#### Scenario: Management operations are not excluded

- **WHEN** the parity check evaluates the tenant management operations
  (datasets, API keys, memberships, schema, tenant self view)
- **THEN** each has a CLI command and an MCP tool; only the OAuth consent
  endpoints and human self-serve tenant creation are excluded

#### Scenario: Stale exclusion is rejected

- **WHEN** the exclusion list names an operation the SDK no longer has
- **THEN** the parity check fails so the list cannot silently rot

#### Scenario: All surfaces are aligned

- **WHEN** every non-excluded SDK operation has a CLI command and an MCP tool,
  every query language has its required CLI flag, every HTTP language has an
  MCP tool, and SQL remains CLI-only
- **THEN** the parity check passes
