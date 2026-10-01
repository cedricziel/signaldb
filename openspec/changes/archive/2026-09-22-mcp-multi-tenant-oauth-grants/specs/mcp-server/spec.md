## MODIFIED Requirements

### Requirement: Bearer authentication and credential forwarding

The MCP server SHALL hold no credential of its own and SHALL NOT validate credentials — the router is the sole authority on whether a credential is valid and what it may access. On each Streamable HTTP request it SHALL require the presence of a bearer token, and, for an API-key credential, an `X-Tenant-ID` header, rejecting a request that lacks either. An invalid, expired, or revoked credential is not rejected locally; it is rejected by the router and surfaces as a clean MCP tool error.

A session SHALL be bound to the credential seen on its first request; a later request on the same session presenting a different credential SHALL be rejected. For an API-key credential, or an OAuth credential whose grant covers exactly one tenant, the session is additionally bound to that one tenant, and a later request declaring a different tenant SHALL be rejected. For an OAuth credential whose grant covers more than one tenant, the session is bound to the credential only: later requests on the same session MAY each independently select any tenant from that credential's own granted set (see mcp-tool-surface's tenant-argument-as-selector behavior), and the server forwards the tenant that call selected as `X-Tenant-ID` to the router for that call, rather than forwarding an inbound header (none exists — OAuth requests carry no `X-Tenant-ID`).

#### Scenario: Missing credential is rejected at the MCP layer

- **WHEN** a client sends a request to `/mcp` without a bearer token, or without `X-Tenant-ID` while authenticating with an API key
- **THEN** the server returns 401 and the request never reaches the MCP transport

#### Scenario: Invalid credential is rejected by the router

- **WHEN** a session presents a bearer token that the router rejects as invalid or revoked
- **THEN** the tool call surfaces the router's rejection as a clean MCP error (the MCP server does not pre-validate)

#### Scenario: Session cannot switch credential mid-stream

- **WHEN** a session established with one credential sends a later request presenting a different credential
- **THEN** the request is rejected rather than served under either credential

#### Scenario: Session cannot switch identity mid-stream

- **WHEN** a session established for tenant A with an API key, or an OAuth credential whose grant covers only tenant A, sends a later request declaring tenant B
- **THEN** the request is rejected rather than served as either identity

#### Scenario: A multi-tenant OAuth session may select a different granted tenant per call

- **WHEN** a session established with an OAuth credential whose grant covers tenants A and B sends one request selecting tenant A and a later request on the same session selecting tenant B
- **THEN** both requests succeed, each scoped to the tenant it selected, because both tenants belong to the same credential's granted set

#### Scenario: Downstream calls are made as the caller

- **WHEN** an authenticated session invokes a tool that reads tenant data
- **THEN** the resulting request to the query API carries the caller's bearer token and an `X-Tenant-ID` identifying the tenant that call is scoped to — forwarded verbatim from the inbound request for an API-key credential, or derived from the tool call's own tenant selection for an OAuth credential — and the server adds no privilege of its own

#### Scenario: Cross-tenant access is denied

- **WHEN** a session authenticated for tenant A invokes a tool referencing data that belongs to tenant B, and tenant B is not in that credential's granted set
- **THEN** no tenant B data is returned, because the forwarded credential and `X-Tenant-ID` scope the query to a tenant the credential is actually granted
