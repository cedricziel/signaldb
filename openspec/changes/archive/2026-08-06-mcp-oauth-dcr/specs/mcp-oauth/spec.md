## Purpose

Lets AI agents (Claude.ai, OpenAI/ChatGPT) register SignalDB's MCP endpoint as a remote connector with no human pre-registration, by making SignalDB an OAuth 2.1 Authorization Server and Resource Server. It authenticates a human, binds the resulting token to a single tenant and a set of read scopes chosen at consent, and validates that token on every MCP call — so tenant isolation is enforced by the credential itself and never expressed by the agent.

## ADDED Requirements

### Requirement: Protected resource advertises its authorization server

The MCP resource origin SHALL expose an OAuth 2.0 Protected Resource Metadata document (RFC 9728) at `/.well-known/oauth-protected-resource` that identifies the SignalDB authorization server, and SHALL answer any unauthenticated or invalidly-authenticated MCP request with `401 Unauthorized` carrying a `WWW-Authenticate: Bearer` challenge whose `resource_metadata` parameter is the URL of that document.

#### Scenario: Metadata document names the authorization server

- **WHEN** a client requests `/.well-known/oauth-protected-resource` from the MCP resource origin
- **THEN** the response is a JSON document whose `authorization_servers` array contains the SignalDB authorization-server issuer URL
- **AND** its `resource` value equals the MCP resource URL clients call

#### Scenario: Unauthenticated MCP request is challenged toward discovery

- **WHEN** an MCP request arrives with no bearer token
- **THEN** the response status is `401`
- **AND** the `WWW-Authenticate` header is `Bearer` with a `resource_metadata` parameter pointing at the protected-resource metadata document

### Requirement: Authorization server metadata discovery

The authorization server SHALL expose an OAuth 2.0 Authorization Server Metadata document (RFC 8414) at `/.well-known/oauth-authorization-server` advertising its registration, authorization, and token endpoints, the `authorization_code` and `refresh_token` grant types, and `S256` as a supported PKCE code-challenge method.

#### Scenario: Metadata advertises the endpoints and PKCE

- **WHEN** a client requests `/.well-known/oauth-authorization-server`
- **THEN** the document includes `registration_endpoint`, `authorization_endpoint`, and `token_endpoint` absolute URLs
- **AND** `code_challenge_methods_supported` contains `S256`
- **AND** `grant_types_supported` contains `authorization_code` and `refresh_token`

### Requirement: Dynamic client registration

The authorization server SHALL accept unauthenticated Dynamic Client Registration requests (RFC 7591) at its registration endpoint, persist the registered client, and return a unique `client_id`. It SHALL reject a registration whose requested `redirect_uris` are malformed or absent.

#### Scenario: A new client registers itself

- **WHEN** a client POSTs a registration request with one or more valid `redirect_uris`
- **THEN** the server persists a new client record and responds with a unique `client_id` and the accepted metadata

#### Scenario: Registration without a valid redirect URI is rejected

- **WHEN** a registration request omits `redirect_uris` or supplies a malformed URI
- **THEN** the server responds with an `invalid_redirect_uri` (or `invalid_client_metadata`) error and persists no client

### Requirement: Authorization with human login and consent-time tenant selection

The authorization endpoint SHALL require the resource owner to authenticate as a SignalDB user (reusing the existing login/session), SHALL present a consent step that lists the requesting client, the scopes requested, and the tenants the user may grant, and SHALL require PKCE — rejecting an authorization request that lacks a `code_challenge`. On approval it SHALL issue a single-use authorization code bound to the chosen tenant, the granted scopes, the client, the redirect URI, the PKCE challenge, and the requested resource (audience).

#### Scenario: User logs in, selects a tenant, and approves

- **WHEN** an authenticated user approves an authorization request for a registered client, selecting a tenant they are a member of
- **THEN** the server redirects to the client's registered `redirect_uri` with a single-use authorization `code`
- **AND** the code is bound to the selected tenant, the approved scopes, the client, the PKCE challenge, and the requested resource

#### Scenario: Authorization without PKCE is rejected

- **WHEN** an authorization request arrives without a `code_challenge`
- **THEN** the server does not issue a code and returns an `invalid_request` error

#### Scenario: A tenant the user cannot access is not offered

- **WHEN** a user reaches the consent step
- **THEN** only tenants for which the user holds a membership are selectable
- **AND** an authorization code can never be bound to a tenant the user is not a member of

### Requirement: Token issuance with PKCE and refresh

The token endpoint SHALL exchange a valid, unexpired, single-use authorization code for an access token and a refresh token only when the presented PKCE `code_verifier` matches the code's stored challenge and the `redirect_uri` and `client_id` match those the code was issued to. It SHALL honor the `refresh_token` grant to mint a new access token carrying the same tenant, scopes, and audience. It SHALL reject a reused, expired, or mismatched code.

#### Scenario: Code plus matching verifier yields tokens

- **WHEN** a client redeems an authorization code with a `code_verifier` that hashes to the code's stored `S256` challenge, matching `client_id` and `redirect_uri`
- **THEN** the server returns an access token and a refresh token
- **AND** marks the authorization code consumed so it cannot be redeemed again

#### Scenario: PKCE mismatch is rejected

- **WHEN** a code is redeemed with a `code_verifier` that does not match its stored challenge
- **THEN** the server returns an `invalid_grant` error and issues no token

#### Scenario: Refresh mints an access token with the same grant

- **WHEN** a client presents a valid refresh token
- **THEN** the server returns a new access token bound to the same tenant, scopes, and audience as the original grant

#### Scenario: A consumed authorization code cannot be reused

- **WHEN** an authorization code that was already redeemed is presented again
- **THEN** the server returns an `invalid_grant` error and issues no token

### Requirement: Opaque catalog-backed tokens

Access tokens, refresh tokens, and authorization codes SHALL be opaque high-entropy values whose hashes and metadata (subject user, tenant, scopes, audience, expiry) are stored in the catalog; the raw value SHALL NOT be reconstructable from stored state. Validating a token SHALL be a catalog lookup, and revoking a token SHALL remove its stored record so subsequent presentations fail. Expired tokens SHALL be rejected.

#### Scenario: A revoked token stops working

- **WHEN** an access token's stored record is revoked (deleted)
- **THEN** a subsequent MCP request bearing that token is rejected with `401`

#### Scenario: An expired token is rejected

- **WHEN** an access token is presented after its stored expiry
- **THEN** the request is rejected with `401` and treated as unauthenticated

### Requirement: Tenant is bound to the token and absent from the agent surface

The authorization server SHALL resolve the tenant for an OAuth-authenticated MCP request solely from the presented access token. The tenant SHALL NOT be taken from an `X-Tenant-ID` header or any MCP tool argument for OAuth sessions, and there SHALL be no request-controllable way to widen a token beyond the single tenant it was granted. Reaching a second tenant requires a separate authorization (a separate token).

#### Scenario: Requests act on the token's tenant regardless of headers

- **WHEN** an OAuth-authenticated MCP request carries an `X-Tenant-ID` header naming a different tenant than the token was granted
- **THEN** the request is served against the token's bound tenant and the header is ignored

#### Scenario: One token cannot reach a second tenant

- **WHEN** a client holding a token for tenant A attempts any operation intended for tenant B
- **THEN** the operation is scoped to tenant A and never returns tenant B's data

### Requirement: Audience-bound tokens

Access tokens SHALL be bound to the MCP resource they were requested for (RFC 8707 resource indicators). The resource server SHALL reject a token whose stored audience does not match the resource being called.

#### Scenario: A token for another resource is rejected

- **WHEN** a token whose recorded audience is a different resource is presented to the MCP resource
- **THEN** the request is rejected with `401` and no tool executes

### Requirement: Read-scope enforcement for query tools

Granted OAuth scopes SHALL populate the caller's enforced scope set. A read tool over a signal SHALL require the matching `<signal>:read` scope (`traces:read`, `logs:read`, `metrics:read`); a token lacking the required read scope SHALL be denied that tool with an authorization error. The consent step SHALL show the scopes the client requested so the human grants them deliberately.

#### Scenario: A token with the read scope may query that signal

- **WHEN** a token holding `traces:read` invokes a trace-read tool
- **THEN** the tool executes and returns results scoped to the token's tenant

#### Scenario: A token lacking the read scope is denied

- **WHEN** a token that does not hold `metrics:read` invokes a metrics-read tool
- **THEN** the tool is denied with an authorization error and returns no metrics data

### Requirement: API-key authentication remains available and unchanged

The existing non-OAuth path SHALL continue to work: a caller presenting `Authorization: Bearer <api-key>` together with `X-Tenant-ID` SHALL be authenticated and tenant-scoped exactly as before, independent of the OAuth flow. Introducing OAuth SHALL NOT change the behavior observed by existing API-key callers.

#### Scenario: A legacy API-key caller is unaffected

- **WHEN** a caller presents a valid API key and `X-Tenant-ID`
- **THEN** the request is authenticated and scoped to that tenant as it was before OAuth existed, with no OAuth flow required
