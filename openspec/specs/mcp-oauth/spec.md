# mcp-oauth Specification

## Purpose

Lets AI agents (Claude.ai, OpenAI/ChatGPT) register SignalDB's MCP endpoint as a remote connector with no human pre-registration, by making SignalDB an OAuth 2.1 Authorization Server and Resource Server. It authenticates a human, binds the resulting token to a single tenant and a set of read scopes chosen at consent, and validates that token on every MCP call — so tenant isolation is enforced by the credential itself and never expressed by the agent.

## Requirements

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

### Requirement: Authorization with human login and consent-time tenant and dataset selection

The authorization endpoint SHALL require the resource owner to authenticate as a SignalDB user (reusing the existing login/session), SHALL present a consent step that lists the requesting client, the scopes requested, and the tenants the user may grant, and SHALL require PKCE — rejecting an authorization request that lacks a `code_challenge`. Once a tenant is selected, the consent step SHALL present that tenant's datasets as an explicit choice between "all datasets" (the default, and the only option before this requirement existed) and "only these datasets," the latter revealing a checklist that MUST have at least one dataset checked to be submittable. On approval it SHALL issue a single-use authorization code bound to the chosen tenant, the granted scopes, the chosen dataset set (or no restriction, when "all datasets" was chosen), the client, the redirect URI, the PKCE challenge, and the requested resource (audience). A submitted dataset set that is empty, or that names a dataset outside the chosen tenant, SHALL be rejected without issuing a code.

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

#### Scenario: User restricts a grant to two datasets

- **WHEN** an authenticated user selects a tenant, chooses "only these datasets," checks two of them, and approves
- **THEN** the issued authorization code is bound to exactly those two datasets alongside the tenant and scopes

#### Scenario: Choosing "all datasets" grants the whole tenant

- **WHEN** an authenticated user selects a tenant, leaves the default "all datasets" choice selected, and approves
- **THEN** the issued authorization code carries no dataset restriction and the resulting token reaches every dataset in the tenant — identical to this authorization server's behavior before this requirement existed

#### Scenario: An empty dataset selection cannot be submitted

- **WHEN** a user chooses "only these datasets" and checks none of them
- **THEN** the consent form cannot be submitted, and a decision request that nonetheless carries an empty `dataset_ids` array is rejected server-side without issuing a code

#### Scenario: A dataset outside the chosen tenant cannot be selected

- **WHEN** a consent decision names a dataset that does not belong to the
  selected tenant
- **THEN** the server rejects the decision and issues no authorization code

#### Scenario: A pre-existing client's consent decision omits the dataset field

- **WHEN** a consent decision is submitted without a `dataset_ids` field at all (a client built before this requirement existed)
- **THEN** the server accepts it and issues a code with no dataset restriction, exactly as it would have before this requirement existed

### Requirement: Token issuance with PKCE and refresh

The token endpoint SHALL exchange a valid, unexpired, single-use authorization
code for an access token and a refresh token only when the presented PKCE
`code_verifier` matches the code's stored challenge and the `redirect_uri`
and `client_id` match those the code was issued to. The issued tokens SHALL
carry the same dataset restriction (or lack of one) that was bound to the
authorization code. It SHALL honor the `refresh_token` grant to mint a new
access token carrying the same tenant, scopes, and audience as the original
grant, and the same dataset restriction as is currently stored on the
presented refresh token — read from that refresh token's own record, not
copied from any access token, since a refresh request guarantees only the
refresh token's own validity. It SHALL reject a reused, expired, or
mismatched code.

#### Scenario: Code plus matching verifier yields tokens

- **WHEN** a client redeems an authorization code with a `code_verifier` that
  hashes to the code's stored `S256` challenge, matching `client_id` and
  `redirect_uri`
- **THEN** the server returns an access token and a refresh token
- **AND** marks the authorization code consumed so it cannot be redeemed
  again

#### Scenario: Issued tokens carry the code's dataset restriction

- **WHEN** a client redeems an authorization code that was bound to a
  two-dataset restriction
- **THEN** the returned access token and refresh token are both bound to
  that same restriction

#### Scenario: PKCE mismatch is rejected

- **WHEN** a code is redeemed with a `code_verifier` that does not match its
  stored challenge
- **THEN** the server returns an `invalid_grant` error and issues no token

#### Scenario: Refresh mints an access token with the stored dataset restriction

- **WHEN** a client presents a valid refresh token whose record carries a
  dataset restriction, and the access token originally issued alongside it
  is no longer available
- **THEN** the server returns a new access token bound to the same tenant,
  scopes, and audience as the original grant, and to the dataset restriction
  read from the presented refresh token's own record

#### Scenario: A consumed authorization code cannot be reused

- **WHEN** an authorization code that was already redeemed is presented
  again
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

The authorization server SHALL resolve the tenant for **every** request
authenticated by an OAuth access token solely from the presented token —
this applies uniformly regardless of which surface the request arrives
through: the MCP tool interface, or a direct HTTP call against the
Tempo/Loki/Prometheus/Pyroscope-compatible query endpoints using the same
bearer token (both go through the same `Authenticator`, per `design.md`
D3, so this is one enforcement point, not one per surface). The tenant
SHALL NOT be taken from an `X-Tenant-ID` header or any MCP tool argument
for OAuth sessions, and there SHALL be no request-controllable way to
widen a token beyond the single tenant it was granted, or — when the token
carries a dataset restriction — beyond that restriction's dataset set,
on any surface. Reaching a second tenant, or a dataset outside a
restricted token's set, requires a separate authorization (a separate
token or a re-consented one).

#### Scenario: Requests act on the token's tenant regardless of headers

- **WHEN** an OAuth-authenticated MCP request carries an `X-Tenant-ID` header
  naming a different tenant than the token was granted
- **THEN** the request is served against the token's bound tenant and the
  header is ignored

#### Scenario: One token cannot reach a second tenant

- **WHEN** a client holding a token for tenant A attempts any operation
  intended for tenant B
- **THEN** the operation is scoped to tenant A and never returns tenant B's
  data

#### Scenario: A dataset-restricted token cannot reach an unlisted dataset

- **WHEN** a client holding a token restricted to `["production"]` calls an
  MCP tool with `dataset: "staging"`
- **THEN** the call is refused with an authorization error and no data from
  `staging` is returned

#### Scenario: A dataset-restricted token with no explicit dataset is rejected, not defaulted

- **WHEN** a client holding a token restricted to `["production",
  "staging"]` calls an MCP tool with no `dataset` argument
- **THEN** the call is refused with an error asking for an explicit
  `dataset`, rather than silently resolving to the tenant's default dataset

#### Scenario: An unrestricted token reaches every dataset in its tenant

- **WHEN** a client holding a token with no dataset restriction calls an MCP
  tool naming any dataset that exists in the token's tenant
- **THEN** the call succeeds — identical to this authorization server's
  behavior for every token issued before this requirement existed

#### Scenario: Dataset restriction is enforced on direct HTTP calls, not only MCP tools

- **WHEN** a client holding a token restricted to `["production"]` presents
  it as a bearer token directly against a Tempo/Loki/Prometheus-compatible
  HTTP endpoint with `X-Dataset-ID: staging`
- **THEN** the call is refused with an authorization error, exactly as the
  equivalent MCP tool call would be — the enforcement point is the shared
  `Authenticator`, not an MCP-specific check

### Requirement: Audience-bound tokens

Access tokens SHALL be bound to the MCP resource they were requested for (RFC 8707 resource indicators). The resource server SHALL reject a token whose stored audience does not match the resource being called.

#### Scenario: A token for another resource is rejected

- **WHEN** a token whose recorded audience is a different resource is presented to the MCP resource
- **THEN** the request is rejected with `401` and no tool executes

### Requirement: Read-scope enforcement for query tools

Granted OAuth scopes SHALL populate the caller's enforced scope set. A read tool over a signal SHALL require the matching `<signal>:read` scope (`traces:read`, `logs:read`, `metrics:read`); a token lacking the required read scope SHALL be denied that tool with an authorization error. Schema-registry lookup tools SHALL require `schema:read`, which is a read scope: it is included when no `scope` is requested (the all-read default) and is grantable at consent. `schema:write` SHALL NOT be grantable through OAuth. The consent step SHALL show the scopes the client requested so the human grants them deliberately.

`processors:read` SHALL be a read scope: included in the default (no `scope`)
grant and grantable at consent. `processors:write` SHALL NOT be grantable
through OAuth.

#### Scenario: A token with the read scope may query that signal

- **WHEN** a token holding `traces:read` invokes a trace-read tool
- **THEN** the tool executes and returns results scoped to the token's tenant

#### Scenario: A token lacking the read scope is denied

- **WHEN** a token that does not hold `metrics:read` invokes a metrics-read tool
- **THEN** the tool is denied with an authorization error and returns no metrics data

#### Scenario: Schema lookup requires schema:read

- **WHEN** a token that does not hold `schema:read` invokes a schema-registry lookup tool
- **THEN** the tool is denied with an authorization error; a token issued with the default (no `scope`) grant holds `schema:read` and succeeds

#### Scenario: schema:write is rejected at authorization

- **WHEN** a client requests only `schema:write`
- **THEN** the authorization request is rejected with `invalid_scope`

#### Scenario: processors:write is rejected at authorization

- **WHEN** an OAuth client requests `processors:write`
- **THEN** the authorization request is rejected with `invalid_scope`

### Requirement: API-key authentication remains available and unchanged

The existing non-OAuth path SHALL continue to work: a caller presenting `Authorization: Bearer <api-key>` together with `X-Tenant-ID` SHALL be authenticated and tenant-scoped exactly as before, independent of the OAuth flow. Introducing OAuth SHALL NOT change the behavior observed by existing API-key callers.

#### Scenario: A legacy API-key caller is unaffected

- **WHEN** a caller presents a valid API key and `X-Tenant-ID`
- **THEN** the request is authenticated and scoped to that tenant as it was before OAuth existed, with no OAuth flow required

### Requirement: A dataset-restricted OAuth session cannot use the management API

An OAuth-authenticated session whose token carries a non-empty dataset
restriction SHALL be refused for every management-API operation
(`/api/v1/manage/*`), regardless of the authenticated user's tenant role.
`tenant:manage` itself is never grantable through OAuth consent, but a
human session authenticated via OAuth and holding the tenant-admin role (or
instance-admin flag) reaches the management API through that role rather
than a scope, and a dataset restriction on the token narrows what that
session may do with data-plane requests without narrowing what the
role-based management check would otherwise allow — so the restriction
must also gate that path directly, the same way it gates a
`tenant:manage`-scoped API key (see `api-key-management`'s "A tenant:manage
scope grants tenant self-management to API keys").

#### Scenario: A dataset-restricted tenant-admin session is refused management access

- **WHEN** a user who is a tenant admin for `acme` authorizes a connector
  restricted to `dataset_ids: ["production"]`, and that connector calls a
  management-API operation such as creating another API key or deleting a
  dataset
- **THEN** the operation is refused with `403`, even though the same user's
  browser session (unrestricted) could perform it

#### Scenario: An unrestricted OAuth session with the tenant-admin role is unaffected

- **WHEN** a user who is a tenant admin authorizes a connector with "all
  datasets" selected, and that connector calls a management-API operation
- **THEN** the operation succeeds exactly as it does today

### Requirement: A restricted grant is gated behind a rollout-complete flag

A consent decision naming a non-empty `dataset_ids` set SHALL be rejected
unless the `[auth].dataset_restriction_rollout_complete` config key is
`true`. This is stricter than the equivalent API-key gate: OAuth tokens
have no legacy dataset column at all (see `design.md` D2), so no old
binary has ever enforced a token's dataset restriction — even a
single-dataset OAuth restriction is unsafe until every node authenticating
OAuth tokens is running code that enforces `dataset_ids`. Choosing "all
datasets" is unaffected by this flag in either state.

#### Scenario: A restricted consent decision is refused before rollout is confirmed complete

- **WHEN** `[auth].dataset_restriction_rollout_complete` is `false` (the
  default) and a consent decision selects "only these datasets" naming one
  or more datasets
- **THEN** the decision is rejected with an error naming the config key,
  and no authorization code is issued

#### Scenario: Choosing all datasets is unaffected by the flag

- **WHEN** `[auth].dataset_restriction_rollout_complete` is `false` and a
  consent decision selects "all datasets"
- **THEN** the decision succeeds exactly as it would with the flag `true`

#### Scenario: A restricted consent decision succeeds once rollout is confirmed complete

- **WHEN** an operator sets `[auth].dataset_restriction_rollout_complete`
  to `true` and a consent decision selects "only these datasets" naming
  one or more datasets
- **THEN** the decision succeeds and the issued authorization code carries
  that restriction
