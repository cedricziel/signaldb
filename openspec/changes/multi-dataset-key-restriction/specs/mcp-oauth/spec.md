## MODIFIED Requirements

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

## ADDED Requirements

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
