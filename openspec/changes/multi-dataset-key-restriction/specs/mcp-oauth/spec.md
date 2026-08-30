## ADDED Requirements

### Requirement: Consent-time dataset selection within the chosen tenant

Once the resource owner selects a tenant at consent, the authorization
endpoint SHALL present that tenant's datasets as a multi-select so the user
may optionally restrict the grant to a subset of them, defaulting to no
dataset selected (unrestricted — every dataset in the tenant). On approval,
the issued authorization code SHALL be bound to the chosen dataset set (or
"unrestricted" when none was selected) alongside the tenant, scopes, client,
redirect URI, PKCE challenge, and resource, exactly as tenant is bound
today.

#### Scenario: User restricts a grant to two datasets

- **WHEN** an authenticated user selects a tenant, checks two of its
  datasets, and approves
- **THEN** the issued authorization code is bound to exactly those two
  datasets alongside the tenant and scopes

#### Scenario: Leaving every dataset unchecked grants the whole tenant

- **WHEN** an authenticated user selects a tenant, leaves every dataset
  unchecked, and approves
- **THEN** the issued authorization code carries no dataset restriction and
  the resulting token reaches every dataset in the tenant — identical to
  this authorization server's behavior before this requirement existed

#### Scenario: A dataset outside the chosen tenant cannot be selected

- **WHEN** a consent decision names a dataset that does not belong to the
  selected tenant
- **THEN** the server rejects the decision and issues no authorization code

## MODIFIED Requirements

### Requirement: Token issuance with PKCE and refresh

The token endpoint SHALL exchange a valid, unexpired, single-use authorization
code for an access token and a refresh token only when the presented PKCE
`code_verifier` matches the code's stored challenge and the `redirect_uri`
and `client_id` match those the code was issued to. The issued tokens SHALL
carry the same dataset restriction (or lack of one) that was bound to the
authorization code. It SHALL honor the `refresh_token` grant to mint a new
access token carrying the same tenant, scopes, dataset restriction, and
audience. It SHALL reject a reused, expired, or mismatched code.

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

#### Scenario: Refresh mints an access token with the same grant

- **WHEN** a client presents a valid refresh token
- **THEN** the server returns a new access token bound to the same tenant,
  scopes, dataset restriction, and audience as the original grant

#### Scenario: A consumed authorization code cannot be reused

- **WHEN** an authorization code that was already redeemed is presented
  again
- **THEN** the server returns an `invalid_grant` error and issues no token

### Requirement: Tenant is bound to the token and absent from the agent surface

The authorization server SHALL resolve the tenant for an OAuth-authenticated
MCP request solely from the presented access token. The tenant SHALL NOT be
taken from an `X-Tenant-ID` header or any MCP tool argument for OAuth
sessions, and there SHALL be no request-controllable way to widen a token
beyond the single tenant it was granted, or — when the token carries a
dataset restriction — beyond that restriction's dataset set. Reaching a
second tenant, or a dataset outside a restricted token's set, requires a
separate authorization (a separate token or a re-consented one).

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

#### Scenario: An unrestricted token reaches every dataset in its tenant

- **WHEN** a client holding a token with no dataset restriction calls an MCP
  tool naming any dataset that exists in the token's tenant
- **THEN** the call succeeds — identical to this authorization server's
  behavior for every token issued before this requirement existed
