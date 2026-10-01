## MODIFIED Requirements

### Requirement: Authorization with human login and consent-time tenant selection

The authorization endpoint SHALL require the resource owner to authenticate as a SignalDB user (reusing the existing login/session), SHALL present a consent step that lists the requesting client, the scopes requested, and the tenants the user may grant as a multi-select choice, and SHALL require PKCE — rejecting an authorization request that lacks a `code_challenge`. On approval it SHALL issue a single-use authorization code bound to the chosen set of one or more tenants (each with its own independent all-datasets/some-datasets choice), the granted scopes (which apply across every tenant in the set), the client, the redirect URI, the PKCE challenge, and the requested resource (audience).

#### Scenario: User logs in, selects a tenant, and approves

- **WHEN** an authenticated user approves an authorization request for a registered client, selecting exactly one tenant they are a member of
- **THEN** the server redirects to the client's registered `redirect_uri` with a single-use authorization `code`
- **AND** the code is bound to a one-tenant grant set, the approved scopes, the client, the PKCE challenge, and the requested resource

#### Scenario: User selects multiple tenants and approves

- **WHEN** an authenticated user approves an authorization request, checking two or more tenants they are a member of, optionally restricting datasets independently for each
- **THEN** the server redirects with a single-use authorization `code`
- **AND** the code is bound to a grant set containing every selected tenant, each with its own dataset restriction (or none)

#### Scenario: Authorization without PKCE is rejected

- **WHEN** an authorization request arrives without a `code_challenge`
- **THEN** the server does not issue a code and returns an `invalid_request` error

#### Scenario: A tenant the user cannot access is not offered

- **WHEN** a user reaches the consent step
- **THEN** only tenants for which the user holds a membership are selectable
- **AND** an authorization code can never be bound to a tenant the user is not a member of

#### Scenario: At least one tenant must be selected

- **WHEN** a user attempts to approve an authorization request with no tenant checked
- **THEN** the server rejects the approval and issues no code

### Requirement: Token issuance with PKCE and refresh

The token endpoint SHALL exchange a valid, unexpired, single-use authorization code for an access token and a refresh token only when the presented PKCE `code_verifier` matches the code's stored challenge and the `redirect_uri` and `client_id` match those the code was issued to. It SHALL honor the `refresh_token` grant to mint a new access token carrying the same grant set (every tenant, each tenant's dataset restriction), scopes, and audience as the token being refreshed. It SHALL reject a reused, expired, or mismatched code.

#### Scenario: Code plus matching verifier yields tokens

- **WHEN** a client redeems an authorization code with a `code_verifier` that hashes to the code's stored `S256` challenge, matching `client_id` and `redirect_uri`
- **THEN** the server returns an access token and a refresh token, both bound to the code's full grant set
- **AND** marks the authorization code consumed so it cannot be redeemed again

#### Scenario: PKCE mismatch is rejected

- **WHEN** a code is redeemed with a `code_verifier` that does not match its stored challenge
- **THEN** the server returns an `invalid_grant` error and issues no token

#### Scenario: Refresh mints an access token with the same grant

- **WHEN** a client presents a valid refresh token that was issued for a grant set of two tenants
- **THEN** the server returns a new access token bound to the same two tenants, their respective dataset restrictions, the same scopes, and the same audience as the original grant

#### Scenario: A consumed authorization code cannot be reused

- **WHEN** an authorization code that was already redeemed is presented again
- **THEN** the server returns an `invalid_grant` error and issues no token

### Requirement: Tenant resolution is bound to the token's grant set and absent from the agent surface

The authorization server SHALL resolve the tenant(s) reachable by an OAuth-authenticated MCP request solely from the presented access token's stored grant set. There SHALL be no request-controllable way to widen a token beyond the tenants it was granted. Reaching a tenant outside the grant set requires a separate authorization (a new or re-consented token).

When a token's grant set contains exactly one tenant, resolution SHALL be implicit and unchanged from before this capability: no tenant selector is required on the request, and the request is served against that one tenant regardless of any `X-Tenant-ID` header, which SHALL be ignored.

When a token's grant set contains more than one tenant, every tenant-scoped request — including the identity-lookup endpoint (`whoami`) — SHALL carry an explicit tenant selector (the `X-Tenant-ID` header) naming one tenant from the grant set; a request with no selector SHALL be rejected rather than defaulting to any tenant, and a selector naming a tenant outside the grant set SHALL be rejected with an error naming the offending tenant. Each tenant's own dataset restriction (or lack of one) from the grant set applies once that tenant is selected. A grant entry naming a tenant that no longer exists SHALL fail to resolve the same way a selector outside the grant set does, without treating the grant as a whole any differently.

#### Scenario: A single-tenant grant needs no selector

- **WHEN** an OAuth-authenticated request carries a token whose grant set has exactly one tenant, with or without an `X-Tenant-ID` header
- **THEN** the request is served against that one tenant and any `X-Tenant-ID` header present is ignored

#### Scenario: A multi-tenant grant requires an explicit selector

- **WHEN** an OAuth-authenticated request carries a token whose grant set has two or more tenants and the request names no `X-Tenant-ID`
- **THEN** the request is rejected rather than resolved against any particular tenant

#### Scenario: A multi-tenant grant's selector is validated against the grant set

- **WHEN** an OAuth-authenticated request carries a token whose grant set is `{A, B}` and names `X-Tenant-ID: C`
- **THEN** the request is rejected with an error naming the offending tenant, and never reaches tenant A's or B's data

#### Scenario: A multi-tenant grant's selector picks the tenant and its own dataset restriction

- **WHEN** an OAuth-authenticated request carries a token whose grant set includes tenant B restricted to dataset `staging`, and names `X-Tenant-ID: B`
- **THEN** the request is served against tenant B, subject to the `staging`-only dataset restriction, independent of any other tenant's restriction in the same grant set

#### Scenario: whoami requires a selector for a multi-tenant grant like any other route

- **WHEN** an OAuth-authenticated `whoami` request carries a token whose grant set has two tenants and names no `X-Tenant-ID`
- **THEN** the request is rejected rather than resolved against any particular tenant, exactly as any other tenant-scoped route would be

#### Scenario: One token cannot reach a tenant outside its grant set

- **WHEN** a client holding a token whose grant set is `{A}` attempts any operation naming tenant B
- **THEN** the operation is rejected and never returns tenant B's data

#### Scenario: A grant entry naming a deleted tenant fails to resolve without affecting the rest of the grant

- **WHEN** a token's grant set is `{A, B}` and tenant A is deleted from the registry, then a request selects tenant A
- **THEN** the request is rejected because tenant A cannot be resolved
- **AND** a later request on the same token selecting tenant B succeeds unaffected

## ADDED Requirements

### Requirement: Token introspection for grant discovery

The authorization server SHALL expose a token-introspection endpoint (`POST /oauth/introspect`, RFC 7662-shaped) that, given a bearer token, reports whether it is active and, if so, the user it belongs to, its full tenant grant set (every granted tenant and each one's dataset restriction), its scopes, its audience, and its expiry — without resolving, requiring, or defaulting to any single tenant. This endpoint SHALL be implemented independently of the resource-API's per-request tenant-resolution path (`authenticate_oauth_token`/`TenantContext`), since its purpose is to answer "what can this token reach" before any one tenant has been selected.

#### Scenario: Introspecting an active multi-tenant token reports its full grant

- **WHEN** a caller posts a valid, unexpired access token to `/oauth/introspect`
- **THEN** the response reports `active: true` and lists every tenant in the token's grant set with each one's dataset restriction, without requiring an `X-Tenant-ID` header

#### Scenario: Introspecting an invalid or expired token reports inactive

- **WHEN** a caller posts a revoked, expired, or unrecognized token to `/oauth/introspect`
- **THEN** the response reports `active: false` and no tenant, scope, or expiry detail
