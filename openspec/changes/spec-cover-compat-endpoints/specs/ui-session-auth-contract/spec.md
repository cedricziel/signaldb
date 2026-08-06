## Purpose

Defines the published OpenAPI contract for the embedded explore UI's
browser-facing authentication endpoints, and documents the session cookie
they issue as a security scheme every authenticated operation accepts.

## ADDED Requirements

### Requirement: UI session lifecycle endpoints are documented

The OpenAPI document SHALL describe `POST /ui/session` (create a session from
email/password credentials) and `DELETE /ui/session` (revoke the current
session), including their request and response schemas and the
`Set-Cookie` response behavior.

#### Scenario: Login operation appears in the document

- **WHEN** the published OpenAPI document is inspected for
  `POST /ui/session`
- **THEN** the operation is present with its credential request schema and a
  response schema describing the resolved tenant, dataset, and membership
  list

#### Scenario: Login and logout require no bearer/cookie credential

- **WHEN** the published document is inspected for the security requirements
  of `POST /ui/session` and `DELETE /ui/session`
- **THEN** neither operation declares `bearerAuth` or `cookieAuth` as
  required, matching that login is a credential exchange and logout accepts
  an optionally-absent cookie

### Requirement: whoami endpoint is documented

The OpenAPI document SHALL describe `GET /api/v1/whoami`, including its
response schema (authenticated user, tenant, datasets, and memberships) and
that it requires authentication like any other tenant-scoped operation.

#### Scenario: whoami appears in the document

- **WHEN** the published OpenAPI document is inspected for
  `GET /api/v1/whoami`
- **THEN** the operation is present with its response schema and inherits the
  document's default security requirement

### Requirement: Session cookie is documented as an alternative security scheme

The OpenAPI document SHALL declare a `cookieAuth` security scheme
(the `signaldb_session` cookie) and SHALL document it as an equally valid
alternative to `bearerAuth` for every operation that requires authentication,
matching the auth middleware's actual behavior of accepting either credential
uniformly.

#### Scenario: An authenticated operation accepts either credential

- **WHEN** the published document is inspected for the security requirement
  of an authenticated operation (e.g. `GET /api/v1/whoami`, or a
  `query-compat-api-contract` or admin/management operation)
- **THEN** the requirement is satisfied by `bearerAuth` OR `cookieAuth`, not
  `bearerAuth` alone

#### Scenario: Document accuracy matches middleware behavior

- **WHEN** a request authenticates via the `signaldb_session` cookie against
  any operation the document marks as requiring `bearerAuth`
- **THEN** the request succeeds, consistent with the document's declared
  `cookieAuth` alternative
