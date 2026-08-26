## MODIFIED Requirements

### Requirement: The published spec matches the served API

The published document SHALL faithfully describe the endpoints the router
actually serves — there SHALL be no drift between the spec and the
implementation. Every documented operation SHALL correspond to a served route,
and the documented request/response schemas SHALL match the wire format the
handlers produce and accept. The document SHALL declare every security scheme
the router accepts — bearer API key, browser session cookie, and OAuth 2.1
access token — and each operation SHALL name the schemes that actually
authenticate it; an operation that is served without authentication SHALL be
marked as such rather than inheriting a default. Session management and the
OAuth authorization-server endpoints SHALL be part of the published contract.

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
- **THEN** it declares the security schemes that authenticate it: admin
  operations declare the bearer admin key and the instance-admin session;
  management operations declare the bearer tenant key, the session cookie,
  and — for read operations — the OAuth access token

#### Scenario: Public routes are marked public

- **WHEN** `/health`, the OpenAPI document itself, or the session-creation
  endpoint is inspected in the document
- **THEN** it carries an empty security requirement rather than the
  document-wide bearer default

#### Scenario: Session and OAuth endpoints are in the contract

- **WHEN** the document is inspected for `/ui/session`, the OAuth
  authorization-server metadata, `/oauth/register`, `/oauth/authorize`, and
  `/oauth/token`
- **THEN** each appears with its request and response schemas, and the
  generated SDK and TypeScript client expose them
