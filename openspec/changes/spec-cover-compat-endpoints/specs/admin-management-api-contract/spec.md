## MODIFIED Requirements

### Requirement: Document faithfully describes served endpoints

The published document SHALL faithfully describe the endpoints the router
actually serves — there SHALL be no drift between the spec and the
implementation. Every documented operation SHALL correspond to a served route,
and the documented request/response schemas SHALL match the wire format the
handlers produce and accept.

#### Scenario: Handler or DTO changes without spec regeneration

- **WHEN** the API's handlers or DTOs change without the published spec being
  regenerated
- **THEN** the project's checks fail, requiring the spec to be brought back in
  sync before the change can merge

#### Scenario: Undeclared error response

- **WHEN** an operation can return an error the handler actually emits — for
  example a quota rejection on admin key/dataset creation, or an internal
  error on a management operation
- **THEN** that response status is declared in the operation's contract, not
  only the success response

#### Scenario: Authentication requirement is declared accurately

- **WHEN** any admin or management operation is inspected in the document
- **THEN** it declares that authentication is required, satisfied by either
  the `bearerAuth` scheme or the `cookieAuth` scheme (see
  `ui-session-auth-contract`) — matching that the auth middleware accepts the
  UI's session cookie for these operations exactly as it does for every other
  authenticated route, rather than describing bearer as the only option
