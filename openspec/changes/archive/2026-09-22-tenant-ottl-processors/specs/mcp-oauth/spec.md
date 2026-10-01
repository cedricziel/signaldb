## MODIFIED Requirements

### Requirement: Granted scopes gate tool access

`processors:read` SHALL be a read scope: included in the default (no `scope`)
grant and grantable at consent. `processors:write` SHALL NOT be grantable
through OAuth.

#### Scenario: processors:write is rejected at authorization

- **WHEN** an OAuth client requests `processors:write`
- **THEN** the authorization request is rejected with `invalid_scope`
