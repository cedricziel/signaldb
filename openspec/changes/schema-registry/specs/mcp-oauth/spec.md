## MODIFIED Requirements

### Requirement: Read-scope enforcement for query tools

Granted OAuth scopes SHALL populate the caller's enforced scope set. A read tool over a signal SHALL require the matching `<signal>:read` scope (`traces:read`, `logs:read`, `metrics:read`); a token lacking the required read scope SHALL be denied that tool with an authorization error. Schema-registry lookup tools SHALL require `schema:read`, which is a read scope: it is included when no `scope` is requested (the all-read default) and is grantable at consent. `schema:write` SHALL NOT be grantable through OAuth. The consent step SHALL show the scopes the client requested so the human grants them deliberately.

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
