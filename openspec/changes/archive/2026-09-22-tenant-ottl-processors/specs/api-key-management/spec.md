## MODIFIED Requirements

### Requirement: One scope vocabulary on every key-management surface

The scope vocabulary SHALL additionally contain `processors:read` and
`processors:write`. The UI scope picker SHALL list them in the same group as
the schema scopes; the admin API, management API, SDK, CLI, and MCP SHALL
accept them on key creation and scope update.

#### Scenario: Create key with processor scopes

- **WHEN** an admin creates a key with `scopes: ["processors:read",
  "processors:write"]`
- **THEN** the key is created and a request using it can list and create
  processors
