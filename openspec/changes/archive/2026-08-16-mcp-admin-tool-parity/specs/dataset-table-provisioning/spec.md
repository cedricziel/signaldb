## ADDED Requirements

### Requirement: On-demand provisioning is reachable from every client surface

The manual provisioning trigger and the read of a tenant's signal tables SHALL
be part of the API contract (declared in the OpenAPI document) and therefore
reachable through the SDK, the CLI (`tenant table list` / `tenant table
provision`), the MCP server (`tenant_list_tables` / `tenant_create_tables`), and
the UI's management area, each consuming the generated client rather than raw
HTTP. Provisioning through any surface SHALL behave identically: it creates the
tenant's enabled signal tables for the named (or default) dataset before
reporting success.

#### Scenario: Tables are listed through the SDK

- **WHEN** a caller lists a tenant's tables through the CLI, an MCP tool, or the
  UI management area
- **THEN** the result is sourced through the generated client and names each
  dataset's provisioned signal tables once the router's table listing is
  implemented (today the endpoint is a placeholder that answers an empty list;
  provisioning itself is verified against the catalog)

#### Scenario: Provisioning through the UI

- **WHEN** a user with management rights triggers "provision tables" for a
  dataset in the UI
- **THEN** the dataset's enabled signal tables exist when the action reports
  success, and the table list refreshes to show them
