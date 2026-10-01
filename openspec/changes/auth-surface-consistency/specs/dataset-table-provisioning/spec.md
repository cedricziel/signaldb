## MODIFIED Requirements

### Requirement: On-demand provisioning is reachable from every client surface

The manual provisioning trigger and the read of a tenant's signal tables SHALL
be part of the API contract (declared in the OpenAPI document) and therefore
reachable through the SDK, the CLI (`tenant table list` / `tenant table
provision`), the MCP server (`tenant_list_tables` / `tenant_create_tables`), and
the UI's management area, each consuming the generated client rather than raw
HTTP. Provisioning through any surface SHALL behave identically: it creates the
tenant's enabled signal tables for the named (or default) dataset before
reporting success. Triggering provisioning SHALL require tenant-management
authority for the named tenant — an API key carrying `tenant:manage`, a
session whose membership role is admin, or an instance-admin session; any
other valid credential SHALL be refused with `403`. Listing tables SHALL be
available to any credential that may read the tenant. A credential restricted
to one dataset SHALL act only within it: its listing SHALL contain that
dataset alone, provisioning through it SHALL create tables only for that
dataset, and a request naming another dataset SHALL be refused with `403`;
full-tenant listing and provisioning SHALL require an unrestricted credential.
The table listing SHALL
report what actually exists in the tenant's catalog — every table in each of
the tenant's datasets, each entry naming its dataset and its signal type, with
every known dataset present even when it holds no tables yet — and SHALL NOT
report tables that have not been created; a tenant whose datasets hold no
tables yet lists its datasets with empty table lists, without error.

#### Scenario: Tables are listed through the SDK

- **WHEN** a caller lists a tenant's tables through the CLI, an MCP tool, or the
  UI management area
- **THEN** the result is sourced through the generated client and names each
  dataset's provisioned signal tables, grouped by dataset

#### Scenario: Listing reflects provisioning

- **WHEN** a tenant has two datasets, one provisioned and one not, and a caller
  lists its tables
- **THEN** the provisioned dataset's signal tables are listed under that
  dataset and the other dataset appears with an empty table list; after
  provisioning the second dataset, a new listing shows its tables too

#### Scenario: Provisioning through the UI

- **WHEN** a user with management rights triggers "provision tables" for a
  dataset in the UI
- **THEN** the dataset's enabled signal tables exist when the action reports
  success, and the table list refreshes to show them

#### Scenario: Ingest-only key cannot provision

- **WHEN** a key carrying only `traces:write` calls the provisioning trigger
  for its own tenant
- **THEN** the call is refused with `403` and no table is created

#### Scenario: Management key can provision

- **WHEN** an unrestricted key carrying `tenant:manage` calls the provisioning
  trigger for its own tenant
- **THEN** the tenant's enabled signal tables are created in every dataset and
  the call reports success

#### Scenario: Dataset-restricted key sees only its dataset

- **WHEN** a tenant has datasets `production` and `staging`, and a key
  restricted to `staging` lists the tenant's tables
- **THEN** the listing contains `staging` alone; `production` and its tables
  are absent

#### Scenario: Dataset-restricted key cannot provision another dataset

- **WHEN** a key carrying `tenant:manage` restricted to `staging` triggers
  provisioning naming `production`
- **THEN** the call is refused with `403` and no table is created; triggering
  it for `staging` (or without naming a dataset) provisions `staging` alone
