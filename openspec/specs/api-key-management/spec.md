# api-key-management Specification

## Purpose

Provides tenant administrators with a self-service page to create, list, and revoke API keys for telemetry ingestion with scoped permissions.

## Requirements

### Requirement: Admin-only access

The system SHALL restrict the API keys page to users with admin role on the current tenant or instance-level administrators.

#### Scenario: Admin user accesses the page

- **WHEN** the user is a tenant admin or instance admin
- **THEN** the API keys page renders normally

#### Scenario: Non-admin user is redirected

- **WHEN** the user is not an admin for the current tenant
- **THEN** the user is redirected to /logs

### Requirement: Display existing API keys

The system SHALL list all API keys for the current tenant, sorted by creation date descending. Revoked keys SHALL be displayed with diminished styling.

#### Scenario: Keys list loads

- **WHEN** the API keys page loads
- **THEN** all keys for the tenant are displayed with name, dataset, scopes, creation date, and status

#### Scenario: Revoked keys are visually distinct

- **WHEN** a key has been revoked
- **THEN** the key row shows diminished styling (opacity, strikethrough) and no revoke button

### Requirement: Create new API key with scopes

The system SHALL allow admins to create API keys with a name, optional dataset
scope, and selected permissions: ingestion scopes (metrics:write, logs:write,
traces:write, profiles:write) and schema-registry scopes (schema:read,
schema:write). The scope picker SHALL group ingestion and schema scopes
separately and describe what each grants.

#### Scenario: Create key with name and scopes

- **WHEN** the admin fills the form and clicks "Create API key"
- **THEN** a new key is created and the secret is displayed in a modal

#### Scenario: Create key with schema scopes

- **WHEN** the admin selects `schema:read` and/or `schema:write` alongside any
  ingestion scopes and creates the key
- **THEN** the key is created carrying exactly the selected scopes and the list
  shows them

#### Scenario: Secret shown once

- **WHEN** a key is created
- **THEN** the secret key value is displayed in a modal with a copy button and "Done" button

#### Scenario: Secret modal dismisses

- **WHEN** the admin clicks "Done" in the secret modal
- **THEN** the modal closes and cannot be reopened

### Requirement: Revoke API key

The system SHALL allow admins to revoke API keys. Revocation is immediate and irreversible.

#### Scenario: Revoke key removes access

- **WHEN** the admin clicks "Revoke" on an active key
- **THEN** the key is revoked and the list updates to show the revoked state

#### Scenario: Revoked key cannot be used

- **WHEN** a key has been revoked
- **THEN** API requests using that key SHALL be rejected (backend behavior, not UI)

### Requirement: Scopes are selectable on every key-management surface

Every surface that creates an API key — the management UI, the admin/management
HTTP API, the SDK, the CLI (`signaldb admin api-key create`), and the MCP admin
toolset — SHALL let the caller choose the key's scopes from the same vocabulary
(`metrics:write`, `logs:write`, `traces:write`, `profiles:write`,
`schema:read`, `schema:write`) and an optional dataset restriction. A creation
request naming an unknown scope SHALL be rejected. Listing keys on any surface
SHALL show each key's scopes. A creation request without scopes SHALL be
rejected on every surface — a key's permissions are always explicit; the
unrestricted legacy behaviour applies only to keys that predate scopes.

#### Scenario: CLI creates a key with scopes

- **WHEN** a user runs `signaldb admin api-key create acme --name ci
  --scope traces:write --scope schema:read`
- **THEN** the key is created carrying exactly those two scopes and the CLI
  prints them with the secret

#### Scenario: MCP admin tool creates a key with scopes

- **WHEN** an MCP client invokes the API-key creation tool with
  `scopes: ["schema:read"]`
- **THEN** the key is created with that scope and the tool result lists it

#### Scenario: Unknown scope is rejected everywhere

- **WHEN** a key is created via HTTP, CLI, or MCP with scope `schema:admin`
- **THEN** the request is rejected with a validation error naming the scope

#### Scenario: Scopes are required

- **WHEN** a key is created via HTTP, CLI, or MCP with no scopes
- **THEN** the request is rejected with a validation error stating that at
  least one scope is required

### Requirement: An existing key's scopes can be updated

Tenant admins SHALL be able to change the scopes (and dataset restriction) of an
existing, non-revoked API key on every key-management surface without rotating
the secret. The change SHALL take effect for subsequent requests made with that
key. Revoked keys SHALL NOT be updatable.

#### Scenario: Scope added to a live key

- **WHEN** an admin adds `schema:write` to a key that previously carried only
  `schema:read`
- **THEN** the next request with that key may create a custom registry, and the
  key list shows both scopes

#### Scenario: Scope removed from a live key

- **WHEN** an admin removes `traces:write` from a key
- **THEN** the next trace ingest with that key is rejected with an authorization
  error

#### Scenario: Revoked key cannot be updated

- **WHEN** an admin attempts to change the scopes of a revoked key
- **THEN** the request is rejected
