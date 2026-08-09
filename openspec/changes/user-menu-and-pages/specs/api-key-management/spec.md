## Purpose

Provides tenant administrators with a self-service page to create, list, and revoke API keys for telemetry ingestion with scoped permissions.

## ADDED Requirements

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

The system SHALL allow admins to create API keys with a name, optional dataset scope, and selected ingestion permissions (metrics:write, logs:write, traces:write, profiles:write).

#### Scenario: Create key with name and scopes

- **WHEN** the admin fills the form and clicks "Create API key"
- **THEN** a new key is created and the secret is displayed in a modal

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
