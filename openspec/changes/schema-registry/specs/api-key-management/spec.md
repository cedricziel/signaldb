## MODIFIED Requirements

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
