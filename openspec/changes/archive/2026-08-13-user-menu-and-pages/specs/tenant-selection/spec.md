## Purpose

Allows authenticated users with multiple tenant memberships to switch between tenants and datasets through a dedicated selection page.

## ADDED Requirements

### Requirement: Display tenant membership list

The system SHALL display a list of all tenants the user is a member of, along with their role in each tenant.

#### Scenario: Multi-tenant user sees all memberships

- **WHEN** the user has memberships in multiple tenants
- **THEN** all tenants are listed with their respective roles

#### Scenario: Single-tenant user still sees their tenant

- **WHEN** the user belongs to only one tenant
- **THEN** that tenant is displayed in the list

### Requirement: Current tenant is visually distinguished

The currently active tenant SHALL be visually highlighted (e.g., accent border or background).

#### Scenario: Active tenant is highlighted

- **WHEN** the tenant selection page loads
- **THEN** the current tenant has a distinct visual style from other tenants

### Requirement: Tenant expansion reveals datasets

The system SHALL allow users to expand a tenant to see its available datasets. The current tenant SHALL be expanded by default. Other tenants SHALL be collapsed and fetch their datasets lazily on expansion.

#### Scenario: Current tenant datasets visible

- **WHEN** the page loads
- **THEN** the current tenant's datasets are displayed

#### Scenario: Other tenant fetches datasets on expand

- **WHEN** the user clicks to expand a non-current tenant
- **THEN** the system fetches that tenant's datasets via whoami(tenant_id)

### Requirement: Dataset selection navigates to explore view

The system SHALL navigate to the explore view when a user selects a dataset, updating the tenant and dataset state.

#### Scenario: Select dataset navigates

- **WHEN** the user clicks on a dataset
- **THEN** the app navigates to /logs with the selected tenant and dataset

#### Scenario: State update propagates to URL

- **WHEN** the user selects a dataset
- **THEN** the URL reflects the new tenant and dataset parameters
