## Purpose

Lets a tenant admin connect SignalDB's GitHub App to the repos that produce their telemetry, so SignalDB can mint short-lived, read-only tokens for that tenant's repos without ever storing a long-lived GitHub credential.

## ADDED Requirements

### Requirement: Tenant-scoped installation linking

A tenant admin SHALL be able to link a GitHub App installation (produced by GitHub's install flow) to their tenant via the admin/management API, recording the installation id, the app account (org or user) it belongs to, and the set of repos the installation covers. Only users with admin role on the tenant, or instance-level administrators, SHALL be able to link, list, or remove an installation, matching the authorization model in `api-key-management`.

#### Scenario: Admin links a new installation

- **WHEN** a tenant admin submits a GitHub installation id received from GitHub's post-install redirect
- **THEN** SignalDB records the installation, its covered repos, and its link to the admin's tenant, and the installation appears in that tenant's installation list

#### Scenario: Non-admin cannot link

- **WHEN** a user without tenant-admin or instance-admin role attempts to link an installation
- **THEN** the request is rejected before any installation record is created

### Requirement: Read-only permission scope

SignalDB's GitHub App SHALL be configured with only `contents:read` and `metadata:read` permissions. The system SHALL NOT request, accept, or use any write, pull-request, issues, or Actions permission for this capability; a future capability that needs such scopes requires its own proposal and a separate, explicit tenant opt-in.

#### Scenario: App manifest carries only read scopes

- **WHEN** the GitHub App's configured permissions are inspected
- **THEN** they include only `contents:read` and `metadata:read`, with no write-capable permission present

### Requirement: On-demand, non-persisted access tokens

SignalDB SHALL NOT persist a long-lived GitHub access token. It SHALL hold only the app's private key and each installation's id, and SHALL mint a short-lived installation access token on demand (by signing an app-level JWT and exchanging it with GitHub) each time a linked installation's repo content is fetched. A minted token SHALL NOT be written to durable storage.

#### Scenario: Token minted per fetch

- **WHEN** a source-context fetch needs to call GitHub for a linked installation
- **THEN** SignalDB mints a fresh installation access token for that call rather than reusing a token read from storage

### Requirement: Tenant isolation of installations

An installation linked to one tenant SHALL NOT be usable to authenticate a request on behalf of a different tenant. Resolving which installation covers a given repo for a given fetch SHALL only consider installations linked to the authenticated caller's own tenant.

#### Scenario: Cross-tenant fetch is rejected

- **WHEN** a request authenticated for tenant A asks for source from a repo covered only by an installation linked to tenant B
- **THEN** the request is rejected as if no installation covers that repo, without revealing tenant B's installation

### Requirement: Listing and removal

A tenant admin SHALL be able to list their tenant's linked installations, including each installation's covered repos, and SHALL be able to remove a link. Removing a link SHALL take effect immediately: SignalDB SHALL stop minting tokens for that installation on behalf of the tenant as soon as the removal is acknowledged, independent of whether the installation is also uninstalled on GitHub's side.

#### Scenario: Removed installation stops working immediately

- **WHEN** a tenant admin removes an installation link and a subsequent source-context fetch is attempted against a repo that installation covered
- **THEN** the fetch is rejected as if no installation covers that repo

### Requirement: Every management surface exposes installations identically

The admin/management HTTP API, the generated SDK the CLI consumes, and the Explore UI's tenant settings SHALL all expose install-link, list, and remove for GitHub installations through the same contract, following `admin-management-api-contract`'s OpenAPI-first, no-drift rule.

#### Scenario: CLI and UI see the same installation list

- **WHEN** a tenant's installations are listed via the CLI and via the Explore UI
- **THEN** both show the same installations and covered repos, sourced from the same generated client
