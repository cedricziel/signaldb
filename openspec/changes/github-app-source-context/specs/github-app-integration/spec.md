## Purpose

Lets a tenant admin connect SignalDB's GitHub App to the repos that produce their telemetry, so SignalDB can mint short-lived, read-only tokens for that tenant's repos without ever storing a long-lived GitHub credential.

## ADDED Requirements

### Requirement: Tenant-scoped installation linking

A tenant admin SHALL be able to link a GitHub App installation (produced by GitHub's install flow) to their tenant via the admin/management API, recording the installation id, the app account (org or user) it belongs to, and the set of repos the installation covers. Only users with admin role on the tenant, or instance-level administrators, SHALL be able to link, list, or remove an installation, matching the authorization model in `api-key-management`.

#### Scenario: Admin links a new installation

- **WHEN** a tenant admin completes GitHub's install flow and returns via the redirect carrying a valid, matching state token (per the State-bound install linking requirement) and an installation id
- **THEN** SignalDB records the installation, its covered repos, and its link to the admin's tenant, and the installation appears in that tenant's installation list

#### Scenario: Non-admin cannot link

- **WHEN** a user without tenant-admin or instance-admin role attempts to link an installation
- **THEN** the request is rejected before any installation record is created

### Requirement: State-bound install linking

Before redirecting a tenant admin to GitHub's install flow, SignalDB SHALL generate a single-use, expiring state token bound to that admin's identity and tenant, and SHALL pass it through GitHub's install flow (as the `state` parameter). SignalDB SHALL only accept an installation id presented on the return redirect when it carries a state token that is unexpired, unused, and matches one it issued for that admin and tenant; a bare installation id submitted without a valid matching state token SHALL be rejected. This prevents a tenant admin from linking an installation they did not initiate — e.g. one they learned the id of but that belongs to another organization's install.

#### Scenario: Missing or mismatched state is rejected

- **WHEN** a link request presents an installation id with no state token, an expired state token, or a state token issued for a different admin or tenant
- **THEN** the request is rejected and no installation record is created

#### Scenario: State token is single-use

- **WHEN** a state token that was already consumed by a completed link is presented again
- **THEN** the request is rejected

### Requirement: Read-only permission scope

SignalDB's GitHub App SHALL be configured with only `contents:read` and `metadata:read` permissions. The system SHALL NOT request, accept, or use any write, pull-request, issues, or Actions permission for this capability; a future capability that needs such scopes requires its own proposal and a separate, explicit tenant opt-in.

#### Scenario: App manifest carries only read scopes

- **WHEN** the GitHub App's configured permissions are inspected
- **THEN** they include only `contents:read` and `metadata:read`, with no write-capable permission present

### Requirement: In-memory, non-persisted access tokens

SignalDB SHALL NOT persist a long-lived or usable GitHub access token to durable storage. It SHALL hold only the app's private key (deploy-time configuration) and each installation's id (catalog) in durable storage. It SHALL mint a short-lived installation access token by signing an app-level JWT and exchanging it with GitHub, and MAY hold that minted token in a process-local, non-durable cache keyed by installation id until shortly before GitHub's declared expiry, reusing it for calls within that window rather than minting on every call. A minted token SHALL NOT be written to durable storage at any point, and SHALL be re-minted once expired or evicted from the in-memory cache (e.g. on process restart).

#### Scenario: Token reused within its validity window

- **WHEN** two source-context fetches for the same installation occur within a previously minted token's cached validity window
- **THEN** the second fetch reuses the in-memory cached token rather than minting a new one, and neither token is read from or written to durable storage

#### Scenario: Token re-minted after expiry or restart

- **WHEN** a source-context fetch needs a token for an installation whose cached token is absent, expired, or near expiry
- **THEN** SignalDB mints a fresh installation access token for that call rather than reusing a stale or absent one

### Requirement: Tenant isolation of installations

An installation linked to one tenant SHALL NOT be usable to authenticate a request on behalf of a different tenant. Resolving which installation covers a given repo for a given fetch SHALL only consider installations linked to the authenticated caller's own tenant.

#### Scenario: Cross-tenant fetch is rejected

- **WHEN** a request authenticated for tenant A asks for source from a repo covered only by an installation linked to tenant B
- **THEN** the request is rejected as if no installation covers that repo, without revealing tenant B's installation

### Requirement: Listing and removal

A tenant admin SHALL be able to list their tenant's linked installations, including each installation's covered repos, and SHALL be able to remove a link. A list request SHALL attempt to refresh each installation's covered-repo list from GitHub before returning it; if that refresh call fails (e.g. GitHub unavailable, installation revoked on GitHub's side), the list SHALL fall back to the last successfully fetched repo list rather than failing the whole request, and SHALL indicate that the repo list may be stale. Removing a link SHALL take effect immediately: SignalDB SHALL stop minting tokens for that installation on behalf of the tenant as soon as the removal is acknowledged, independent of whether the installation is also uninstalled on GitHub's side.

#### Scenario: Listing refreshes covered repos

- **WHEN** a tenant admin lists their installations and GitHub reports a different set of covered repos than what was last stored
- **THEN** the returned list reflects the freshly fetched repos, and the stored copy is updated

#### Scenario: Listing falls back to cached repos on refresh failure

- **WHEN** a tenant admin lists their installations and the live refresh call to GitHub fails
- **THEN** the list is still returned, using the last successfully fetched repo list, marked as potentially stale

#### Scenario: Removed installation stops working immediately

- **WHEN** a tenant admin removes an installation link and a subsequent source-context fetch is attempted against a repo that installation covered
- **THEN** the fetch is rejected as if no installation covers that repo

### Requirement: Every management surface exposes installations identically

The admin/management HTTP API, the generated SDK the CLI consumes, and the Explore UI's tenant settings SHALL all expose install-link, list, and remove for GitHub installations through the same contract, following `admin-management-api-contract`'s OpenAPI-first, no-drift rule.

#### Scenario: CLI and UI see the same installation list

- **WHEN** a tenant's installations are listed via the CLI and via the Explore UI
- **THEN** both show the same installations and covered repos, sourced from the same generated client
