## Why

The standalone SignalDB UI (`src/ui/`) has no user-facing navigation for account management, tenant switching, API key management, or instrumentation guidance. Users must rely on the admin-only management panel or raw API calls. Adding a user menu with dedicated pages closes this UX gap and makes the standalone UI self-service for authenticated users.

## What Changes

- Add **UserMenu** dropdown to the TopBar: avatar with initials, user info, theme toggle, navigation links, and sign-out action
- Add **SelectTenant** page for users with multi-tenant memberships to switch tenants and datasets
- Add **ApiKeys** page for tenant admins to create, list, and revoke ingestion API keys (extracted from the admin-only management panel)
- Add **Instrumentation** page with guided setup instructions for sending telemetry (OTel SDK, Collector, Kubernetes, Docker, journald, Prometheus)
- Update the React Router to include 3 new routes with appropriate auth guards
- Update the Explore UI living doc (`docs/users/explore-ui.md`) to cover the new components

## Capabilities

### New Capabilities

- `user-menu`: Dropdown menu in the top bar showing authenticated user info, theme toggle, navigation shortcuts, and sign-out
- `tenant-selection`: Dedicated page for switching between tenant/dataset combinations from the user's membership list
- `api-key-management`: Self-service page for tenant admins to create and revoke API keys with scoped permissions
- `instrumentation-guide`: Interactive guide showing source-specific code/config snippets for sending telemetry to SignalDB

### Modified Capabilities

<!-- No existing capability specs are being modified — these are all net-new UI features. -->

## Impact

**Affected code**: `src/ui/` only (React SPA, not the Grafana plugin). Specifically:

- `src/ui/src/features/shell/` — UserMenu component + CSS + test, TopBar integration
- `src/ui/src/features/management/` — 3 new page components + CSS + tests
- `src/ui/src/routes.tsx` — new route definitions
- `src/ui/src/styles/global.css` — no changes needed (uses existing design tokens)
- `docs/users/explore-ui.md` — doc update for new components

**No backend changes.** All pages use existing APIs (`whoami`, `deleteSession`, `listApiKeys`, `createApiKey`, `revokeApiKey`).

**No breaking changes.** Pure frontend additions.
