## Context

See proposal.md for motivation. The standalone SignalDB UI (`src/ui/`) is a React 19 SPA with React Router 8, TanStack Query 5, and plain CSS. It's served by the router at `/ui` from a configurable `SIGNALDB_UI_DIR`. Authentication uses HTTP-only session cookies via `/ui/session`. The `whoami` API returns user info, memberships, tenant, and datasets.

**Current state**: The TopBar has logo, tenant selector, and a "Manage" link (admin-only). No user-facing navigation exists. Three pages are needed: tenant selection, API key management (extracted from admin panel), and instrumentation guide.

**Constraints**: No backend changes. All data comes from existing APIs. CSS uses custom properties (design tokens) from `global.css`. Tests use `renderWithClient` + `stubFetchRoutes` + `MemoryRouter` + Vitest + Testing Library.

## Goals / Non-Goals

**Goals:**

- User menu dropdown in TopBar with avatar, info, theme toggle, nav, sign-out
- Three new pages as route children of App (inherit TopBar via Outlet)
- Admin guard on ApiKeys page, auth guard on SelectTenant and Instrumentation
- Follow existing test, CSS, and component patterns exactly

**Non-Goals:**

- Real-time verification of signal ingestion (hardcoded "Waiting for data" for now)
- Multi-language instrumentation examples (English only, OTel SDK per language is a follow-up)
- Role-based dataset filtering (all datasets shown for current tenant)
- Theme system redesign (only toggle between existing light/dark)
- Backend API changes

## Decisions

### Pages as route children of App (not standalone)

All three new pages (`/select-tenant`, `/api-keys`, `/instrumentation`) are children of the `<App />` route, so they inherit the TopBar and shell layout via `<Outlet>`. This matches how the existing Explore and Management routes work.

**Alternative**: Standalone routes without the shell — rejected because users expect consistent navigation.

### UserMenu integrated into TopBar, not App

UserMenu is rendered inside TopBar because the TopBar is the natural location for user-facing controls in a top navigation bar. The TopBar already has the `whoami` query and role logic; UserMenu shares this data via its own query with the same query key, so React Query deduplicates.

**Alternative**: Render UserMenu in App and pass it to TopBar as a prop — more indirection for no benefit.

### Theme toggle uses localStorage, not server-side

Theme preference is stored in `localStorage` and applied via `data-theme` on `<html>`. This is consistent with the existing theme system in `global.css`. No backend state needed.

### ApiKeys page extracts from ManagementPanel

The API key CRUD logic already exists in `ManagementPanel.tsx`. The new ApiKeys page reuses the same API functions from `management.ts` and the same auth guard pattern from `ManagementRoute.tsx`. This is a focused extraction, not a refactor — ManagementPanel keeps its own copy for now.

**Alternative**: Extract a shared `ApiKeyManager` component — premature abstraction; the admin panel may diverge in the future.

### Lazy-fetch datasets for non-current tenants

The `whoami()` response only includes datasets for the current tenant. When a user expands a different tenant on the SelectTenant page, a separate `whoami(tenant_id)` call fetches that tenant's datasets. This avoids requiring a new API endpoint.

**Alternative**: Create a `/tenants/:id/datasets` endpoint — backend change, out of scope.

### Instrumentation snippets are static with runtime interpolation

Code snippets for each source are hardcoded strings with placeholder values replaced at render time using `whoami` data. No markdown parsing or dynamic loading needed.

**Alternative**: Load snippets from external files or an API — unnecessary complexity for 6 short snippets.

## Risks / Trade-offs

| Risk                                                              | Mitigation                                                                                                                                |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| UserMenu `whoami` query duplicates TopBar's query                 | React Query deduplicates by query key; both use `["whoami", tenant, dataset]`                                                             |
| ApiKeys page and ManagementPanel diverge over time                | Acceptable — they serve different user personas (tenant admin vs. instance admin). Can extract shared component later if needed.          |
| Instrumentation snippets become stale as SignalDB evolves         | Snippets reference stable API paths (`/api/v1/prometheus/write`, port `3000`/`4317`). Review on API changes.                              |
| SelectTenant lazy-fetch adds latency on expand                    | Single whoami call is fast (<100ms). Show loading state during fetch.                                                                     |
| CSS z-index conflicts between UserMenu popover and other overlays | UserMenu uses z-index 41 (popover) / 40 (backdrop). TopBar should have sufficient z-index. Test with other overlays (LoginPanel, modals). |

## Migration Plan

1. No data migration needed — pure frontend additions
2. Deploy: build UI, serve from `SIGNALDB_UI_DIR`
3. Rollback: revert the build, no state to clean up
4. Feature flag: none — all features available to all authenticated users
