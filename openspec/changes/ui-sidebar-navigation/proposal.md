## Why

The explore UI's navigation grew by accretion: a top bar with the brand, a
tenant chip and a user menu, a horizontally scrolling tab strip for seven
signal views, and Schema, Processors, Instrumentation and API keys reachable
only through the user menu. On a phone the tab strip hides most of its tabs,
and there's no way to jump straight to a service or a trace id from
anywhere in the app.

The Claude Design handoff "App navigation" (the `app-shell` / `app-nav`
templates) replaces it with one navigation model for every page.

## What Changes

- Replace the top bar and the signal tab strip with a collapsible left
  sidebar: brand, tenant/dataset switcher, pages grouped as Monitor
  (Errors, Catalog), Investigate (Logs, Traces, Metrics, Profiles, Query)
  and Configure (Schema, Processors, Instrumentation), then Manage
  (admins), the account menu and a collapse toggle.
- The sidebar starts collapsed below 1024px, expanded above; the user's
  toggle (button or `[`) is persisted in `localStorage`.
- Add a sticky page header with a "Group / Page" breadcrumb and a search
  field that opens the command palette.
- Below 720px, a 48px top bar (menu, brand, current page, search, account)
  with a navigation drawer replaces the sidebar.
- Add a centered ⌘K / Ctrl+K command palette: pages, catalog services,
  recent logs/traces queries (per-browser, `localStorage`), actions, and a
  jump to a pasted trace id.
- The tenant/dataset free-text form is replaced by a listbox of the
  user's memberships and the tenant's datasets.

Not breaking: every route and URL parameter is unchanged, and the root
still redirects to `/logs`. The System Overview page (`/overview`) that the
handoff pairs with this shell is a separate change.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `explore-ui-navigation`: selecting a signal happens through the sidebar
  (and the command palette) instead of a tab strip; adds requirements for
  the sidebar, the command palette and the mobile drawer.

## Impact

- **src/ui** only: `App.tsx`, `features/shell/*` (new `AppNav`,
  `PageHeader`, `CommandPalette`, nav and palette models),
  `features/explore/ExploreView.tsx` (tab strip removed), `UserMenu`
  (sidebar/compact triggers), `lib/recentQueries.ts`.
- No HTTP API, CLI or SDK surface changes: the palette reads the existing
  catalog query; recent queries are client-side only.
- `components/TopBar` stays in the design-system export until the design
  sync drops it; the app no longer renders it.
