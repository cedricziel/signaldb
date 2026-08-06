## 1. Dependency

- [x] 1.1 Add `react-router` to `src/ui/package.json` (`pnpm --filter signaldb-ui add react-router`) and update `pnpm-lock.yaml`.

## 2. Routing core

- [x] 2.1 `src/ui/src/lib/urlState.ts`: export `SIGNALS` and add `signalFromParam(value)`, mapping a `:signal` route param to a known `Signal`, defaulting anything unrecognized/missing to `"logs"`.
- [x] 2.2 `parseExploreState`/`buildSearch`: stop reading/writing `signal` from the query string (it now comes from the route path only).
- [x] 2.3 Rewrite `useExploreState()` to derive `signal` via `useParams()` and `range`/`filters`/etc. via `useLocation().search`, and to navigate (`useNavigate()`, `{ replace: true }`) to `/${signal}${query}` on `update()`, instead of `window.history`/`popstate`.
- [x] 2.4 Add `src/ui/src/lib/outletState.ts`: `ShellContext` type (`{ state, update }`) and `useOutletState()` wrapper around `useOutletContext`.
- [x] 2.5 Add `src/ui/src/routes.tsx`: `AppRoutes` with `/oauth/consent` as a top-level sibling; the shell layout route (`App`) with children `:signal` (`ExploreRoute`, redirecting unrecognized segments to `/logs` + original query string via `<Navigate>`) and `manage` (`ManagementRoute`); `index` and `*` both redirect to `/logs`.
- [x] 2.6 `src/ui/src/App.tsx`: render `TopBar` + `<Outlet context={{ state, update }}>` + `LoginGate` instead of directly rendering `ExploreView`.

## 3. Management panel as a route

- [x] 3.1 Add `src/ui/src/features/management/ManagementRoute.tsx`: fetch `whoami`, redirect (`<Navigate to="/logs" replace>`) unless the user is a tenant admin or instance admin, otherwise render `ManagementPanel` with `onClose` calling `navigate(-1)`.
- [x] 3.2 `src/ui/src/features/shell/TopBar.tsx`: drop local `managing` state and the inline `ManagementPanel` render; render a `<Link to="/manage">` instead of a button with an `onClick` handler.
- [x] 3.3 `src/ui/src/features/shell/TopBar.css`: adjust `.manage-trigger` for anchor rendering (`display: inline-block`, `text-decoration: none`).

## 4. Consent screen and app bootstrap

- [x] 4.1 `src/ui/src/main.tsx`: wrap `AppRoutes` in `<BrowserRouter>`; remove the manual `window.location.pathname === "/oauth/consent"` branch now that `/oauth/consent` is a declared route.

## 5. Tests

- [x] 5.1 `src/ui/src/lib/urlState.test.ts`: add `signalFromParam` coverage; update `parseExploreState`/`buildSearch` tests to reflect that `signal` is no longer read from or written to the query string.
- [x] 5.2 `src/ui/src/App.test.tsx`: render the app via `<BrowserRouter><AppRoutes /></BrowserRouter>`; add coverage for `/` and unknown-path redirects to `/logs`, signal-tab clicks changing `window.location.pathname`, and `/manage` navigation (admin sees the panel and back-button returns to the prior view; non-admin is redirected to `/logs`).
- [x] 5.3 `src/ui/src/features/shell/TopBar.test.tsx`: wrap `TopBar` in a router for the new `<Link>`; assert the Manage control is a link to `/manage` for admins and absent for non-admins (the panel-open behavior moved to `App.test.tsx`).

## 6. Verification and docs

- [x] 6.1 `pnpm test`, `pnpm typecheck`, `pnpm lint` all pass in `src/ui`.
- [x] 6.2 Manually verify in a running dev server: `/` and an unknown path redirect to `/logs` (preserving query params), clicking a signal tab updates the path, `/manage` redirects non-admins to `/logs`, `/oauth/consent` renders standalone.
- [x] 6.3 Update `docs/users/explore-ui.md` to describe the per-signal paths and `/manage`, replacing the old "every view is a URL via query parameters" description.
