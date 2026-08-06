## Why

The explore UI (`src/ui`) kept all navigation state — including which signal
view (logs/traces/metrics/profiles/query) was active — behind a single root
URL with a `?signal=` query param, plus one piece of navigation (the tenant
management panel) that lived in local component state with no URL
representation at all. That meant the management panel couldn't be
deep-linked or bookmarked, and the browser back button closed nothing: it
either did nothing or unwound unrelated query-param history. The OAuth
consent screen was reached via a hand-rolled `window.location.pathname`
check in `main.tsx` that bypassed routing entirely. None of this matched how
the rest of the app is served — the router (`src/router/src/ui.rs`) already
SPA-falls-back any unknown path to `index.html`, so path-based routes were
supported server-side but unused client-side.

## What Changes

- Add `react-router` as the SPA's client-side router (previously hand-rolled
  `window.history`/`popstate` plumbing in `lib/urlState.ts`).
- Each signal now has its own path — `/logs`, `/traces`, `/metrics`,
  `/profiles`, `/query` — instead of `?signal=<value>` on `/`. Time range,
  filters, trace/group selection, and tenant/dataset context remain query
  params, unchanged.
- **BREAKING**: the query param `?signal=` is no longer read; old bookmarked
  `/?signal=traces` links resolve to `/logs` (root now redirects to
  `/logs`), not to the traces view. No migration/redirect shim is provided.
- Tenant/API-key management moves from ad hoc `TopBar` component state to a
  real route, `/manage`, guarded to redirect non-admins to `/logs`.
- `/oauth/consent` becomes a declared route instead of a manual pathname
  check in `main.tsx`; behavior is unchanged.
- Unknown paths (typos, stale bookmarks, e.g. `/bogus`) redirect to `/logs`,
  preserving the query string.

## Capabilities

### New Capabilities

- `explore-ui-navigation`: path-based client-side routing for the explore
  UI — which signal a URL selects, how the management panel and OAuth
  consent screen are reached, and how invalid/legacy URLs resolve.

### Modified Capabilities

(none — no other capability's requirements change; this introduces
client-side routing that previously had no spec coverage)

## Impact

- `src/ui/src/lib/urlState.ts` — signal now derived from the route's
  `:signal` param via `react-router`'s `useParams`/`useLocation`/
  `useNavigate` instead of `window.history`/`popstate`.
- `src/ui/src/routes.tsx` (new) — the route tree.
- `src/ui/src/lib/outletState.ts` (new) — typed outlet-context accessor
  shared by routed children.
- `src/ui/src/App.tsx` — becomes the shell layout (`TopBar` + `Outlet` +
  `LoginGate`) instead of directly rendering the explore view.
- `src/ui/src/features/management/ManagementRoute.tsx` (new) — the
  `/manage` route; `src/ui/src/features/shell/TopBar.tsx` now links to it
  instead of rendering `ManagementPanel` itself.
- `src/ui/src/main.tsx` — wraps the app in `BrowserRouter`; drops the manual
  `/oauth/consent` pathname check.
- `src/ui/package.json` / `pnpm-lock.yaml` — new dependency on
  `react-router`.
- No Rust crates are affected; `src/router/src/ui.rs`'s SPA-fallback
  serving already supports arbitrary client-side paths and needs no change.
- `docs/users/explore-ui.md` updated to describe the new URL scheme.
