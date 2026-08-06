## Context

See proposal.md - Why. Relevant current state:

- `src/ui/src/lib/urlState.ts` already parses/serializes most explore state
  (range, filters, trace/group selection, PromQL, profile selectors,
  tenant/dataset) to/from the query string via a hand-rolled
  `useExploreState()` hook (`window.history.replaceState` + a `popstate`
  listener). Only the signal itself lived in `?signal=`.
- `src/ui/src/features/shell/TopBar.tsx` held the management panel open/closed
  as local `useState`, rendering `ManagementPanel` as a sibling of the header
  inside a fragment.
- `src/ui/src/main.tsx` special-cased `window.location.pathname ===
"/oauth/consent"` to decide whether to mount `ConsentView` or `App`.
- `src/router/src/ui.rs` already serves any unknown path as `index.html`
  (SPA fallback) in production, and the Vite dev server does the same in
  development — so client-side path routing needs no server changes.
- The UI has zero existing routing dependency; `package.json` lists no
  `react-router*` package before this change.

## Goals / Non-Goals

**Goals:**

- Give every reachable screen (signal view, management panel, OAuth
  consent) a real, distinct URL.
- Keep non-signal state (range/filters/trace id/tenant/etc.) in query
  params, unchanged in shape, so existing bookmarks minus the signal
  selector keep working.
- Make the management panel participate in browser history (openable via
  URL, closable via back button).

**Non-Goals:**

- No redirect/alias from the old `?signal=` query param to the new path —
  breaking changes are acceptable post-1.0 (see CLAUDE.md); no shim is
  added.
- No change to which state lives in the query string vs. the path beyond
  moving `signal` itself — filters, range, trace id, etc. are out of scope.
- No nested/sub-routes within a signal view (e.g. no `/traces/:traceId`);
  trace/group selection stays a query param, matching how it already
  worked.

## Decisions

**Adopt `react-router` (v8) instead of extending the hand-rolled
`window.history` pattern.** The alternative — teaching `urlState.ts` to also
own a second piece of path state and a `/manage` visibility flag — would
have meant hand-rolling route matching, redirects, and outlet-style context
passing that `react-router` already provides. The user chose this directly
when this change was scoped (full path-based routing + adopt react-router)
over extending the hand-rolled pattern.

**Route tree shape**: `/oauth/consent` is a top-level sibling route,
independent of the shell — matches its current behavior of bypassing the
explore UI entirely. Everything else nests under a shell layout route
(`App`, rendering `TopBar` + `<Outlet>` + `LoginGate`) with children
`:signal` (the explore view) and `manage`. The shell computes the one
`ExploreState`/`update` pair via `useExploreState()` and passes it to
children through `<Outlet context>` (typed via `lib/outletState.ts`) rather
than each route re-deriving it — there is exactly one source of truth for
tenant/dataset/range etc., matching the pre-existing design where `App` held
that state.

**`useParams` on the parent layout, not each leaf route.** React Router
merges route params across the whole matched branch, so `App` (the parent)
can read `useParams().signal` even though `:signal` is declared on a child
route, and gets `undefined` on `/manage` (which declares no such param).
This keeps `useExploreState()` a single hook call at the shell level instead
of duplicating it per route.

**`/manage` is a standalone route, not an overlay nested inside `:signal`.**
The pre-existing UX rendered the management panel as a fixed, blurred-backdrop
overlay on top of whatever signal view was active, opened via local state.
Making it a real route sacrifices that blurred-backdrop-over-live-content
effect (the signal view unmounts while on `/manage`, since it isn't part of
that route's matched tree) in exchange for URL-addressability and back-button
correctness. `ManagementPanel` itself (the fixed backdrop + centered panel)
is unchanged — only what mounts it moved from `TopBar` state to a route
component (`ManagementRoute`).

**Invalid signal segments redirect via `<Navigate>`, not silent
defaulting alone.** `signalFromParam()` defaults an unrecognized value to
`"logs"` for state purposes, but `ExploreRoute` additionally checks the raw
param against `SIGNALS` and issues a `<Navigate to="/logs...">` when it
doesn't match — so the address bar itself corrects to `/logs` instead of
quietly rendering the logs view under a wrong-looking URL.

**Tenant creation from `/manage` lands on `/logs`, not the signal the user
was on before opening management.** Previously, creating a tenant called
`update({tenant, dataset})` while remaining on whatever signal tab was
already active (management was an overlay, so the underlying signal never
changed). Since `/manage` has no signal in its URL, `update()` (which always
navigates to `/${state.signal}...`) defaults to `/logs`. This was accepted
as a reasonable simplification rather than reintroducing non-URL "last
active signal" state to preserve the exact prior destination.

**Alternatives considered:**

- _Keep `?signal=` and only fix `/manage`_: rejected — user explicitly chose
  full path-based routing over the smaller fix during scoping.
- _Nest `/manage` under `/:signal/manage` to preserve the overlay-over-content
  visual_: rejected as unnecessary complexity for a cosmetic effect (blurred
  content behind the panel) with no functional value; the panel is fully
  legible as a standalone screen.

## Risks / Trade-offs

- **[Old bookmarks/links with `?signal=X` silently land on `/logs`]** →
  Accepted per CLAUDE.md's "breaking changes OK post-1.0" guidance; the UI
  is not yet at a stability point where legacy query shims are warranted.
- **[Losing the blurred-backdrop-over-live-content visual when opening
  `/manage`]** → Accepted; `ManagementPanel` still renders as a centered,
  bordered panel, just without the previous view visible/blurred behind it.
- **[New runtime dependency (`react-router`) increases bundle size]** →
  Accepted as the cost of standard, tested route-matching/history handling
  instead of hand-rolled path logic; `pnpm typecheck`/`pnpm lint`/`pnpm test`
  all pass with it added.

## Migration Plan

No data migration. This is a client-only SPA change:

1. Land the dependency + routing refactor (`urlState.ts`, `routes.tsx`,
   `App.tsx`, `ManagementRoute.tsx`, `TopBar.tsx`, `main.tsx`) together —
   they are interdependent and don't work individually.
2. No server-side deploy coordination needed: `src/router/src/ui.rs`'s
   SPA-fallback already serves any path as `index.html`.
3. Rollback is a plain revert of the same commit(s); no persisted state or
   external contract changes hands.
