# Design: explore-ui-pwa-installability

## Context

See `proposal.md` for motivation. Relevant existing structure:

- `src/ui` is a Vite + React SPA served by the router at root, with a dev
  proxy (`vite.config.ts`) forwarding a fixed list of backend path prefixes
  (`PROXIED_PATHS`) to a live instance. `proxyKey()`
  (`src/ui/src/lib/proxyKey.ts`) turns a prefix into a segment-anchored,
  properly-escaped regex string (`^<literal>(/|\?|$)`) specifically so that
  `/api` matches `/api/v1/...` but never the SPA route `/api-keys` — a bug
  the dev proxy hit once already (see the comment above `PROXIED_PATHS`).
- `src/main.tsx` bootstraps `initTheme()`, `sidebarWidth`/`spanDetailWidth`,
  and `initTelemetry()` in sequence before rendering; `initTelemetry()`
  follows an idempotent `let started = false` guard pattern.
- The UI's own brand mark exists as inline SVG in two places already
  (`public/favicon.svg`, `src/components/BrandMark.tsx`) — a third,
  hand-exported copy (PNG icons) would be a third thing to keep in sync by
  hand.

## Goals / Non-Goals

**Goals:**

- Installable on mobile and desktop from the existing web UI, no second
  codebase.
- Live data is never served stale: the service worker caches the app shell
  only.
- A long-lived, never-renavigated tab cannot get stuck on an old build.
- Icons stay reproducible from the one source mark.

**Non-Goals:**

- Offline querying. Offline behavior is scoped to "the app shell loads
  instantly and shows its own UI"; a query made while offline fails as a
  normal network error, exactly as it would without a service worker.
- A native desktop wrapper (Tauri) or native mobile SDKs — separate,
  larger changes tracked outside this one.
- A visible "update available" prompt UI. `autoUpdate` was chosen
  specifically so no such UI needs to exist yet (see Decision 1).

## Decisions

### 1. `registerType: "autoUpdate"`, not `"prompt"`

`vite-plugin-pwa`'s `"prompt"` mode installs a new service worker but leaves
it waiting until the app calls `updateSW()` — normally from a "new version
available, reload?" banner. Building that banner was out of scope for a
first version, and shipping `"prompt"` without one means a new build simply
never activates: the exact "stuck" failure this change exists to avoid.
`"autoUpdate"` installs, activates (`skipWaiting()` + `clientsClaim()`), and
reloads automatically once a new service worker is detected — no UI to
build, no way to silently strand a tab.

### 2. A visibility-gated poll, not reliance on the browser's own check

A service worker's own update check fires on navigation or re-registration.
An always-open dashboard tab may do neither for days. `schedulePeriodicUpdateCheck`
(`src/ui/src/lib/pwaUpdate.ts`) polls `registration.update()` hourly, gated
on `document.visibilityState === "visible"`: a hidden background tab gains
nothing from a fresher build nobody is looking at, and `update()` forces a
network revalidation that (once a change is detected) reloads every open tab
— unconditional polling would multiply that cost across however many tabs
are open. A `visibilitychange` listener also checks immediately when a tab
regains focus after being hidden past the interval, so returning to a
backgrounded tab doesn't cost up to an extra hour's wait.

### 3. No `runtimeCaching` entries at all

The workbox config declares no runtime caching strategy for `/api`, `/loki`,
`/tempo`, `/prometheus`, `/pyroscope`, or any other data path. Workbox only
intercepts requests matching a configured route; omitting these paths
entirely means they pass straight to the network, untouched by the service
worker, by construction — there is no cache-then-network or
stale-while-revalidate policy to misconfigure because none exists. The
alternative (a short-TTL cache for resilience) was rejected: a dashboard
showing a cached query result during an active investigation is a worse
failure mode than a request that simply fails offline.

### 4. Navigate-fallback denylist reuses `proxyKey`, not a new pattern

The service worker's SPA navigation fallback (serve cached `index.html` for
any unmatched navigation) must exclude the backend's own routes, or a
`GET /api/v1/whoami` navigation would resolve to the cached app shell
instead of hitting the server. The denylist is built by mapping
`PROXIED_PATHS` through `new RegExp(proxyKey(path))` — the same
segment-anchored, escaped pattern the dev proxy already uses — instead of a
second hand-written regex, so the `/api` vs. `/api-keys` bug class has
exactly one place it can be introduced (and is already guarded there).

### 5. Icons generated from `favicon.svg`, committed alongside a regenerating script

`scripts/generate-pwa-icons.mjs` (a `sharp`-based script, `pnpm
generate:icons`) rasterizes `public/favicon.svg` into the 192px/512px icons,
a maskable 512px variant (80%-scale artwork on an opaque background, inside
the safe zone OS icon masks crop to), and the iOS `apple-touch-icon`. The
generated PNGs are committed (a build step shouldn't need image tooling at
build time), but the script that reproduces them is committed too — so the
mark can change once, in one place, without the icons silently drifting.

### 6. Registration deferred past first paint

`initPwaUpdates()` is called via a dynamic `import("./pwa")` after
`createRoot().render()`, not from the synchronous bootstrap sequence in
`main.tsx`. Service worker registration itself already waits for the
`load` event internally, so nothing about it benefits from running before
first paint — and a static import would pull `workbox-window` into the
entry chunk and compete with initial render for parse time.

## Risks / Trade-offs

- [Silent auto-reload could interrupt an in-progress investigation] →
  Accepted: the alternative (a tab stuck on a stale, possibly-buggy build
  indefinitely) is the worse failure mode for an ops tool, and reloads only
  happen after a real update is detected — at most hourly, and only while
  the tab is visible.
- [No offline query support] → Explicit non-goal; the app shell loading
  instantly is the offline value delivered here, not offline data access.
- [Icons are committed binaries] → Mitigated by the regenerating script
  (Decision 5); this is the same trade-off every repo with committed,
  source-derived image assets makes.

## Migration Plan

Purely additive to the UI build: new manifest, new service worker, new
icons, no server-side or API change. Deploys with any router/UI build
combination — an old server serving the new UI works unchanged, and a
rollback is a normal UI redeploy (the outgoing service worker's own
`autoUpdate` mechanism picks up the rollback build the same way it picks up
a forward update).

## Open Questions

None outstanding; offline query support and a native desktop/mobile client
are explicitly out of scope (see Non-Goals) and would each warrant their own
proposal.
