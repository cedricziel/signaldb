# Proposal: explore-ui-pwa-installability

## Why

The Explore UI (`src/ui`) is only reachable as a browser tab today — there is
no way to pin it to a phone home screen or give it its own desktop window,
even though it is already the primary first-party client for every signal
(see `client-surface-parity`). Bringing SignalDB to mobile and desktop users
without a second native codebase starts with making the existing web UI
installable: a PWA manifest plus a service worker gets an icon, a standalone
window, and instant/offline loading of the app shell essentially for free
from the app that already exists.

The one hard requirement a PWA adds to an observability dashboard is safe
caching: a service worker that caches too eagerly can show stale
investigation data, and one that updates too passively can strand a
long-lived tab (an on-call engineer's pinned dashboard, or a wall-mounted
always-on tab) on a build that predates a bug fix. Both failure modes are
addressed directly in this change rather than left as follow-up risk.

## What Changes

- **Installable app shell**: a web app manifest (name, themed icons including
  a maskable variant, `display: standalone`) and the iOS-specific
  `apple-touch-icon`/meta tags iOS reads instead of the manifest for "Add to
  Home Screen". Icons are generated reproducibly from the existing
  `favicon.svg` mark via a committed script, not hand-exported.
- **App-shell-only caching**: a generated service worker (`vite-plugin-pwa`)
  precaches JS/CSS/HTML/manifest/favicon only. No `runtimeCaching` entries
  are configured — every query, telemetry, and auth request the UI makes
  continues to hit the network directly and is never served from cache. The
  service worker's SPA navigation fallback excludes every backend-proxied
  route (`/api`, `/loki`, `/tempo`, `/prometheus`, `/pyroscope`, OAuth
  endpoints, `/runtime-config.js`) by reusing the dev proxy's existing
  `proxyKey` path matcher, so a client route and a same-prefixed backend
  route (`/api` vs. `/api-keys`) are never confused.
- **Update without getting stuck**: new builds install and activate silently
  (`registerType: "autoUpdate"`) and reload the page — no unbuilt "new
  version available" prompt to get stuck behind. Because a browser only
  re-checks a service worker for updates on navigation/registration by
  default, and this UI is exactly the kind of dashboard that stays open,
  unnavigated, for days, a visibility-gated poll re-checks for updates
  hourly whenever the tab is in the foreground.

## Capabilities

### New Capabilities

- `explore-ui-pwa`: the Explore UI's installability contract — manifest and
  icons, what the service worker is and is not allowed to cache, and the
  update-delivery guarantee for long-lived tabs.

### Modified Capabilities

_None — no existing spec covers UI installability or service-worker caching._

## Impact

- **UI only** (`src/ui`): `vite.config.ts` (VitePWA plugin config), `index.html`
  (manifest/icon links, iOS meta tags), `src/pwa.ts` + `src/lib/pwaUpdate.ts`
  (update wiring), `public/*.png` + `scripts/generate-pwa-icons.mjs`
  (generated icons), `main.tsx` (deferred registration).
- **CLI / HTTP API**: not applicable. Installability is a browser/UI-shell
  concept with no CLI or server-side analogue; this change adds no endpoint
  and changes no OpenAPI surface, so the usual UI/CLI/API parity requirement
  does not apply here.
- Not BREAKING: purely additive to the UI build (new manifest, new service
  worker, new icons); no API, Flight schema, or on-disk layout changes.
