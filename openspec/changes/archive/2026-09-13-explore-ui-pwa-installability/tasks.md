# Tasks: explore-ui-pwa-installability

## 1. Update-check logic (TDD core)

- [x] 1.1 Write failing tests for `schedulePeriodicUpdateCheck`: polls
      `registration.update()` on the configured interval while the tab is
      visible; defaults to an hourly interval; skips the check while hidden;
      checks immediately on regaining visibility once the interval has
      elapsed; returns a disposer that stops further checks (`src/ui`,
      `pnpm test`)
- [x] 1.2 Implement `schedulePeriodicUpdateCheck` in `src/lib/pwaUpdate.ts`;
      tests pass

## 2. Service worker wiring (vite-plugin-pwa)

- [x] 2.1 Add `vite-plugin-pwa` (dev) and `workbox-window` (runtime — it
      ships in the client bundle via the generated `virtual:pwa-register`
      module) as dependencies
- [x] 2.2 Configure the `VitePWA` plugin in `vite.config.ts`:
      `registerType: "autoUpdate"`, manifest (name, themed icons incl.
      maskable, `display: standalone`), no `runtimeCaching` entries
- [x] 2.3 Build the navigate-fallback denylist from `PROXIED_PATHS` via
      `proxyKey`, not a new pattern (avoids reintroducing the `/api` vs.
      `/api-keys` bug the dev proxy already guards against)
- [x] 2.4 `src/pwa.ts`: `initPwaUpdates()` wires `registerSW` to
      `schedulePeriodicUpdateCheck`, idempotent (`started` guard) matching
      `initTelemetry`'s convention; excluded from coverage (imports the
      build-only `virtual:pwa-register` module, same category as
      `telemetry/index.ts`)
- [x] 2.5 Call `initPwaUpdates()` from `main.tsx` via a deferred dynamic
      `import("./pwa")` after the initial render, not the synchronous
      bootstrap sequence

## 3. Manifest, icons, and iOS meta tags

- [x] 3.1 `scripts/generate-pwa-icons.mjs` (`pnpm generate:icons`):
      rasterizes `public/favicon.svg` into 192px/512px icons, a maskable
      512px variant, and the iOS `apple-touch-icon`, via `sharp`
- [x] 3.2 Commit the generated icons under `public/`
- [x] 3.3 `index.html`: `theme-color`, `apple-touch-icon` link,
      `apple-mobile-web-app-capable`/`apple-mobile-web-app-title` meta tags
      (iOS reads these instead of the manifest for "Add to Home Screen")

## 4. Verification

- [x] 4.1 Full test suite green (`pnpm test`), typecheck and lint clean
- [x] 4.2 Production build verified directly: `dist/manifest.webmanifest`
      and `dist/sw.js` generated; inspected the built service worker to
      confirm `skipWaiting()`/`clientsClaim()` are wired, the precache list
      contains only app-shell/icon/manifest entries (no API paths), and the
      navigate-fallback denylist regexes correctly exclude proxied backend
      routes (`/api` no longer matches `/api-keys`) while still escaping
      regex metacharacters in paths like `/.well-known/...`

## 5. Docs

- [x] 5.1 Document PWA installability (manifest/icons, app-shell-only
      caching, silent auto-update) in `docs/users/explore-ui.md`'s
      Availability section
