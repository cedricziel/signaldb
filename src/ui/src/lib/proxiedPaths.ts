/**
 * Paths the SignalDB router serves directly (not SPA client-side routes).
 *
 * Two consumers share this list from `vite.config.ts`:
 * - the dev server's proxy, which forwards these to a live backend instance
 *   so the browser only ever sees same-origin requests, exactly as in the
 *   embedded production build;
 * - the PWA service worker's `navigateFallbackDenylist`, which must never
 *   intercept a top-level browser navigation to one of these with the
 *   cached app shell — that would silently swallow the real response
 *   (notably GitHub's OAuth-callback redirect to `/ui/github/callback`,
 *   which never reaches the server at all if it's missing here).
 *
 * Every entry matches whole path segments (see `proxyKey`): a bare prefix
 * would also swallow SPA routes that merely start with it, as `/api` once
 * did to `/api-keys`.
 */
export const PROXIED_PATHS = [
  "/loki",
  "/tempo",
  "/prometheus",
  "/pyroscope",
  "/api",
  "/ui/session",
  "/ui/github/callback",
  "/runtime-config.js",
  "/.well-known/oauth-authorization-server",
  "/.well-known/oauth-protected-resource",
  "/oauth/authorize",
  "/oauth/consent/context",
  "/oauth/register",
  "/oauth/token",
];
