/**
 * Turn a proxied path prefix into a Vite `server.proxy` key.
 *
 * Vite treats a key starting with `^` as a regex and tests it against the
 * raw request URL (query string included). The pattern anchors the path and
 * lets it continue only at a `/`, a `?`, or the end, so `/api` matches
 * `/api/v1/whoami` and `/api?x=y` but never the SPA route `/api-keys`.
 */
export function proxyKey(path: string): string {
  const literal = path.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return `^${literal}(/|\\?|$)`;
}
