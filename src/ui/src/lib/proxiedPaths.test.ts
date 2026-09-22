import { describe, expect, it } from "vitest";
import { proxyKey } from "./proxyKey";
import { PROXIED_PATHS } from "./proxiedPaths";

const isDenylisted = (url: string) =>
  PROXIED_PATHS.some((path) => new RegExp(proxyKey(path)).test(url));

describe("PROXIED_PATHS", () => {
  it("covers every backend route the router serves directly under /ui", () => {
    // Regression: GitHub's OAuth-callback redirect landed on this exact
    // path, but it was missing from the list — the PWA service worker's
    // navigateFallbackDenylist (derived from this same array) then
    // intercepted the browser's top-level navigation and served the
    // cached app shell instead of letting the request reach the router,
    // silently dropping the callback with no server-side trace at all.
    expect(isDenylisted("/ui/github/callback?code=abc&state=xyz")).toBe(true);
    expect(isDenylisted("/ui/session?next=%2Flogs")).toBe(true);
  });

  it("leaves SPA client-side routes alone", () => {
    // /integrations/github is the React route the callback redirects to
    // once linking completes — it must still be served the cached app
    // shell, not proxied to the backend.
    expect(isDenylisted("/integrations/github?github=linked")).toBe(false);
  });
});
