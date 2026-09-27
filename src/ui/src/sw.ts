/// <reference lib="webworker" />
import {
  cleanupOutdatedCaches,
  matchPrecache,
  precacheAndRoute,
} from "workbox-precaching";
import { NavigationRoute, registerRoute } from "workbox-routing";
import { PROXIED_PATHS } from "./lib/proxiedPaths";
import { proxyKey } from "./lib/proxyKey";
import { networkFirstNavigation } from "./sw/navigation";

declare const self: ServiceWorkerGlobalScope;

// `registerSW`'s `updateSW` (src/pwa.ts) posts this once the visitor accepts
// the update banner, to activate the waiting worker immediately.
self.addEventListener("message", (event: ExtendableMessageEvent) => {
  if ((event.data as { type?: unknown } | null)?.type === "SKIP_WAITING") {
    void self.skipWaiting();
  }
});

async function appShell(): Promise<Response> {
  const cached = await matchPrecache("index.html");
  return cached ?? fetch("index.html");
}

// Registered before precacheAndRoute: the precache route also matches "/"
// and "/index.html" (directoryIndex) and, being cache-first, would answer
// every navigation from the cache before this route ever saw the request.
registerRoute(
  new NavigationRoute(
    ({ request }) =>
      networkFirstNavigation(request, { fetch, shell: appShell }),
    {
      // See proxiedPaths.ts: these are the backend's own routes, which this
      // navigation route must never intercept.
      denylist: PROXIED_PATHS.map((path) => new RegExp(proxyKey(path))),
    },
  ),
);

precacheAndRoute(self.__WB_MANIFEST);
cleanupOutdatedCaches();
