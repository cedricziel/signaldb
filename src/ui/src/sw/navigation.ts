/** How long a navigation waits on the network before falling back to the
 * precached app shell — long enough for a slow but live proxy, short enough
 * that a truly offline visitor isn't stuck staring at a blank tab. */
export const NAVIGATION_TIMEOUT_MS = 5_000;

export interface NavigationFetchDeps {
  fetch: (request: Request) => Promise<Response>;
  shell: () => Promise<Response>;
  timeoutMs?: number;
}

/**
 * Network-first navigation handler, kept free of `self`/workbox so it can be
 * unit-tested under vitest/jsdom. The network response — redirect included —
 * is returned as-is: a same-origin navigation must be allowed to follow a
 * reverse proxy's redirect to its own login page, which an
 * `opaqueredirect`-swallowing cache-first strategy would otherwise hide
 * behind the cached app shell forever. Only a rejected or too-slow fetch
 * (offline, or the proxy itself unreachable) falls back to the shell.
 */
export async function networkFirstNavigation(
  request: Request,
  { fetch, shell, timeoutMs = NAVIGATION_TIMEOUT_MS }: NavigationFetchDeps,
): Promise<Response> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error("navigation timeout")),
      timeoutMs,
    );
  });
  try {
    // eslint-disable-next-line no-restricted-syntax -- injected navigation transport, not an API call
    return await Promise.race([fetch(request), timeout]);
  } catch {
    return shell();
  } finally {
    clearTimeout(timer);
  }
}
