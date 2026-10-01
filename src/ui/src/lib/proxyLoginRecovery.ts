/**
 * Recovers from a reverse auth proxy whose login has expired: every
 * same-origin request then gets redirected cross-origin to the proxy's own
 * login page, which the browser turns into an opaque `TypeError` —
 * indistinguishable from the network just being down. A full reload sends
 * the browser's own top-level navigation instead of a `fetch`, which the
 * proxy is free to redirect wherever it likes.
 *
 * Generic by design: any proxy that answers an expired login with a
 * redirect works here.
 */
import { isSameOrigin } from "./redirectTarget";

const MARKER_KEY = "signaldb.proxyLoginReload";

/** The probe target: `/api/v1/whoami`. */
const WHOAMI_PATH = "/api/v1/whoami";

interface RecoveryDeps {
  fetch: typeof fetch;
  reload: () => void;
  isOnline: () => boolean;
}

const defaultDeps: RecoveryDeps = {
  // eslint-disable-next-line no-restricted-syntax -- the default transport this wrapper decorates
  fetch: (...args) => globalThis.fetch(...args),
  reload: () => window.location.reload(),
  isOnline: () => navigator.onLine,
};

let deps: RecoveryDeps = defaultDeps;

/** `null` until the marker's state is first read from `sessionStorage`, so
 * a normal successful response (the common case) doesn't touch storage. */
let markerSet: boolean | null = null;
let probeInFlight: Promise<void> | null = null;

function isMarkerSet(): boolean {
  if (markerSet === null) {
    try {
      markerSet = sessionStorage.getItem(MARKER_KEY) === "1";
    } catch {
      // Storage unavailable (private mode): behave as if never set.
      markerSet = false;
    }
  }
  return markerSet;
}

function setMarker(): void {
  markerSet = true;
  try {
    sessionStorage.setItem(MARKER_KEY, "1");
  } catch {
    // Storage unavailable: the in-memory flag still prevents a loop for the
    // rest of this page's lifetime.
  }
}

/** Clear the marker once a request gets any HTTP response — the proxy let
 * it through, so a later expiry should be allowed to reload again. Reads
 * the cached in-memory flag first so the common case (already clear) never
 * touches `sessionStorage`. */
export function clearProxyLoginMarker(): void {
  if (!isMarkerSet()) return;
  markerSet = false;
  try {
    sessionStorage.removeItem(MARKER_KEY);
  } catch {
    // ignore
  }
}

async function probe(): Promise<void> {
  try {
    const response = await deps.fetch(WHOAMI_PATH, {
      redirect: "manual",
      cache: "no-store",
      credentials: "same-origin",
    });
    if (response.type === "opaqueredirect") {
      setMarker();
      deps.reload();
    }
  } catch {
    // Probe itself failed (still offline, etc.): nothing to recover from.
  }
}

/**
 * Called when a same-origin request fails with a `TypeError` (connect
 * failure). Probes once, with the global `fetch` (never the decorated
 * transport, to avoid recursing), for whether the proxy is redirecting to
 * its own login — and if so, reloads the page once.
 */
function notifyFetchFailure(url: string): void {
  if (!deps.isOnline()) return;
  if (!isSameOrigin(url)) return;
  if (isMarkerSet()) return;
  if (probeInFlight) return;
  probeInFlight = probe().finally(() => {
    probeInFlight = null;
  });
}

/**
 * Wraps a `fetch`-shaped function so every call feeds this module's
 * recovery logic: a returned response (of any status) clears the marker,
 * and a `TypeError` triggers {@link notifyFetchFailure} before the error is
 * rethrown unchanged. Install it on whatever transport a caller already
 * uses as `fetch` — the generated client's config, or a raw caller's own
 * fetch reference — rather than baking it into `retryingFetch` itself,
 * which has no notion of proxies or logins.
 */
export function withProxyLoginRecovery<F extends typeof fetch>(fetchFn: F): F {
  return (async (...args: Parameters<F>) => {
    const [input] = args;
    try {
      const response = await (
        fetchFn as (...a: Parameters<F>) => Promise<Response>
      )(...args);
      clearProxyLoginMarker();
      return response;
    } catch (err) {
      if (err instanceof TypeError) {
        notifyFetchFailure(
          input instanceof Request ? input.url : String(input),
        );
      }
      throw err;
    }
  }) as F;
}

/** Test hook: override fetch/reload/online-detection, or call with no
 * arguments to restore the real globals. Does not reset the marker or
 * in-flight-probe state — pair with `resetProxyLoginRecoveryState`. */
export function setProxyLoginRecoveryDeps(
  overrides?: Partial<RecoveryDeps>,
): void {
  deps = overrides ? { ...defaultDeps, ...overrides } : defaultDeps;
}

/** Test hook: forget the in-memory marker cache and any in-flight probe,
 * so the next check re-reads `sessionStorage`. */
export function resetProxyLoginRecoveryState(): void {
  markerSet = null;
  probeInFlight = null;
}
