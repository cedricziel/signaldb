import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NO_RETRY_POLICY, retryingFetch } from "../api/http";
import {
  resetProxyLoginRecoveryState,
  setProxyLoginRecoveryDeps,
  withProxyLoginRecovery,
} from "./proxyLoginRecovery";

/** A redirect-type response — jsdom can't construct a real `opaqueredirect`
 * `Response` (that requires an actual cross-origin fetch), so the probe
 * mock returns a plain object shaped like one instead. */
const REDIRECT_RESPONSE = { type: "opaqueredirect" } as Response;

function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Stub the transport `fetch` to always fail with a connect error, drive it
 * through the decorated `retryingFetch`, and wait for the resulting
 * (fire-and-forget) probe to settle. */
async function failWithConnectError(url = "/api/v1/query"): Promise<void> {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockRejectedValue(new TypeError("Failed to fetch")),
  );
  const fetchWithRecovery = withProxyLoginRecovery(retryingFetch);
  await expect(
    fetchWithRecovery(url, undefined, NO_RETRY_POLICY),
  ).rejects.toBeInstanceOf(TypeError);
  await flush();
}

describe("proxy login recovery", () => {
  let reload: ReturnType<typeof vi.fn<() => void>>;
  let probeFetch: ReturnType<typeof vi.fn<typeof fetch>>;

  beforeEach(() => {
    sessionStorage.clear();
    resetProxyLoginRecoveryState();
    reload = vi.fn();
    probeFetch = vi.fn().mockResolvedValue(REDIRECT_RESPONSE);
    setProxyLoginRecoveryDeps({
      reload,
      isOnline: () => true,
      fetch: probeFetch,
    });
  });

  afterEach(() => {
    setProxyLoginRecoveryDeps();
    resetProxyLoginRecoveryState();
    vi.unstubAllGlobals();
  });

  it("reloads once when a same-origin request fails and the proxy is redirecting to login", async () => {
    await failWithConnectError();
    expect(probeFetch).toHaveBeenCalledWith(
      "/api/v1/whoami",
      expect.objectContaining({
        redirect: "manual",
        cache: "no-store",
        credentials: "same-origin",
      }),
    );
    expect(reload).toHaveBeenCalledTimes(1);
    expect(sessionStorage.getItem("signaldb.proxyLoginReload")).toBe("1");
  });

  it("does not reload again once the marker is already set", async () => {
    sessionStorage.setItem("signaldb.proxyLoginReload", "1");
    resetProxyLoginRecoveryState();
    await failWithConnectError();
    expect(probeFetch).not.toHaveBeenCalled();
    expect(reload).not.toHaveBeenCalled();
  });

  it("probes once and reloads once for many concurrent failures", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockRejectedValue(new TypeError("Failed to fetch")),
    );
    const fetchWithRecovery = withProxyLoginRecovery(retryingFetch);
    await Promise.allSettled([
      fetchWithRecovery("/api/v1/a", undefined, NO_RETRY_POLICY),
      fetchWithRecovery("/api/v1/b", undefined, NO_RETRY_POLICY),
      fetchWithRecovery("/api/v1/c", undefined, NO_RETRY_POLICY),
    ]);
    await flush();
    expect(probeFetch).toHaveBeenCalledTimes(1);
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("does not probe or reload while offline", async () => {
    setProxyLoginRecoveryDeps({
      reload,
      fetch: probeFetch,
      isOnline: () => false,
    });
    await failWithConnectError();
    expect(probeFetch).not.toHaveBeenCalled();
    expect(reload).not.toHaveBeenCalled();
  });

  it("does not reload when the probe gets a normal response", async () => {
    setProxyLoginRecoveryDeps({
      reload,
      isOnline: () => true,
      fetch: vi.fn().mockResolvedValue(new Response(null, { status: 401 })),
    });
    await failWithConnectError();
    expect(reload).not.toHaveBeenCalled();
  });

  it("does not reload when the probe itself rejects", async () => {
    setProxyLoginRecoveryDeps({
      reload,
      isOnline: () => true,
      fetch: vi.fn().mockRejectedValue(new TypeError("still down")),
    });
    await failWithConnectError();
    expect(reload).not.toHaveBeenCalled();
  });

  it("ignores a cross-origin request failure", async () => {
    await failWithConnectError("https://other.test/api/v1/query");
    expect(probeFetch).not.toHaveBeenCalled();
    expect(reload).not.toHaveBeenCalled();
  });

  it("clears the marker on a successful response, allowing a later reload", async () => {
    sessionStorage.setItem("signaldb.proxyLoginReload", "1");
    resetProxyLoginRecoveryState();

    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("ok")));
    const fetchWithRecovery = withProxyLoginRecovery(retryingFetch);
    await fetchWithRecovery("/api/v1/whoami", undefined, NO_RETRY_POLICY);
    expect(sessionStorage.getItem("signaldb.proxyLoginReload")).toBeNull();

    await failWithConnectError();
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("leaves retry behaviour and error propagation of retryingFetch unchanged", async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValueOnce(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);
    const fetchWithRecovery = withProxyLoginRecovery(retryingFetch);
    const res = await fetchWithRecovery("/api/v1/query");
    expect(res.status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(reload).not.toHaveBeenCalled();

    const alwaysFailing = vi
      .fn()
      .mockRejectedValue(new TypeError("Failed to fetch"));
    vi.stubGlobal("fetch", alwaysFailing);
    const err = await fetchWithRecovery(
      "/api/v1/query",
      { method: "POST" },
      NO_RETRY_POLICY,
    ).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(TypeError);
  });
});
