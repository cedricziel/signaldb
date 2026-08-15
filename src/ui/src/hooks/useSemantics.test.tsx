import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { setTenantContext } from "../api/http";
import { client as generatedClient } from "../api/gen/client.gen";
import { stubFetchRoutes } from "../test/render";
import {
  resetSemanticsCache,
  useAttributeSearch,
  useSemantics,
} from "./useSemantics";

const POD_UID = {
  key: "k8s.pod.uid",
  brief: "The UID of the Pod.",
  type: "string",
  group_id: "registry.k8s.pod",
  group_display_name: "Kubernetes Attributes",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
};

/** Answer `?keys=` with a resolution per key, known ones from `known`. */
function resolvingFetch(known: Record<string, unknown> = {}) {
  return vi
    .fn()
    .mockImplementation(async (input: RequestInfo | URL): Promise<Response> => {
      const url = new URL(String(input instanceof Request ? input.url : input));
      const keys = (url.searchParams.get("keys") ?? "").split(",");
      const resolutions = keys.map((key) => {
        const hit = known[key];
        return hit ? { key, hits: [hit], primary: hit } : { key, hits: [] };
      });
      return new Response(JSON.stringify({ hits: [], resolutions }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    });
}

/** Route the generated client (and raw fetch) through `fn`. */
function installFetch(fn: ReturnType<typeof vi.fn>) {
  vi.stubGlobal("fetch", fn);
  generatedClient.setConfig({
    baseUrl: "http://localhost",
    fetch: fn as unknown as typeof fetch,
  });
}

const keysOf = (fetchMock: ReturnType<typeof vi.fn>, call: number) =>
  new URL(String((fetchMock.mock.calls[call]![0] as Request).url)).searchParams
    .get("keys")!
    .split(",");

beforeEach(() => {
  resetSemanticsCache();
  setTenantContext({ tenant: "acme", dataset: "prod" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
});

describe("useSemantics", () => {
  it("resolves known keys and leaves unknown ones absent", async () => {
    const fetchMock = resolvingFetch({ "k8s.pod.uid": POD_UID });
    installFetch(fetchMock);
    const { result } = renderHook(() =>
      useSemantics(["k8s.pod.uid", "app.order.id"]),
    );
    // Rows render immediately: the first snapshot is empty, never pending.
    expect(result.current.size).toBe(0);
    await waitFor(() => expect(result.current.has("k8s.pod.uid")).toBe(true));
    const sem = result.current.get("k8s.pod.uid")!;
    expect(sem.primary.brief).toBe("The UID of the Pod.");
    expect(sem.title).toBe("Kubernetes Attributes");
    expect(result.current.has("app.order.id")).toBe(false);
  });

  it("batches and de-duplicates keys across hooks into one request", async () => {
    const fetchMock = resolvingFetch();
    installFetch(fetchMock);
    renderHook(() => useSemantics(["a", "b", "b"]));
    renderHook(() => useSemantics(["b", "c"]));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(keysOf(fetchMock, 0).sort()).toEqual(["a", "b", "c"]);
  });

  it("chunks large key sets at the batch limit", async () => {
    const fetchMock = resolvingFetch();
    installFetch(fetchMock);
    const many = Array.from({ length: 250 }, (_, i) => `k${i}`);
    renderHook(() => useSemantics(many));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(keysOf(fetchMock, 0)).toHaveLength(200);
    expect(keysOf(fetchMock, 1)).toHaveLength(50);
  });

  it("serves later hooks from the session cache without refetching", async () => {
    const fetchMock = resolvingFetch({ "k8s.pod.uid": POD_UID });
    installFetch(fetchMock);
    const first = renderHook(() => useSemantics(["k8s.pod.uid"]));
    await waitFor(() =>
      expect(first.result.current.has("k8s.pod.uid")).toBe(true),
    );
    first.unmount();
    const second = renderHook(() => useSemantics(["k8s.pod.uid"]));
    expect(second.result.current.has("k8s.pod.uid")).toBe(true);
    await new Promise((r) => setTimeout(r, 60));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("caches per tenant: switching tenants resolves again", async () => {
    const fetchMock = resolvingFetch({ "k8s.pod.uid": POD_UID });
    installFetch(fetchMock);
    const hook = renderHook(() => useSemantics(["k8s.pod.uid"]));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    setTenantContext({ tenant: "globex", dataset: "prod" });
    hook.rerender();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  });

  it("degrades to no semantics when the endpoint fails, without throwing", async () => {
    const fetchMock = stubFetchRoutes([
      {
        match: "/api/v1/schema/attributes",
        body: { error: "boom" },
        status: 500,
      },
    ]);
    const { result } = renderHook(() => useSemantics(["k8s.pod.uid"]));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    await new Promise((r) => setTimeout(r, 30));
    expect(result.current.size).toBe(0);
  });

  it("degrades to no semantics on a network error", async () => {
    installFetch(vi.fn().mockRejectedValue(new TypeError("network down")));
    const { result } = renderHook(() => useSemantics(["k8s.pod.uid"]));
    await new Promise((r) => setTimeout(r, 60));
    expect(result.current.size).toBe(0);
  });
});

describe("useAttributeSearch", () => {
  it("returns prefix hits and an empty list for a blank prefix", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/schema/attributes", body: { hits: [POD_UID] } },
    ]);
    const blank = renderHook(() => useAttributeSearch(""));
    expect(blank.result.current).toEqual([]);
    const { result } = renderHook(() => useAttributeSearch("k8s.po"));
    await waitFor(() => expect(result.current).toHaveLength(1));
    expect(result.current[0]!.key).toBe("k8s.pod.uid");
    const url = new URL(String((fetchMock.mock.calls[0]![0] as Request).url));
    expect(url.searchParams.get("prefix")).toBe("k8s.po");
  });

  it("degrades to no hits on error", async () => {
    const fetchMock = stubFetchRoutes([
      { match: "/api/v1/schema/attributes", body: { error: "x" }, status: 500 },
    ]);
    const { result } = renderHook(() => useAttributeSearch("k8s"));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    await new Promise((r) => setTimeout(r, 30));
    expect(result.current).toEqual([]);
  });
});
