import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createApiKey, revokeApiKey } from "./management";
import { ApiError, setTenantContext } from "./http";
import { client } from "./gen/client.gen";

// The generated client builds a `new Request(url, init)` before calling fetch.
// In the browser a same-origin base URL ("") resolves against the document
// origin, but the Node/undici `Request` in the test environment rejects a
// relative URL. Give the client an absolute base for the duration of the
// tests; the request path and headers we assert on are unaffected.
beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
  client.setConfig({ baseUrl: "" });
});

describe("management API", () => {
  it("creates an explicitly scoped, dataset-bound key", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          id: "key-1",
          key: "sdbk_secret",
          dataset_id: "prod",
          scopes: ["metrics:write"],
        }),
        { status: 201, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    setTenantContext({ tenant: "acme", dataset: "prod" });

    const result = await createApiKey("acme", {
      name: "collector",
      dataset_id: "prod",
      scopes: ["metrics:write"],
    });

    expect(result.key).toBe("sdbk_secret");

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/api-keys");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual({
      name: "collector",
      dataset_id: "prod",
      scopes: ["metrics:write"],
    });
    // The interceptor applies the request-scoped tenant headers.
    expect(req.headers.get("X-Tenant-ID")).toBe("acme");
    expect(req.headers.get("X-Dataset-ID")).toBe("prod");
  });

  it("revokes only the selected tenant key", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(revokeApiKey("acme", "key-1")).resolves.toBeUndefined();

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/api-keys/key-1");
    expect(req.method).toBe("DELETE");
  });

  it("rejects with an ApiError carrying the HTTP status", async () => {
    // A Response body can be read only once; this test makes two calls, so
    // mint a fresh Response per call rather than sharing one instance.
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        new Response(JSON.stringify({ error: "forbidden" }), {
          status: 403,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      createApiKey("acme", { scopes: ["metrics:write"] }),
    ).rejects.toMatchObject({ name: "ApiError", status: 403 });
    await expect(
      createApiKey("acme", { scopes: ["metrics:write"] }),
    ).rejects.toBeInstanceOf(ApiError);
  });
});
