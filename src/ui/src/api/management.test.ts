import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createApiKey,
  createDataset,
  createTenant,
  deleteDataset,
  listApiKeys,
  listMemberships,
  removeMembership,
  revokeApiKey,
  upsertMembership,
} from "./management";
import { ApiError, setTenantContext } from "./http";
import { client } from "./gen/client.gen";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

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

  it("lists a tenant's API keys", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse([{ id: "key-1" }]));
    vi.stubGlobal("fetch", fetchMock);

    const result = await listApiKeys("acme");

    expect(result).toEqual([{ id: "key-1" }]);
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/api-keys");
    expect(req.method).toBe("GET");
  });

  it("creates a dataset under a tenant", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ id: "staging" }, 201));
    vi.stubGlobal("fetch", fetchMock);

    const result = await createDataset("acme", "staging");

    expect(result).toEqual({ id: "staging" });
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/datasets");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual({ name: "staging" });
  });

  it("deletes a dataset by name", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(deleteDataset("acme", "staging")).resolves.toBeUndefined();

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/datasets/staging");
    expect(req.method).toBe("DELETE");
  });

  it("creates a tenant with an optional default dataset", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ id: "acme" }, 201));
    vi.stubGlobal("fetch", fetchMock);

    const result = await createTenant({
      id: "acme",
      name: "Acme Corp",
      default_dataset: "production",
    });

    expect(result).toEqual({ id: "acme" });
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual({
      id: "acme",
      name: "Acme Corp",
      default_dataset: "production",
    });
  });

  it("lists a tenant's memberships", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse([{ user_id: "u1", role: "admin" }]));
    vi.stubGlobal("fetch", fetchMock);

    const result = await listMemberships("acme");

    expect(result).toEqual([{ user_id: "u1", role: "admin" }]);
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/memberships");
    expect(req.method).toBe("GET");
  });

  it("upserts a membership by email", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ email: "alice@example.com", role: "member" }),
      );
    vi.stubGlobal("fetch", fetchMock);

    const result = await upsertMembership("acme", {
      email: "alice@example.com",
      role: "member",
    });

    expect(result).toEqual({ email: "alice@example.com", role: "member" });
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/memberships");
    expect(req.method).toBe("PUT");
    expect(await req.clone().json()).toEqual({
      email: "alice@example.com",
      role: "member",
    });
  });

  it("removes a membership by user id", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(removeMembership("acme", "u1")).resolves.toBeUndefined();

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/memberships/u1");
    expect(req.method).toBe("DELETE");
  });
});
