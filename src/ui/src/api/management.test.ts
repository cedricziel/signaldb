import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createApiKey,
  createDataset,
  createTenant,
  deleteDataset,
  getSchema,
  listApiKeys,
  listMemberships,
  listTables,
  provisionTables,
  removeMembership,
  revokeApiKey,
  updateApiKey,
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
          dataset_ids: ["prod"],
          scopes: ["metrics:write"],
        }),
        { status: 201, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    setTenantContext({ tenant: "acme", dataset: "prod" });

    const result = await createApiKey("acme", {
      name: "collector",
      dataset_ids: ["prod"],
      scopes: ["metrics:write"],
    });

    expect(result.key).toBe("sdbk_secret");

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/api-keys");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual({
      name: "collector",
      dataset_ids: ["prod"],
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

  it("creates a key carrying schema scopes", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(
        {
          id: "key-2",
          key: "sdbk_schema",
          scopes: ["schema:read", "schema:write"],
        },
        201,
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await createApiKey("acme", { scopes: ["schema:read", "schema:write"] });

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(await req.clone().json()).toEqual({
      scopes: ["schema:read", "schema:write"],
    });
  });

  it("updates a live key's scopes and dataset restriction via PATCH", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        id: "key-1",
        name: "collector",
        dataset_ids: ["prod"],
        scopes: ["schema:read", "traces:write"],
        revoked: false,
        created_at: "2026-08-01T00:00:00Z",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await updateApiKey("acme", "key-1", {
      scopes: ["schema:read", "traces:write"],
      dataset_ids: ["prod"],
    });
    expect(result.scopes).toEqual(["schema:read", "traces:write"]);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/tenants/acme/api-keys/key-1");
    expect(req.method).toBe("PATCH");
    expect(await req.clone().json()).toEqual({
      scopes: ["schema:read", "traces:write"],
      dataset_ids: ["prod"],
    });
  });

  it("clears a live key's dataset restriction via clear_dataset_restriction", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        id: "key-1",
        name: "collector",
        dataset_ids: null,
        scopes: ["schema:read"],
        revoked: false,
        created_at: "2026-08-01T00:00:00Z",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await updateApiKey("acme", "key-1", {
      scopes: ["schema:read"],
      clear_dataset_restriction: true,
    });

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(await req.clone().json()).toEqual({
      scopes: ["schema:read"],
      clear_dataset_restriction: true,
    });
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

  it("fetches the logical and physical schema", async () => {
    const body = {
      logical_schema_version: "otel-2026-08",
      logical: [
        {
          source: "traces",
          level: null,
          name: "span.name",
          value_type: "string",
          filterability: "filterable",
          kind: "record_metadata",
          non_native: false,
        },
      ],
      physical: [
        {
          source: "traces",
          version: "physical-v2",
          is_current: true,
          description: "d",
          partition_by: ["timestamp"],
          fields: [],
        },
      ],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(body));
    vi.stubGlobal("fetch", fetchMock);

    const result = await getSchema();

    expect(result).toEqual(body);
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/manage/schema");
    expect(req.method).toBe("GET");
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

  it("lists a tenant's provisioned signal tables", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        tenant_id: "acme",
        tables: [{ name: "traces", schema_type: "traces", description: "x" }],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await listTables("acme");

    expect(result.tables).toEqual([
      { name: "traces", schema_type: "traces", description: "x" },
    ]);
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/tenants/acme/tables");
    expect(req.method).toBe("GET");
  });

  it("provisions a tenant's enabled signal tables", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          {
            message: "Default tables created for tenant 'acme'",
            tenant_id: "acme",
          },
          201,
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    const result = await provisionTables("acme");

    expect(result.tenant_id).toBe("acme");
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/tenants/acme/tables/create");
    expect(req.method).toBe("POST");
  });
});
