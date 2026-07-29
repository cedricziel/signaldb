import { afterEach, describe, expect, it, vi } from "vitest";
import { createApiKey, revokeApiKey } from "./management";
import { setTenantContext } from "./http";

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
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
    expect(String(fetchMock.mock.calls[0]?.[0])).toContain(
      "/tenants/acme/api-keys",
    );
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      name: "collector",
      dataset_id: "prod",
      scopes: ["metrics:write"],
    });
    expect(init.headers).toMatchObject({ "X-Tenant-ID": "acme" });
  });

  it("revokes only the selected tenant key", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    await revokeApiKey("acme", "key-1");
    expect(String(fetchMock.mock.calls[0]?.[0])).toBe(
      "/api/v1/manage/tenants/acme/api-keys/key-1",
    );
    expect((fetchMock.mock.calls[0]?.[1] as RequestInit).method).toBe("DELETE");
  });
});
