import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { connectionInfo } from "./connection";
import { ApiError, setTenantContext } from "./http";
import { client } from "./gen/client.gen";
import { connectionInfoBody } from "../test/connectionInfo";

// The generated client builds a `new Request(url, init)` before calling
// fetch. In the browser a same-origin base URL ("") resolves against the
// document origin, but the Node/undici `Request` in the test environment
// rejects a relative URL — give the client an absolute base for the
// duration of the tests (see management.test.ts's precedent).
beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
  client.setConfig({ baseUrl: "" });
});

describe("connectionInfo", () => {
  it("returns the parsed response and attaches tenant headers", async () => {
    const body = connectionInfoBody();
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    setTenantContext({ tenant: "acme", dataset: "production" });

    const res = await connectionInfo();
    expect(res).toEqual(body);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/connection");
    expect(req.headers.get("X-Tenant-ID")).toBe("acme");
    expect(req.headers.get("X-Dataset-ID")).toBe("production");
  });

  it("throws an ApiError carrying the status when unavailable", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify({}), { status: 404 })),
    );

    const err = await connectionInfo().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(404);
  });
});
