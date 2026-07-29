import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, setTenantContext } from "./http";
import { createSession, deleteSession, whoami } from "./session";

function mockFetchOnce(body: unknown, status = 200) {
  const fn = vi.fn().mockResolvedValue(
    status === 204
      ? new Response(null, { status })
      : new Response(JSON.stringify(body), {
          status,
          headers: { "Content-Type": "application/json" },
        }),
  );
  vi.stubGlobal("fetch", fn);
  return fn;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("createSession", () => {
  it("POSTs the credentials as JSON and resolves on 204", async () => {
    const fn = mockFetchOnce(null, 204);
    await createSession({ apiKey: "sk-1", tenant: "acme", dataset: "prod" });
    expect(String(fn.mock.calls[0]?.[0])).toBe("/ui/session");
    const init = fn.mock.calls[0]?.[1] as RequestInit;
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      api_key: "sk-1",
      tenant: "acme",
      dataset: "prod",
    });
  });

  it("omits the dataset field when not provided", async () => {
    const fn = mockFetchOnce(null, 204);
    await createSession({ apiKey: "sk-1", tenant: "acme" });
    const init = fn.mock.calls[0]?.[1] as RequestInit;
    expect(JSON.parse(String(init.body))).toEqual({
      api_key: "sk-1",
      tenant: "acme",
    });
  });

  it("throws the server's error message with the status on failure", async () => {
    mockFetchOnce({ error: "Invalid or revoked API key" }, 401);
    const err = await createSession({ apiKey: "bad", tenant: "acme" }).catch(
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(401);
    expect((err as ApiError).message).toBe("Invalid or revoked API key");
  });

  it("falls back to a generic message on non-JSON error bodies", async () => {
    const fn = vi.fn().mockResolvedValue(new Response("boom", { status: 500 }));
    vi.stubGlobal("fetch", fn);
    await expect(createSession({ apiKey: "k", tenant: "t" })).rejects.toThrow(
      /Login failed \(500\)/,
    );
  });
});

describe("deleteSession", () => {
  it("sends DELETE /ui/session", async () => {
    const fn = mockFetchOnce(null, 204);
    await deleteSession();
    expect(String(fn.mock.calls[0]?.[0])).toBe("/ui/session");
    expect((fn.mock.calls[0]?.[1] as RequestInit).method).toBe("DELETE");
  });

  it("throws on failure", async () => {
    mockFetchOnce({}, 500);
    await expect(deleteSession()).rejects.toThrow(/Logout failed \(500\)/);
  });
});

describe("whoami", () => {
  const BODY = {
    tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
    datasets: [
      { id: "production", slug: "production", is_default: true },
      { id: "staging", slug: "staging", is_default: false },
    ],
    default_dataset: "production",
  };

  it("returns the parsed response and attaches tenant headers", async () => {
    const fn = mockFetchOnce(BODY);
    setTenantContext({ tenant: "acme", dataset: "prod" });
    try {
      const res = await whoami();
      expect(res).toEqual(BODY);
    } finally {
      setTenantContext({ tenant: "", dataset: "" });
    }
    expect(String(fn.mock.calls[0]?.[0])).toBe("/api/v1/whoami");
    const init = fn.mock.calls[0]?.[1] as RequestInit;
    expect(init.headers).toMatchObject({
      "X-Tenant-ID": "acme",
      "X-Dataset-ID": "prod",
    });
  });

  it("throws an ApiError carrying the status when unavailable", async () => {
    mockFetchOnce({}, 404);
    const err = await whoami().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(404);
  });
});
