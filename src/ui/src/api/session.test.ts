import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { client } from "./gen/client.gen";
import { ApiError, setTenantContext } from "./http";
import {
  createSession,
  currentSession,
  deleteSession,
  loginConfig,
  whoami,
} from "./session";

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

// The generated client needs an absolute base URL under jsdom's stricter
// Request parsing (see management.test.ts / consent.test.ts).
beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
  client.setConfig({ baseUrl: "" });
});

function sentRequest(fn: ReturnType<typeof vi.fn>): Request {
  return fn.mock.calls[0]?.[0] as Request;
}

describe("createSession", () => {
  const RESULT = { tenant: "acme", dataset: "prod", memberships: [] };

  it("POSTs user credentials and returns the resolved context", async () => {
    const fn = mockFetchOnce(RESULT);
    await expect(
      createSession({
        email: "alice@example.com",
        password: "secret",
        tenant: "acme",
        dataset: "prod",
      }),
    ).resolves.toEqual(RESULT);
    const req = sentRequest(fn);
    expect(new URL(req.url).pathname).toBe("/ui/session");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual({
      email: "alice@example.com",
      password: "secret",
      tenant: "acme",
      dataset: "prod",
    });
  });

  it("omits the dataset field when not provided", async () => {
    const fn = mockFetchOnce(RESULT);
    await createSession({
      email: "alice@example.com",
      password: "secret",
      tenant: "acme",
    });
    expect(await sentRequest(fn).clone().json()).toEqual({
      email: "alice@example.com",
      password: "secret",
      tenant: "acme",
    });
  });

  it("throws the server's error message with the status on failure", async () => {
    mockFetchOnce({ error: "Invalid email or password" }, 401);
    const err = await createSession({
      email: "alice@example.com",
      password: "bad",
      tenant: "acme",
    }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(401);
    expect((err as ApiError).message).toBe("Invalid email or password");
  });

  it("falls back to a generic message on non-JSON error bodies", async () => {
    const fn = vi.fn().mockResolvedValue(new Response("boom", { status: 500 }));
    vi.stubGlobal("fetch", fn);
    await expect(
      createSession({ email: "a@b.test", password: "x", tenant: "t" }),
    ).rejects.toThrow(/Login failed \(500\)/);
  });
});

describe("deleteSession", () => {
  it("sends DELETE /ui/session", async () => {
    const fn = mockFetchOnce(null, 204);
    await deleteSession();
    const req = sentRequest(fn);
    expect(new URL(req.url).pathname).toBe("/ui/session");
    expect(req.method).toBe("DELETE");
  });

  it("throws on failure", async () => {
    mockFetchOnce({}, 500);
    await expect(deleteSession()).rejects.toThrow(/Logout failed \(500\)/);
  });
});

describe("whoami", () => {
  const BODY = {
    user: {
      id: "user-1",
      email: "alice@example.com",
      display_name: "Alice",
      is_instance_admin: false,
    },
    memberships: [{ tenant_id: "acme", role: "admin" }],
    tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
    user_id: "user-1",
    dataset: "production",
    datasets: [
      { id: "production", slug: "production", is_default: true },
      { id: "staging", slug: "staging", is_default: false },
    ],
    default_dataset: "production",
    granted_tenants: [{ tenant_id: "acme" }],
  };

  it("returns the parsed response and attaches tenant headers", async () => {
    const fn = mockFetchOnce(BODY);
    setTenantContext({ tenant: "acme", dataset: "prod" });
    await expect(whoami()).resolves.toEqual(BODY);
    const req = sentRequest(fn);
    expect(new URL(req.url).pathname).toBe("/api/v1/whoami");
    expect(req.headers.get("X-Tenant-ID")).toBe("acme");
    expect(req.headers.get("X-Dataset-ID")).toBe("prod");
  });

  it("scopes to an explicit tenant without the current context's dataset", async () => {
    const fn = mockFetchOnce(BODY);
    setTenantContext({ tenant: "globex", dataset: "main" });
    await whoami("acme");
    const req = sentRequest(fn);
    expect(req.headers.get("X-Tenant-ID")).toBe("acme");
    expect(req.headers.get("X-Dataset-ID")).toBeNull();
  });

  it("throws an ApiError carrying the status when unavailable", async () => {
    mockFetchOnce({}, 404);
    const err = await whoami().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(404);
  });
});

describe("loginConfig / currentSession", () => {
  it("loginConfig() returns the probe response", async () => {
    mockFetchOnce({ password_enabled: true, oidc: null });
    await expect(loginConfig()).resolves.toEqual({
      password_enabled: true,
      oidc: null,
    });
  });

  it("loginConfig() throws ApiError(404) on an older router", async () => {
    mockFetchOnce({}, 404);
    const err = await loginConfig().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(404);
  });

  it("loginConfig() throws ApiError(500) on a server error", async () => {
    mockFetchOnce({}, 500);
    const err = await loginConfig().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(500);
  });

  it("currentSession() returns the introspection response", async () => {
    const body = {
      user: {
        id: "user-1",
        email: "alice@example.com",
        display_name: "Alice",
        is_instance_admin: false,
      },
      tenant: "acme",
      dataset: "prod",
      memberships: [{ tenant_id: "acme", name: "Acme", role: "admin" }],
    };
    mockFetchOnce(body);
    await expect(currentSession()).resolves.toEqual(body);
  });

  it("currentSession() throws ApiError(401) without a session cookie", async () => {
    mockFetchOnce({}, 401);
    const err = await currentSession().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(401);
  });
});
