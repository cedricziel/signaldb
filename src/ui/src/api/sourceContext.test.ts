import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchSourceContext, fetchSourceContextAvailability } from "./sourceContext";
import { ApiError, setTenantContext } from "./http";
import { client } from "./gen/client.gen";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Same reasoning as api/github.test.ts: the generated client builds a
// `new Request(url, init)` before calling fetch, and the Node/undici
// `Request` rejects a relative URL, so tests need an absolute base.
beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  setTenantContext({ tenant: "", dataset: "" });
  client.setConfig({ baseUrl: "" });
});

describe("sourceContext API", () => {
  it("returns an available snippet", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        status: "available",
        snippet: {
          repository: "acme-org/api",
          ref: "main",
          path: "src/handler.rs",
          line: 42,
          start_line: 40,
          lines: ["fn handler() {", "    do_thing();", "}"],
          html_url: "https://github.com/acme-org/api/blob/main/src/handler.rs#L42",
          sha: "abc123",
        },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    setTenantContext({ tenant: "acme", dataset: "" });

    const result = await fetchSourceContext("acme", {
      path: "src/handler.rs",
      line: 42,
    });

    expect(result.status).toBe("available");
    expect(result.snippet?.repository).toBe("acme-org/api");
    expect(result.snippet?.lines).toHaveLength(3);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/tenants/acme/source-context");
    expect(req.method).toBe("POST");
  });

  it("returns an unavailable result with a reason", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ status: "unavailable", reason: "not_found" }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchSourceContext("acme", {
      path: "src/missing.rs",
      line: 1,
    });

    expect(result.status).toBe("unavailable");
    expect(result.reason).toBe("not_found");
    expect(result.snippet).toBeUndefined();
  });

  it("surfaces a 403 as an ApiError", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ error: "forbidden" }, 403),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      fetchSourceContext("acme", { path: "src/x.rs", line: 1 }),
    ).rejects.toThrow(ApiError);
    try {
      await fetchSourceContext("acme", { path: "src/x.rs", line: 1 });
      throw new Error("expected fetchSourceContext to throw");
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      expect((err as ApiError).status).toBe(403);
    }
  });
});

describe("sourceContextAvailability API", () => {
  it("reports whether GitHub is configured and linked for the tenant", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ configured: true, linked: true }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchSourceContextAvailability("acme");

    expect(result).toEqual({ configured: true, linked: true });
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/tenants/acme/source-context");
    expect(req.method).toBe("GET");
  });

  it("surfaces a 403 as an ApiError", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ error: "forbidden" }, 403),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchSourceContextAvailability("acme")).rejects.toThrow(
      ApiError,
    );
  });
});
