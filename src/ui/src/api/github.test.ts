import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  listGithubInstallations,
  removeGithubInstallation,
  startGithubLink,
} from "./github";
import { ApiError, setTenantContext } from "./http";
import { client } from "./gen/client.gen";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Same reasoning as api/management.test.ts: the generated client builds a
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

describe("github API", () => {
  it("lists installations, including when GitHub is unconfigured", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        configured: true,
        app_slug: "signaldb",
        installations: [
          {
            installation_id: 42,
            account_login: "acme-org",
            account_type: "Organization",
            repositories: ["acme-org/api", "acme-org/web"],
            repositories_synced_at: "2026-09-01T00:00:00Z",
            stale: false,
            linked_by_github_login: "alice",
            manage_url: "https://github.com/organizations/acme-org/settings/installations/42",
            created_at: "2026-08-01T00:00:00Z",
            updated_at: "2026-09-01T00:00:00Z",
          },
        ],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    setTenantContext({ tenant: "acme", dataset: "prod" });

    const result = await listGithubInstallations("acme");

    expect(result.configured).toBe(true);
    expect(result.installations).toHaveLength(1);
    expect(result.installations[0]?.account_login).toBe("acme-org");

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain(
      "/api/v1/tenants/acme/github-installations",
    );
    expect(req.method).toBe("GET");
    expect(req.headers.get("X-Tenant-ID")).toBe("acme");
  });

  it("starts a link and returns the install URL", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse(
        {
          install_url: "https://github.com/apps/signaldb/installations/new?state=abc",
          expires_at: "2026-09-01T00:10:00Z",
        },
        201,
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await startGithubLink("acme");

    expect(result.install_url).toContain("state=abc");
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain(
      "/api/v1/tenants/acme/github-installations/link",
    );
    expect(req.method).toBe("POST");
  });

  it("removes an installation", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      removeGithubInstallation("acme", 42),
    ).resolves.toBeUndefined();

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain(
      "/api/v1/tenants/acme/github-installations/42",
    );
    expect(req.method).toBe("DELETE");
  });

  it("surfaces a 403 as an ApiError", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ error: "forbidden" }, 403),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(listGithubInstallations("acme")).rejects.toThrow(ApiError);
    try {
      await listGithubInstallations("acme");
      throw new Error("expected listGithubInstallations to throw");
    } catch (err) {
      expect(err).toBeInstanceOf(ApiError);
      expect((err as ApiError).status).toBe(403);
    }
  });
});
