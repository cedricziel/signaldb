import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { consentContext, submitConsentDecision } from "./consent";
import { ApiError } from "./http";
import { client } from "./gen/client.gen";

// See management.test.ts: the generated client needs an absolute base URL
// under jsdom's stricter Request parsing.
beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("consent API", () => {
  it("fetches the consent context for a client", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        client_name: "Claude",
        scopes: ["traces:read"],
        tenants: [{ id: "acme", name: "Acme Corp" }],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await consentContext("client-1");

    expect(result.client_name).toBe("Claude");
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("client_id=client-1");
    expect(req.method).toBe("GET");
  });

  it("submits a consent decision and returns the redirect URL", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ redirect: "https://client.example/callback?code=abc" }),
      );
    vi.stubGlobal("fetch", fetchMock);

    const redirect = await submitConsentDecision({
      approved: true,
      client_id: "client-1",
      redirect_uri: "https://client.example/callback",
      code_challenge: "challenge",
      code_challenge_method: "S256",
      tenant: "acme",
    });

    expect(redirect).toBe("https://client.example/callback?code=abc");
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toMatchObject({
      approved: true,
      client_id: "client-1",
      tenant: "acme",
    });
  });

  it("rejects with an ApiError carrying the error_description", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          { error: "access_denied", error_description: "no soup" },
          400,
        ),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(consentContext("client-1")).rejects.toMatchObject({
      name: "ApiError",
      status: 400,
      message: "no soup",
    });
    await expect(consentContext("client-1")).rejects.toBeInstanceOf(ApiError);
  });
});
