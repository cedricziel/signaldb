import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runIrQuery } from "./queryIr";
import { ApiError } from "./http";
import { client } from "./gen/client.gen";
import type { QueryIrRequest } from "./gen";

beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

const DOC: QueryIrRequest = {
  from: "logs",
  irVersion: 1,
  range: { from: "now-1h", to: "now" },
  result: "rows",
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("runIrQuery", () => {
  // `client-retry-on-throttle`: a 429 that survives the retry budget reaches
  // the panel as an ApiError whose message names the server-stated wait, so
  // per-panel `error.message` renderers show throttling, not "failed".
  it("reports an exhausted 429 as a throttling message naming the wait", async () => {
    // Retry-After beyond the per-attempt cap: the client fails fast, so no
    // timers are involved.
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ error: "rate_limited" }), {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": "3600",
        },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const err = await runIrQuery(DOC).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as ApiError).status).toBe(429);
    expect((err as ApiError).retryAfterMs).toBe(3_600_000);
    expect((err as ApiError).message).toBe(
      "Rate limited — server asked to retry in 3600 s",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("submits the IR document and returns the response envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ result: "rows", columns: [], rows: [] }),
      );
    vi.stubGlobal("fetch", fetchMock);

    const result = await runIrQuery(DOC);

    expect(result).toEqual({ result: "rows", columns: [], rows: [] });
    const req = fetchMock.mock.calls[0]?.[0] as Request;
    expect(req.url).toContain("/api/v1/query");
    expect(req.method).toBe("POST");
    expect(await req.clone().json()).toEqual(DOC);
  });

  it("rejects with an ApiError on a failed request", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ error: "invalid IR" }, 400));
    vi.stubGlobal("fetch", fetchMock);

    await expect(runIrQuery(DOC)).rejects.toBeInstanceOf(ApiError);
    await expect(runIrQuery(DOC)).rejects.toMatchObject({ status: 400 });
  });
});
