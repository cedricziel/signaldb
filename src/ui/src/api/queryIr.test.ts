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
