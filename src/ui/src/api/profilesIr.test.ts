import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchFlamegraph, fetchFlamegraphById } from "./profilesIr";
import { ApiError } from "./http";
import { client } from "./gen/client.gen";

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

const FLAMEGRAPH = {
  result: "flamegraph",
  window: { start_ns: 0, end_ns: 0 },
  flamegraph: {
    names: ["total", "main"],
    levels: [
      [0, 10, 0, 0],
      [0, 10, 10, 1],
    ],
    total: 10,
    max_self: 10,
    truncated: false,
  },
};

describe("fetchFlamegraph", () => {
  it("builds where-stages for service, sample type, and an attribute matcher", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(FLAMEGRAPH));
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchFlamegraph(
      { fromMs: 0, toMs: 60_000 },
      {
        service: "checkout",
        sampleType: "cpu",
        matcher: { label: "region", value: "eu" },
      },
    );

    expect(result.render.flamebearer.names).toEqual(["total", "main"]);
    expect(result.render.flamebearer.numTicks).toBe(10);
    expect(result.truncated).toBe(false);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body.from).toBe("profiles");
    expect(body.result).toBe("flamegraph");
    expect(body.pipeline).toEqual([
      { where: { field: "service.name", op: "eq", value: "checkout" } },
      { where: { field: "sample.type", op: "eq", value: "cpu" } },
      { where: { field: "region", op: "eq", value: "eu" } },
    ]);
  });

  it("omits where-stages that weren't asked for", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(FLAMEGRAPH));
    vi.stubGlobal("fetch", fetchMock);

    await fetchFlamegraph({ fromMs: 0, toMs: 60_000 }, {});

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body.pipeline).toEqual([]);
  });

  it("surfaces a truncated result", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ...FLAMEGRAPH,
        flamegraph: { ...FLAMEGRAPH.flamegraph, truncated: true },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchFlamegraph({ fromMs: 0, toMs: 60_000 }, {});
    expect(result.truncated).toBe(true);
  });

  it("rejects when the response carries no flamegraph envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({
          result: "flamegraph",
          window: { start_ns: 0, end_ns: 0 },
        }),
      );
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      fetchFlamegraph({ fromMs: 0, toMs: 60_000 }, {}),
    ).rejects.toBeInstanceOf(ApiError);
  });
});

describe("fetchFlamegraphById", () => {
  it("filters on profile.id over a 30-day default window", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(FLAMEGRAPH));
    vi.stubGlobal("fetch", fetchMock);

    const render = await fetchFlamegraphById("abc123");
    expect(render.flamebearer.names).toEqual(["total", "main"]);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body.range).toEqual({ from: "now-30d", to: "now" });
    expect(body.pipeline).toEqual([
      { where: { field: "profile.id", op: "eq", value: "abc123" } },
    ]);
  });

  it("treats a zero-match flamegraph as not found", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ...FLAMEGRAPH,
        flamegraph: {
          names: [],
          levels: [],
          total: 0,
          max_self: 0,
          truncated: false,
        },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchFlamegraphById("missing")).rejects.toMatchObject({
      status: 404,
    });
  });
});
