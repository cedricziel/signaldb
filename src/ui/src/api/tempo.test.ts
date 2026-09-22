import { afterEach, describe, expect, it, vi } from "vitest";
import { flattenAttrValue, tempoSearch } from "./tempo";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_500 };

function mockFetchOnce(body: unknown, status = 200) {
  const fn = vi.fn().mockResolvedValue(
    new Response(JSON.stringify(body), {
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

describe("flattenAttrValue", () => {
  it("unwraps every value variant", () => {
    expect(flattenAttrValue({ stringValue: "x" })).toBe("x");
    expect(flattenAttrValue({ intValue: 7 })).toBe(7);
    expect(flattenAttrValue({ doubleValue: 1.5 })).toBe(1.5);
    expect(flattenAttrValue({ boolValue: false })).toBe(false);
    expect(flattenAttrValue({})).toBe("");
  });
});

describe("tempoSearch", () => {
  it("maps summaries and passes unix-second bounds", async () => {
    const fn = mockFetchOnce({
      traces: [
        {
          traceID: "t1",
          rootServiceName: "checkout",
          rootTraceName: "GET /cart",
          startTimeUnixNano: "5000",
          durationMs: 88,
        },
      ],
      metrics: {},
    });
    const out = await tempoSearch(RANGE, 25);
    const url = String(fn.mock.calls[0]?.[0]);
    expect(url).toContain("/tempo/api/search?");
    expect(url).toContain("start=1000");
    expect(url).toContain("end=2001");
    expect(url).toContain("limit=25");
    expect(out).toEqual([
      {
        traceId: "t1",
        rootServiceName: "checkout",
        rootTraceName: "GET /cart",
        startNs: "5000",
        durationMs: 88,
        rootAttributes: {},
        rootError: false,
      },
    ]);
  });

  it("flags traces whose root span errored", async () => {
    mockFetchOnce({
      traces: [
        {
          traceID: "t1",
          rootServiceName: "checkout",
          rootTraceName: "GET /cart",
          startTimeUnixNano: "5000",
          durationMs: 88,
          spanSets: [
            {
              matched: 1,
              spans: [
                {
                  spanID: "root",
                  startTimeUnixNano: "5000",
                  durationNanos: "10",
                  status: "error",
                },
              ],
            },
          ],
        },
      ],
      metrics: {},
    });
    const out = await tempoSearch(RANGE, 25);
    expect(out[0]?.rootError).toBe(true);
  });

  it("extracts the root span's attributes from spanSets", async () => {
    mockFetchOnce({
      traces: [
        {
          traceID: "t1",
          rootServiceName: "checkout",
          rootTraceName: "GET /cart",
          startTimeUnixNano: "5000",
          durationMs: 88,
          spanSets: [
            {
              matched: 2,
              spans: [
                {
                  spanID: "child",
                  parentSpanID: "root",
                  startTimeUnixNano: "5100",
                  durationNanos: "10",
                  attributes: {
                    ignored: { key: "ignored", value: { stringValue: "x" } },
                  },
                },
                {
                  spanID: "root",
                  startTimeUnixNano: "5000",
                  durationNanos: "88000000",
                  attributes: {
                    "resource.host.name": {
                      key: "resource.host.name",
                      value: { stringValue: "web-1" },
                    },
                  },
                },
              ],
            },
          ],
        },
      ],
      metrics: {},
    });
    const out = await tempoSearch(RANGE, 25);
    expect(out[0]?.rootAttributes).toEqual({ "resource.host.name": "web-1" });
  });

  it("tolerates an empty result", async () => {
    mockFetchOnce({ metrics: {} });
    expect(await tempoSearch(RANGE, 10)).toEqual([]);
  });
});
