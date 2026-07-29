import { afterEach, describe, expect, it, vi } from "vitest";
import { flattenAttrValue, tempoGetTrace, tempoSearch } from "./tempo";

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

describe("tempoGetTrace", () => {
  it("maps the wire format to flat spans", async () => {
    const fn = mockFetchOnce({
      traceID: "abc",
      rootServiceName: "gateway",
      rootTraceName: "POST /checkout",
      startTimeUnixNano: "1000",
      durationMs: 412,
      spanSets: [
        {
          matched: 2,
          spans: [
            {
              spanID: "s1",
              startTimeUnixNano: "1000",
              durationNanos: "2000",
              name: "root-op",
              serviceName: "gateway",
              status: "ok",
              attributes: {
                "http.method": {
                  key: "http.method",
                  value: { stringValue: "POST" },
                },
                "http.status_code": {
                  key: "http.status_code",
                  value: { intValue: 200 },
                },
              },
            },
            {
              spanID: "s2",
              startTimeUnixNano: "1100",
              durationNanos: "500",
              parentSpanID: "s1",
              status: "error",
            },
          ],
        },
      ],
    });
    const trace = await tempoGetTrace("abc", RANGE);
    expect(String(fn.mock.calls[0]?.[0])).toContain(
      "/tempo/api/traces/abc?start=1000&end=2001",
    );
    expect(trace.traceId).toBe("abc");
    expect(trace.spans).toHaveLength(2);
    expect(trace.spans[0]).toMatchObject({
      spanId: "s1",
      parentSpanId: null,
      name: "root-op",
      serviceName: "gateway",
      attributes: { "http.method": "POST", "http.status_code": 200 },
    });
    expect(trace.spans[1]).toMatchObject({
      spanId: "s2",
      parentSpanId: "s1",
      status: "error",
    });
  });

  it("throws a readable error on failure", async () => {
    mockFetchOnce({ error: "trace not found" }, 404);
    await expect(tempoGetTrace("missing")).rejects.toThrow(
      /traces\/missing failed \(404\)/,
    );
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
      },
    ]);
  });

  it("tolerates an empty result", async () => {
    mockFetchOnce({ metrics: {} });
    expect(await tempoSearch(RANGE, 10)).toEqual([]);
  });
});
