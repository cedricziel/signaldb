import { afterEach, describe, expect, it, vi } from "vitest";
import { resetApiClient, stubApiFetch } from "../test/apiClient";
import { ApiError } from "./http";
import {
  flattenAttrValue,
  tempoGetTrace,
  tempoSearch,
  tempoSearchTags,
} from "./tempo";

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
    const url = new URL(String(fn.mock.calls[0]?.[0]), "http://localhost");
    expect(url.pathname).toBe("/tempo/api/traces/abc");
    expect(url.searchParams.get("start")).toBe("1000");
    expect(url.searchParams.get("end")).toBe("2001");
    expect(url.searchParams.get("include_profiles")).toBe("true");
    expect(trace.traceId).toBe("abc");
    expect(trace.profiles).toEqual([]);
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

  it("maps span events, flattening exception attributes", async () => {
    mockFetchOnce({
      traceID: "abc",
      rootServiceName: "gateway",
      rootTraceName: "root-op",
      startTimeUnixNano: "1000",
      durationMs: 1,
      spanSets: [
        {
          matched: 1,
          spans: [
            {
              spanID: "s1",
              startTimeUnixNano: "1000",
              durationNanos: "2000",
              name: "root-op",
              status: "error",
              events: [
                {
                  name: "exception",
                  timeUnixNano: "1005",
                  attributes: {
                    "exception.message": {
                      key: "exception.message",
                      value: { stringValue: "boom" },
                    },
                    "exception.type": {
                      key: "exception.type",
                      value: { stringValue: "std::io::Error" },
                    },
                  },
                },
              ],
            },
          ],
        },
      ],
    });
    const trace = await tempoGetTrace("abc");
    expect(trace.spans[0]?.events).toEqual([
      {
        name: "exception",
        timeUnixNano: "1005",
        attributes: {
          "exception.message": "boom",
          "exception.type": "std::io::Error",
        },
      },
    ]);
  });

  it("defaults span events to an empty array when absent", async () => {
    mockFetchOnce({
      traceID: "abc",
      rootServiceName: "gateway",
      rootTraceName: "root-op",
      startTimeUnixNano: "1000",
      durationMs: 1,
      spanSets: [
        {
          matched: 1,
          spans: [
            {
              spanID: "s1",
              startTimeUnixNano: "1000",
              durationNanos: "2000",
            },
          ],
        },
      ],
    });
    const trace = await tempoGetTrace("abc");
    expect(trace.spans[0]?.events).toEqual([]);
  });

  it("treats an all-zero parent span id as a root", async () => {
    mockFetchOnce({
      traceID: "abc",
      rootServiceName: "gateway",
      rootTraceName: "root-op",
      startTimeUnixNano: "1000",
      durationMs: 1,
      spanSets: [
        {
          matched: 1,
          spans: [
            {
              spanID: "s1",
              parentSpanID: "0000000000000000",
              startTimeUnixNano: "1000",
              durationNanos: "2000",
              attributes: {
                "resource.env": {
                  key: "resource.env",
                  value: { stringValue: "prod" },
                },
              },
            },
          ],
        },
      ],
    });
    const trace = await tempoGetTrace("abc");
    expect(trace.spans[0]?.parentSpanId).toBeNull();
    expect(trace.rootAttributes).toEqual({ "resource.env": "prod" });
  });

  it("maps linked profile summaries onto their spans", async () => {
    mockFetchOnce({
      traceID: "abc",
      rootServiceName: "gateway",
      rootTraceName: "root-op",
      startTimeUnixNano: "1000",
      durationMs: 1,
      spanSets: [{ matched: 1, spans: [] }],
      profiles: [
        {
          profileID: "p1",
          timeUnixNano: "1000",
          durationNano: "2000",
          sampleType: "cpu",
          sampleUnit: "nanoseconds",
          serviceName: "gateway",
          spanID: "s1",
        },
        {
          profileID: "p2",
          timeUnixNano: "1500",
          durationNano: "500",
          sampleType: "alloc_space",
          sampleUnit: "bytes",
          serviceName: "gateway",
        },
      ],
    });
    const trace = await tempoGetTrace("abc");
    expect(trace.profiles).toEqual([
      {
        profileId: "p1",
        timeUnixNano: "1000",
        durationNano: "2000",
        sampleType: "cpu",
        sampleUnit: "nanoseconds",
        serviceName: "gateway",
        spanId: "s1",
      },
      {
        profileId: "p2",
        timeUnixNano: "1500",
        durationNano: "500",
        sampleType: "alloc_space",
        sampleUnit: "bytes",
        serviceName: "gateway",
        spanId: null,
      },
    ]);
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

// `tempoSearchTags` goes through the generated OpenAPI client (#1073), not
// the hand-written `tempoFetch` helper the tests above stub via a raw
// `fetch` mock — it needs the generated client's own test transport.
describe("tempoSearchTags", () => {
  afterEach(() => resetApiClient());

  it("passes unix-second window bounds and returns the tag names", async () => {
    const calls = stubApiFetch({
      tagNames: ["deployment.environment.name", "http.route", "service.name"],
    });
    const names = await tempoSearchTags(RANGE);
    const url = new URL(calls[0]!.url);
    expect(url.pathname).toBe("/tempo/api/search/tags");
    expect(url.searchParams.get("start")).toBe("1000");
    expect(url.searchParams.get("end")).toBe("2001");
    expect(names).toEqual([
      "deployment.environment.name",
      "http.route",
      "service.name",
    ]);
  });

  it("surfaces failures as ApiError with the status", async () => {
    stubApiFetch({ error: "forbidden" }, 403);
    await expect(tempoSearchTags(RANGE)).rejects.toBeInstanceOf(ApiError);
  });
});
