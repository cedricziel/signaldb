import { afterEach, describe, expect, it, vi } from "vitest";
import {
  promLabelNames,
  promLabelStats,
  promLabelValues,
  promMetricNames,
  promQueryRange,
  seriesName,
} from "./prom";
import { resetApiClient, stubApiFetch } from "../test/apiClient";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

// promLabelStats stays on raw `fetch` (not yet in the OpenAPI document, see
// prom.ts), so it keeps stubbing the global directly.
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
  resetApiClient();
});

describe("promQueryRange", () => {
  it("maps matrix results to millisecond points", async () => {
    const calls = stubApiFetch({
      status: "success",
      data: {
        resultType: "matrix",
        result: [
          {
            metric: { __name__: "up", service_name: "checkout" },
            values: [
              [1000, "1"],
              [1060, "0.5"],
            ],
          },
        ],
      },
    });
    const series = await promQueryRange("up", RANGE, 60);
    expect(series).toEqual([
      {
        labels: { __name__: "up", service_name: "checkout" },
        points: [
          [1_000_000, 1],
          [1_060_000, 0.5],
        ],
      },
    ]);
    const url = calls[0]!.url;
    expect(url).toContain("/prometheus/api/v1/query_range?");
    expect(url).toContain("step=60");
    expect(url).toContain("start=1000");
    expect(url).toContain("end=2000");
  });

  it("throws the API error message on status=error", async () => {
    stubApiFetch({
      status: "error",
      error: "parse error at char 3",
      data: { resultType: "matrix", result: [] },
    });
    await expect(promQueryRange("up{", RANGE, 60)).rejects.toThrow(
      /parse error at char 3/,
    );
  });

  it("throws on HTTP failure", async () => {
    stubApiFetch({ oops: true }, 500);
    await expect(promQueryRange("up", RANGE, 60)).rejects.toThrow(/\(500\)/);
  });

  it("rejects non-matrix results", async () => {
    stubApiFetch({
      status: "success",
      data: { resultType: "vector", result: [] },
    });
    await expect(promQueryRange("up", RANGE, 60)).rejects.toThrow(/vector/);
  });
});

describe("seriesName", () => {
  it("formats name and sorted labels", () => {
    expect(seriesName({ service_name: "b", __name__: "up", env: "a" })).toBe(
      'up{env="a", service_name="b"}',
    );
  });

  it("handles missing name and empty labels", () => {
    expect(seriesName({ __name__: "up" })).toBe("up");
    expect(seriesName({})).toBe("value");
    expect(seriesName({ a: "1" })).toBe('{a="1"}');
  });
});

describe("metadata pickers", () => {
  it("promLabelNames returns the data array and sends the window", async () => {
    const calls = stubApiFetch({
      status: "success",
      data: ["service", "host"],
    });
    await expect(promLabelNames(RANGE)).resolves.toEqual(["service", "host"]);
    const url = calls[0]!.url;
    expect(url).toContain("/prometheus/api/v1/labels?");
    expect(url).toContain("start=1000");
    expect(url).toContain("end=2000");
  });

  it("promLabelValues encodes the label name in the path", async () => {
    const calls = stubApiFetch({ status: "success", data: ["checkout"] });
    await expect(promLabelValues("k8s.pod", RANGE)).resolves.toEqual([
      "checkout",
    ]);
    expect(calls[0]!.url).toContain("/prometheus/api/v1/label/k8s.pod/values?");
  });

  it("promMetricNames queries the reserved __name__ label", async () => {
    const calls = stubApiFetch({
      status: "success",
      data: ["up", "http_reqs"],
    });
    await expect(promMetricNames(RANGE)).resolves.toEqual(["up", "http_reqs"]);
    expect(calls[0]!.url).toContain(
      "/prometheus/api/v1/label/__name__/values?",
    );
  });

  it("defaults to an empty list when data is absent", async () => {
    stubApiFetch({ status: "success" });
    await expect(promLabelNames(RANGE)).resolves.toEqual([]);
  });

  it("throws on HTTP failure", async () => {
    // 500 is not retried by `retryingFetch`; a 503 on GET would back off first.
    stubApiFetch("nope", 500);
    await expect(promLabelNames(RANGE)).rejects.toThrow(/\(500\)/);
  });
});

describe("promLabelStats", () => {
  it("returns the stats array and hits the label_stats endpoint", async () => {
    const fn = mockFetchOnce({
      status: "success",
      data: [
        { name: "service", distinct_estimate: 12, presence: 1, capped: false },
        {
          name: "k8s.pod",
          distinct_estimate: 10000,
          presence: 0.9,
          capped: true,
        },
      ],
    });
    const stats = await promLabelStats(RANGE);
    expect(stats).toHaveLength(2);
    expect(stats[1]).toMatchObject({ name: "k8s.pod", capped: true });
    const url = String(fn.mock.calls[0]?.[0]);
    expect(url).toContain("/prometheus/api/v1/label_stats?");
    expect(url).toContain("start=1000");
  });

  it("defaults to an empty list when data is absent", async () => {
    mockFetchOnce({ status: "success" });
    await expect(promLabelStats(RANGE)).resolves.toEqual([]);
  });

  it("throws on HTTP failure", async () => {
    mockFetchOnce("nope", 500);
    await expect(promLabelStats(RANGE)).rejects.toThrow(/\(500\)/);
  });
});
