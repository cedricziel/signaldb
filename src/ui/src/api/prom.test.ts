import { afterEach, describe, expect, it, vi } from "vitest";
import {
  promLabelNames,
  promLabelValues,
  promMetricNames,
  promQueryRange,
  seriesName,
} from "./prom";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

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

describe("promQueryRange", () => {
  it("maps matrix results to millisecond points", async () => {
    const fn = mockFetchOnce({
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
    const url = String(fn.mock.calls[0]?.[0]);
    expect(url).toContain("/prometheus/api/v1/query_range?");
    expect(url).toContain("step=60");
    expect(url).toContain("start=1000");
    expect(url).toContain("end=2000");
  });

  it("throws the API error message on status=error", async () => {
    mockFetchOnce({
      status: "error",
      error: "parse error at char 3",
      data: { resultType: "matrix", result: [] },
    });
    await expect(promQueryRange("up{", RANGE, 60)).rejects.toThrow(
      /parse error at char 3/,
    );
  });

  it("throws on HTTP failure", async () => {
    mockFetchOnce({ oops: true }, 500);
    await expect(promQueryRange("up", RANGE, 60)).rejects.toThrow(/\(500\)/);
  });

  it("rejects non-matrix results", async () => {
    mockFetchOnce({
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
    const fn = mockFetchOnce({ status: "success", data: ["service", "host"] });
    await expect(promLabelNames(RANGE)).resolves.toEqual(["service", "host"]);
    const url = String(fn.mock.calls[0]?.[0]);
    expect(url).toContain("/prometheus/api/v1/labels?");
    expect(url).toContain("start=1000");
    expect(url).toContain("end=2000");
  });

  it("promLabelValues encodes the label name in the path", async () => {
    const fn = mockFetchOnce({ status: "success", data: ["checkout"] });
    await expect(promLabelValues("k8s.pod", RANGE)).resolves.toEqual([
      "checkout",
    ]);
    expect(String(fn.mock.calls[0]?.[0])).toContain(
      "/prometheus/api/v1/label/k8s.pod/values?",
    );
  });

  it("promMetricNames queries the reserved __name__ label", async () => {
    const fn = mockFetchOnce({ status: "success", data: ["up", "http_reqs"] });
    await expect(promMetricNames(RANGE)).resolves.toEqual(["up", "http_reqs"]);
    expect(String(fn.mock.calls[0]?.[0])).toContain(
      "/prometheus/api/v1/label/__name__/values?",
    );
  });

  it("defaults to an empty list when data is absent", async () => {
    mockFetchOnce({ status: "success" });
    await expect(promLabelNames(RANGE)).resolves.toEqual([]);
  });

  it("throws on HTTP failure", async () => {
    mockFetchOnce("nope", 503);
    await expect(promLabelNames(RANGE)).rejects.toThrow(/\(503\)/);
  });
});
