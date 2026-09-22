import { afterEach, describe, expect, it } from "vitest";
import { promQueryRange, seriesName } from "./prom";
import { resetApiClient, stubApiFetch } from "../test/apiClient";

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

afterEach(() => {
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
