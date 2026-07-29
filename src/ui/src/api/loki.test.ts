import { afterEach, describe, expect, it, vi } from "vitest";
import {
  lokiLabels,
  lokiLabelValues,
  lokiQueryHistogram,
  lokiQueryLogs,
} from "./loki";

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

describe("lokiQueryLogs", () => {
  it("flattens streams and sorts rows newest-first", async () => {
    mockFetchOnce({
      status: "success",
      data: {
        resultType: "streams",
        result: [
          {
            stream: { service_name: "checkout", level: "info" },
            values: [["1000000000", "older line"]],
          },
          {
            stream: { service_name: "payments", level: "error" },
            values: [["3000000000", "newest line"]],
          },
        ],
      },
    });
    const rows = await lokiQueryLogs("{}", RANGE, 100);
    expect(rows.map((r) => r.line)).toEqual(["newest line", "older line"]);
    expect(rows[0]?.labels).toEqual({
      service_name: "payments",
      level: "error",
    });
    expect(rows[0]?.tsMs).toBe(3000);
  });

  it("sends nanosecond bounds and backward direction", async () => {
    const fn = mockFetchOnce({
      status: "success",
      data: { resultType: "streams", result: [] },
    });
    await lokiQueryLogs('{x="y"}', RANGE, 250);
    const url = String(fn.mock.calls[0]?.[0]);
    expect(url).toContain("/loki/api/v1/query_range?");
    expect(url).toContain("start=1000000000000");
    expect(url).toContain("end=2000000000000");
    expect(url).toContain("direction=backward");
    expect(url).toContain("limit=250");
  });

  it("throws a readable error on HTTP failure", async () => {
    mockFetchOnce({ error: "parse error in query" }, 400);
    await expect(lokiQueryLogs("{", RANGE, 10)).rejects.toThrow(
      /query_range failed \(400\)/,
    );
  });

  it("rejects matrix responses for a log query", async () => {
    mockFetchOnce({
      status: "success",
      data: { resultType: "matrix", result: [] },
    });
    await expect(lokiQueryLogs("{}", RANGE, 10)).rejects.toThrow(
      /resultType=matrix/,
    );
  });
});

describe("lokiQueryHistogram", () => {
  it("maps matrix results to per-level series in milliseconds", async () => {
    mockFetchOnce({
      status: "success",
      data: {
        resultType: "matrix",
        result: [
          {
            metric: { level: "error" },
            values: [
              [1700, "3"],
              [1760, "5"],
            ],
          },
          { metric: {}, values: [[1700, "10"]] },
        ],
      },
    });
    const series = await lokiQueryHistogram("q", RANGE, "1m");
    expect(series).toEqual([
      {
        level: "error",
        points: [
          [1_700_000, 3],
          [1_760_000, 5],
        ],
      },
      { level: "unknown", points: [[1_700_000, 10]] },
    ]);
  });
});

describe("label endpoints", () => {
  it("fetches labels within the range", async () => {
    const fn = mockFetchOnce({
      status: "success",
      data: ["level", "service_name"],
    });
    const labels = await lokiLabels(RANGE);
    expect(labels).toEqual(["level", "service_name"]);
    expect(String(fn.mock.calls[0]?.[0])).toContain("/loki/api/v1/labels?");
  });

  it("URL-encodes label names for the values endpoint", async () => {
    const fn = mockFetchOnce({ status: "success", data: ["a"] });
    await lokiLabelValues("weird/label", RANGE);
    expect(String(fn.mock.calls[0]?.[0])).toContain(
      "/loki/api/v1/label/weird%2Flabel/values?",
    );
  });

  it("tolerates a missing data field", async () => {
    mockFetchOnce({ status: "success" });
    expect(await lokiLabels(RANGE)).toEqual([]);
  });
});
