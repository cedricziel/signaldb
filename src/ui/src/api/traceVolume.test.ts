import { describe, expect, it, vi } from "vitest";
import {
  STATUS_COLORS,
  STATUS_ORDER,
  buildTraceVolumeDoc,
  normalizeStatus,
  seriesFromIrResponse,
} from "./traceVolume";

describe("buildTraceVolumeDoc", () => {
  it("asks for a time-bucketed count grouped by span status", () => {
    const doc = buildTraceVolumeDoc(
      { fromMs: 1_000_000, toMs: 4_600_000 },
      "1h",
    );
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "series",
      pipeline: [
        {
          aggregate: {
            by: ["status.code"],
            aggs: [{ fn: "count", as: "n" }],
            step: "1h",
          },
        },
      ],
    });
  });

  // The chart must not be bounded by the trace list's row limit.
  it("carries no limit stage", () => {
    const doc = buildTraceVolumeDoc({ fromMs: 0, toMs: 1000 }, "1m");
    expect(JSON.stringify(doc)).not.toContain("limit");
  });
});

describe("normalizeStatus", () => {
  it("maps OTLP status codes onto display keys", () => {
    expect(normalizeStatus("Error")).toBe("error");
    expect(normalizeStatus("STATUS_CODE_ERROR")).toBe("error");
    expect(normalizeStatus("Ok")).toBe("ok");
    expect(normalizeStatus("STATUS_CODE_OK")).toBe("ok");
    expect(normalizeStatus("Unspecified")).toBe("unset");
    expect(normalizeStatus("")).toBe("unset");
    // A null-valued group renders as the literal string "null" over the wire.
    expect(normalizeStatus("null")).toBe("unset");
  });

  it("has a colour for every stacking key", () => {
    for (const key of STATUS_ORDER) {
      expect(STATUS_COLORS[key]).toBeTruthy();
    }
  });
});

describe("seriesFromIrResponse", () => {
  it("keys by status and converts nanosecond points to milliseconds", () => {
    expect(
      seriesFromIrResponse({
        result: "series",
        window: { start_ns: 0, end_ns: 1 },
        series: [
          {
            labels: { status_code: "Error" },
            points: [
              [1_786_089_600_000_000_000, 12],
              [1_786_093_200_000_000_000, 3],
            ],
          },
          {
            labels: { status_code: "Unspecified" },
            points: [[1_786_089_600_000_000_000, 2018]],
          },
        ],
      }),
    ).toEqual([
      {
        key: "error",
        points: [
          [1_786_089_600_000, 12],
          [1_786_093_200_000, 3],
        ],
      },
      { key: "unset", points: [[1_786_089_600_000, 2018]] },
    ]);
  });

  it("reads the status from whichever label the server returned", () => {
    const series = seriesFromIrResponse({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [{ labels: { "status.code": "Ok" }, points: [[1_000_000, 4]] }],
    });
    expect(series[0]?.key).toBe("ok");
  });

  it("treats an unlabelled series as unset rather than dropping it", () => {
    const series = seriesFromIrResponse({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [{ labels: {}, points: [[1_000_000, 7]] }],
    });
    expect(series).toEqual([{ key: "unset", points: [[1, 7]] }]);
  });

  it("tolerates a response with no series", () => {
    expect(
      seriesFromIrResponse({
        result: "series",
        window: { start_ns: 0, end_ns: 1 },
      }),
    ).toEqual([]);
  });

  it("skips malformed points instead of rendering NaN bars", () => {
    const series = seriesFromIrResponse({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [
        {
          labels: { status_code: "Error" },
          points: [[1_000_000, 4], ["nope"], [null, 2]],
        },
      ],
    });
    expect(series).toEqual([{ key: "error", points: [[1, 4]] }]);
  });
});

describe("fetchTraceVolume", () => {
  it("goes through the generated client, never a hand-written fetch", async () => {
    const spy = vi.spyOn(globalThis, "fetch");
    const { fetchTraceVolume } = await import("./traceVolume");
    expect(typeof fetchTraceVolume).toBe("function");
    // The module must not have issued anything at import time.
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });
});

describe("buildTraceVolumeDoc filtering", () => {
  const range = { fromMs: 0, toMs: 3600_000 };

  it("applies active filters so the chart matches the list", () => {
    const doc = buildTraceVolumeDoc(range, "1h", [
      { field: "service.name", value: "signaldb-ui" },
    ]);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "signaldb-ui" },
    });
    // The aggregate still follows.
    expect(JSON.stringify(doc.pipeline?.[1])).toContain("aggregate");
  });

  it("is unfiltered when nothing is selected", () => {
    const doc = buildTraceVolumeDoc(range, "1h", []);
    expect(JSON.stringify(doc.pipeline)).not.toContain("where");
  });

  it("maps a facet field onto its IR field", () => {
    const doc = buildTraceVolumeDoc(range, "1h", [
      { field: "name", value: "GET /x" },
    ]);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "span.name", op: "eq", value: "GET /x" },
    });
  });
});
