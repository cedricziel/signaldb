import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildSparklineDoc,
  fetchEntitySparklines,
  headlineMetric,
} from "./entitySparkline";
import { runIrQuery } from "./queryIr";
import type { MetricHit } from "../features/schema/api";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

function metric(name: string, instrument = "gauge"): MetricHit {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument,
    unit: "1",
    attributes: [],
    entity_associations: ["host"],
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as MetricHit;
}

describe("headlineMetric", () => {
  it("is the first associated metric the window holds", () => {
    // Deterministic rather than curated: picking a "best" metric per entity
    // type is exactly the hand-maintained mapping this feature exists to
    // avoid, and the registry declares no such thing.
    const chosen = headlineMetric([
      metric("system.cpu.time"),
      metric("system.memory.usage"),
    ]);

    expect(chosen?.name).toBe("system.cpu.time");
  });

  it("is nothing when the entity type has no associated metrics", () => {
    expect(headlineMetric([])).toBeUndefined();
  });
});

describe("buildSparklineDoc", () => {
  it("groups by the entity type's identity so one query serves every row", () => {
    const doc = buildSparklineDoc(
      metric("system.cpu.utilization"),
      ["host.name"],
      range,
      300,
    );

    expect(doc.from).toBe("metrics");
    expect(doc.pipeline![0]).toEqual({
      where: {
        field: "metric.name",
        op: "eq",
        value: "system.cpu.utilization",
      },
    });
    expect(doc.pipeline!.at(-1)).toEqual({
      aggregate: {
        by: ["host.name"],
        aggs: [{ fn: "avg", of: "metric.value", as: "v" }],
        step: "300s",
      },
    });
  });

  it("asks the histogram source through the quantile stage", () => {
    const doc = buildSparklineDoc(
      metric("http.server.request.duration", "histogram"),
      ["service.name"],
      range,
      300,
    );

    expect(doc.from).toBe("metrics_histogram");
    expect(doc.pipeline!.at(-1)).toEqual({
      histogram_quantile: {
        q: 0.95,
        by: ["service.name"],
        step: "300s",
        as: "p95",
      },
    });
  });
});

describe("fetchEntitySparklines", () => {
  it("indexes each row's series by its identity values", async () => {
    vi.mocked(runIrQuery).mockResolvedValue({
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: [
        { labels: { host_name: "db-01" }, points: [["1000", 0.2]] },
        { labels: { host_name: "web-02" }, points: [["1000", 0.7]] },
      ],
    });

    const byRow = await fetchEntitySparklines(
      metric("system.cpu.utilization"),
      ["host.name"],
      range,
      300,
    );

    expect(byRow.get("db-01")).toHaveLength(1);
    expect(byRow.get("web-02")).toHaveLength(1);
  });

  it("has no entry for a row the window holds no points for", async () => {
    // An empty cell, not a flat line at zero: the row's other columns are
    // real measurements and this one must not look like one.
    vi.mocked(runIrQuery).mockResolvedValue({
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: [{ labels: { host_name: "db-01" }, points: [["1000", 0.2]] }],
    });

    const byRow = await fetchEntitySparklines(
      metric("system.cpu.utilization"),
      ["host.name"],
      range,
      300,
    );

    expect(byRow.has("web-02")).toBe(false);
  });
});
