import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEntityMetricDocs,
  fetchEntityMetricSeries,
  METRIC_NAMES_PER_QUERY,
  splitSeriesByMetric,
} from "./entityMetricSeries";
import { runIrQuery } from "./queryIr";
import type { MetricHit } from "../features/schema/api";
import type { QueryIrRequest } from "./gen";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };
const pins = [{ field: "host.name", value: "db-01" }];

function metric(name: string, instrument: string): MetricHit {
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

/** The metric names a set of documents selects, read back out of their
 * alternations — the names as the querier will see them, unescaped. */
function alternatedNames(docs: QueryIrRequest[]): string[] {
  return docs.flatMap((d) => {
    const clause = d.pipeline![0] as { where: { value: string } };
    return clause.where.value
      .replace(/^\^\(|\)\$$/g, "")
      .split("|")
      .map((n) => n.replace(/\\(.)/g, "$1"));
  });
}

describe("buildEntityMetricDocs", () => {
  it("alternates the names and pins the entity", () => {
    const [doc] = buildEntityMetricDocs(
      [metric("system.cpu.utilization", "gauge")],
      pins,
      range,
      300,
    );

    expect(doc!.from).toBe("metrics");
    expect(doc!.pipeline![0]).toEqual({
      where: {
        field: "metric.name",
        op: "regex",
        value: "^(system\\.cpu\\.utilization)$",
      },
    });
    // The dots are escaped: an unescaped `.` matches any character, so
    // `system.cpu.utilization` would also select `systemXcpuYutilization`.
    expect(doc!.pipeline![1]).toEqual({
      where: { field: "host.name", op: "eq", value: "db-01" },
    });
  });

  it("groups by metric name so one response covers every metric", () => {
    const [doc] = buildEntityMetricDocs(
      [metric("system.cpu.utilization", "gauge")],
      pins,
      range,
      300,
    );

    expect(doc!.pipeline!.at(-1)).toEqual({
      aggregate: {
        by: ["metric.name"],
        aggs: [{ fn: "avg", of: "metric.value", as: "v" }],
        step: "300s",
      },
    });
    expect(doc!.result).toBe("series");
  });

  it("aggregates each instrument the way that instrument means something", () => {
    // Averaging a cumulative counter across a step is nonsense; taking the
    // step's maximum keeps the series monotonic, which is what a counter is.
    const docs = buildEntityMetricDocs(
      [
        metric("system.cpu.utilization", "gauge"),
        metric("system.memory.usage", "updowncounter"),
        metric("system.cpu.time", "counter"),
      ],
      pins,
      range,
      300,
    );

    const byFn = docs.map((d) => {
      const last = d.pipeline!.at(-1) as {
        aggregate: { aggs: { fn: string }[]; by: string[] };
      };
      return last.aggregate.aggs[0]!.fn;
    });
    expect([...byFn].sort()).toEqual(["avg", "max"]);

    // and every metric is still covered, none dropped by the split
    expect(alternatedNames(docs).sort()).toEqual([
      "system.cpu.time",
      "system.cpu.utilization",
      "system.memory.usage",
    ]);
  });

  it("chunks a long name list rather than building one huge alternation", () => {
    const many = Array.from({ length: METRIC_NAMES_PER_QUERY + 3 }, (_, i) =>
      metric(`system.metric.${i}`, "gauge"),
    );

    const docs = buildEntityMetricDocs(many, pins, range, 300);

    expect(docs).toHaveLength(2);
    expect(docs.every((d) => d.pipeline!.length === 3)).toBe(true);
  });

  it("sends a histogram to the source that can answer for it", () => {
    // A metrics_histogram row is a whole bucketed histogram, not a scalar —
    // there is no value column to average, so the scalar aggregate is
    // rejected outright. The quantile stage is the only way in, and its
    // default rate mode is also the right reading of a cumulative histogram.
    const docs = buildEntityMetricDocs(
      [
        metric("system.cpu.utilization", "gauge"),
        metric("http.server.request.duration", "histogram"),
      ],
      pins,
      range,
      300,
    );

    const hist = docs.find((d) => d.from === "metrics_histogram")!;
    expect(hist).toBeDefined();
    expect(hist.pipeline!.at(-1)).toEqual({
      histogram_quantile: { q: 0.95, step: "300s", as: "p95" },
    });
    expect(alternatedNames([hist])).toEqual(["http.server.request.duration"]);

    // and the scalar source never sees the histogram
    const scalar = docs.filter((d) => d.from === "metrics");
    expect(alternatedNames(scalar)).toEqual(["system.cpu.utilization"]);
  });

  it("asks nothing of the histogram source when no metric is a histogram", () => {
    const docs = buildEntityMetricDocs(
      [metric("system.cpu.utilization", "gauge")],
      pins,
      range,
      300,
    );

    expect(docs.every((d) => d.from === "metrics")).toBe(true);
  });

  it("asks nothing when the entity has no associated metrics", () => {
    expect(buildEntityMetricDocs([], pins, range, 300)).toEqual([]);
  });
});

describe("splitSeriesByMetric", () => {
  it("groups the response's series under their metric name", () => {
    const groups = splitSeriesByMetric([
      {
        labels: { metric_name: "system.cpu.utilization", cpu_mode: "user" },
        points: [["1000", 0.2]],
      },
      {
        labels: { metric_name: "system.cpu.utilization", cpu_mode: "system" },
        points: [["1000", 0.1]],
      },
      {
        labels: { metric_name: "system.memory.usage" },
        points: [["1000", 4096]],
      },
    ]);

    expect([...groups.keys()]).toEqual([
      "system.cpu.utilization",
      "system.memory.usage",
    ]);
    // A metric split by an attribute keeps both series — that is the shape of
    // the metric, not two different metrics.
    expect(groups.get("system.cpu.utilization")).toHaveLength(2);
  });

  it("yields no group for a metric the window held no points for", () => {
    // The absent metric is simply absent: no key, so nothing downstream can
    // render it as a flat zero.
    const groups = splitSeriesByMetric([
      { labels: { metric_name: "system.memory.usage" }, points: [["1000", 1]] },
    ]);

    expect(groups.has("system.cpu.utilization")).toBe(false);
  });

  it("ignores a series carrying no metric name", () => {
    expect(splitSeriesByMetric([{ labels: {}, points: [] }]).size).toBe(0);
  });
});

describe("fetchEntityMetricSeries", () => {
  it("merges every document's series into one map", async () => {
    vi.mocked(runIrQuery)
      .mockResolvedValueOnce({
        result: "series",
        window: { start_ns: 0, end_ns: 0 },
        series: [
          {
            labels: { metric_name: "system.cpu.utilization" },
            points: [["1000", 0.2]],
          },
        ],
      })
      .mockResolvedValueOnce({
        result: "series",
        window: { start_ns: 0, end_ns: 0 },
        series: [
          {
            labels: { metric_name: "system.cpu.time" },
            points: [["1000", 12]],
          },
        ],
      });

    const groups = await fetchEntityMetricSeries(
      [
        metric("system.cpu.utilization", "gauge"),
        metric("system.cpu.time", "counter"),
      ],
      pins,
      range,
      300,
    );

    expect([...groups.keys()].sort()).toEqual([
      "system.cpu.time",
      "system.cpu.utilization",
    ]);
  });
});
