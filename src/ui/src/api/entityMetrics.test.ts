import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildObservedMetricNamesDoc,
  discoverObservedMetricNames,
  fetchEntityMetricNames,
  fetchMetricDefinitions,
  nameSegments,
} from "./entityMetrics";
import { runIrQuery } from "./queryIr";
import { resolveEntity, searchMetrics } from "../features/schema/api";
import type { MetricHit } from "../features/schema/api";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));
vi.mock("../features/schema/api", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../features/schema/api")>();
  return { ...actual, searchMetrics: vi.fn(), resolveEntity: vi.fn() };
});

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

describe("observed metric-name discovery", () => {
  it("asks the metrics source to group by metric name over the window", () => {
    // The registry cannot say which metrics a deployment emits, so the set
    // the panel starts from comes from the data: one aggregate, no filter.
    const doc = buildObservedMetricNamesDoc("metrics", range);

    expect(doc.from).toBe("metrics");
    expect(doc.range).toEqual({
      from: "1000000000000",
      to: "4600000000000",
    });
    expect(doc.pipeline).toEqual([
      { aggregate: { by: ["metric.name"], aggs: [{ fn: "count", as: "n" }] } },
    ]);
  });

  it("returns the observed names", async () => {
    vi.mocked(runIrQuery).mockResolvedValue({
      result: "table",
      window: { start_ns: 0, end_ns: 0 },
      columns: [
        { name: "metric_name", type: "string" },
        { name: "n", type: "int64" },
      ],
      rows: [
        ["system.cpu.time", 47160],
        ["process.memory.usage", 5877],
      ],
    });

    expect(await discoverObservedMetricNames("metrics", range)).toEqual([
      "system.cpu.time",
      "process.memory.usage",
    ]);
  });

  it("reports no names for a window nothing was written in", async () => {
    // Distinct from "this tenant has no metrics": the caller must be able to
    // widen the window rather than conclude the entity has none.
    vi.mocked(runIrQuery).mockResolvedValue({
      result: "table",
      window: { start_ns: 0, end_ns: 0 },
      columns: [{ name: "metric_name", type: "string" }],
      rows: [],
    });

    expect(await discoverObservedMetricNames("metrics", range)).toEqual([]);
  });
});

function metric(
  name: string,
  entities: string[],
  instrument = "gauge",
): MetricHit {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument,
    unit: "1",
    attributes: [],
    entity_associations: entities,
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as MetricHit;
}

describe("metric definition lookup", () => {
  it("groups names by their first segment", () => {
    // The endpoint searches by prefix, so the segment is the unit of work —
    // a deployment emitting 80 metrics spans a handful of them.
    expect(
      nameSegments([
        "system.cpu.time",
        "system.memory.usage",
        "process.cpu.utilization",
        "signaldb.wal.entries_processed",
      ]),
    ).toEqual(["system", "process", "signaldb"]);
  });

  it("segments a Prometheus-style name at its underscore too", () => {
    // Measured against a live deployment: splitting on the dot alone turned
    // 80 observed names into 39 prefixes, because collector metrics are named
    // `otelcol_exporter_sent_spans` with no dot at all — one request per
    // metric, which is what batching by prefix exists to avoid. The search is
    // a plain string prefix, so the first word covers them all.
    expect(
      nameSegments([
        "otelcol_exporter_sent_spans",
        "otelcol_receiver_accepted_spans",
        "otelcol_scraper_errored_metric_points",
      ]),
    ).toEqual(["otelcol"]);
  });

  it("fetches one prefix search per segment, not one per metric", async () => {
    vi.mocked(searchMetrics).mockImplementation(async (prefix: string) =>
      prefix === "system"
        ? [metric("system.cpu.time", ["host"])]
        : [metric("process.cpu.utilization", ["process"])],
    );

    const defs = await fetchMetricDefinitions([
      "system.cpu.time",
      "system.memory.usage",
      "process.cpu.utilization",
    ]);

    expect(vi.mocked(searchMetrics).mock.calls.map((c) => c[0])).toEqual([
      "system",
      "process",
    ]);
    expect(defs.map((d) => d.name)).toEqual([
      "system.cpu.time",
      "process.cpu.utilization",
    ]);
  });

  it("keeps only definitions for names actually observed", async () => {
    // A prefix search answers with the registry's whole namespace; the panel
    // must not promise a metric this deployment never wrote.
    vi.mocked(searchMetrics).mockResolvedValue([
      metric("system.cpu.time", ["host"]),
      metric("system.paging.faults", ["host"]),
    ]);

    const defs = await fetchMetricDefinitions(["system.cpu.time"]);

    expect(defs.map((d) => d.name)).toEqual(["system.cpu.time"]);
  });

  it("asks nothing when the window held no metrics", async () => {
    expect(await fetchMetricDefinitions([])).toEqual([]);
    expect(searchMetrics).not.toHaveBeenCalled();
  });
});

describe("the registry's own metric list for an entity", () => {
  it("asks the entity, which is where the association is recorded", async () => {
    // The registry records the association on the entity, so it can simply be
    // read — no guessing prefixes from what happens to be observed, and no
    // missing a family whose names look nothing like the entity's own
    // (`host` is described by `nfs.*` as well as `system.*`).
    vi.mocked(resolveEntity).mockResolvedValue({
      key: "host",
      hits: [
        { metrics: ["system.cpu.time", "nfs.server.io"] },
        { metrics: ["system.cpu.time", "custom.host.temp"] },
      ] as never,
    });

    expect(await fetchEntityMetricNames("host")).toEqual([
      "system.cpu.time",
      "nfs.server.io",
      "custom.host.temp",
    ]);
  });

  it("is empty for an entity no registry associates a metric with", async () => {
    vi.mocked(resolveEntity).mockResolvedValue({
      key: "telemetry.sdk",
      hits: [{ metrics: [] } as never],
    });

    expect(await fetchEntityMetricNames("telemetry.sdk")).toEqual([]);
  });
});
