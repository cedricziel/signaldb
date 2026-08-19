import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildObservedMetricNamesDoc,
  discoverObservedMetricNames,
  fetchMetricDefinitions,
  metricsForEntity,
  nameSegments,
} from "./entityMetrics";
import type { EntityTypeDef } from "../features/catalog/entityTypes";
import { runIrQuery } from "./queryIr";
import { searchMetrics } from "../features/schema/api";
import type { MetricHit } from "../features/schema/api";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));
vi.mock("../features/schema/api", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../features/schema/api")>();
  return { ...actual, searchMetrics: vi.fn() };
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
        "otelcol_receiver_accepted_spans",
        "signaldb.wal.entries_processed",
      ]),
    ).toEqual([
      "system",
      "process",
      "otelcol_receiver_accepted_spans",
      "signaldb",
    ]);
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

describe("filtering definitions to an entity type", () => {
  const defs = [
    metric("system.cpu.time", ["host"], "counter"),
    metric("system.memory.usage", ["host"], "updowncounter"),
    metric("process.cpu.utilization", ["process"]),
    metric("otelcol_receiver_accepted_spans", []),
    metric("signaldb.wal.entries_processed", []),
  ];

  it("keeps the metrics the entity is associated with", () => {
    const host: EntityTypeDef = {
      id: "host",
      label: "Hosts",
      singular: "host",
      identity: ["host.name"],
      registryEntity: "host",
    };

    expect(metricsForEntity(defs, host).map((d) => d.name)).toEqual([
      "system.cpu.time",
      "system.memory.usage",
    ]);
  });

  it("drops metrics that associate with nothing", () => {
    // `otelcol_*` and `signaldb.*` measure a collector and this database, not
    // any entity the catalog knows — the correct answer, not a special case.
    const process: EntityTypeDef = {
      id: "process",
      label: "Processes",
      singular: "process",
      identity: ["process.pid"],
      registryEntity: "process",
    };

    expect(metricsForEntity(defs, process).map((d) => d.name)).toEqual([
      "process.cpu.utilization",
    ]);
  });

  it("finds nothing for an entity type no registry declares", () => {
    // A curated type the registry never mentions carries no registry name, so
    // there is no association to look up — and no panel to draw.
    const database: EntityTypeDef = {
      id: "database",
      label: "Databases",
      singular: "database",
      identity: ["db.namespace"],
    };

    expect(metricsForEntity(defs, database)).toEqual([]);
  });

  it("finds nothing for an entity type nothing associates with", () => {
    const sdk: EntityTypeDef = {
      id: "telemetry_sdk",
      label: "Telemetry sdks",
      singular: "telemetry sdk",
      identity: ["telemetry.sdk.name"],
      registryEntity: "telemetry.sdk",
    };

    expect(metricsForEntity(defs, sdk)).toEqual([]);
  });
});
