import { afterEach, describe, expect, it, vi } from "vitest";
import { buildEntitySourceDoc, fetchCatalogEntities } from "./catalog";
import { runIrQuery } from "./queryIr";
import { ERROR_PATTERN, GROUP_BUDGET } from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

const service: EntityTypeDef = {
  id: "service",
  label: "Services",
  singular: "service",
  identity: ["service.name", "service.namespace"],
  spanKindScope: "Server",
};

const database: EntityTypeDef = {
  id: "database",
  label: "Databases",
  singular: "database",
  identity: ["db.namespace", "db.system.name"],
};

const host: EntityTypeDef = {
  id: "host",
  label: "Hosts",
  singular: "host",
  identity: ["host.name"],
};

const hostMultiSource: EntityTypeDef = {
  ...host,
  sources: ["traces", "logs"],
};

describe("buildEntitySourceDoc", () => {
  it("asks the traces source for RED, most frequent first", () => {
    const doc = buildEntitySourceDoc(database, "traces", range);
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "table",
      pipeline: [
        {
          aggregate: {
            by: ["db.namespace", "db.system.name"],
            aggs: [
              { fn: "count", as: "n" },
              {
                fn: "count",
                as: "errors",
                where: {
                  field: "status.code",
                  op: "regex",
                  value: ERROR_PATTERN,
                },
              },
              { fn: "quantile", of: "duration", arg: 0.5, as: "p50" },
              { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
              { fn: "max", of: "start_time_unix_nano", as: "last" },
            ],
          },
        },
        { order: [{ of: "n", dir: "desc" }] },
        { limit: GROUP_BUDGET + 1 },
      ],
    });
  });

  it("asks a non-traces source for count and last-seen only", () => {
    const doc = buildEntitySourceDoc(host, "logs", range);
    expect(doc.from).toBe("logs");
    expect(doc.pipeline?.[0]).toEqual({
      aggregate: {
        by: ["host.name"],
        aggs: [
          { fn: "count", as: "n" },
          { fn: "max", of: "timestamp", as: "last" },
        ],
      },
    });
  });

  it("scopes only a traces query to spanKindScope", () => {
    const tracesDoc = buildEntitySourceDoc(service, "traces", range);
    expect(tracesDoc.pipeline?.[0]).toEqual({
      where: { field: "span_kind", op: "eq", value: "Server" },
    });

    const logsDoc = buildEntitySourceDoc(service, "logs", range);
    expect(logsDoc.pipeline?.[0]).toEqual({
      aggregate: expect.anything(),
    });
  });

  it("adds a raw equality clause per pinned field, after the span-kind scope", () => {
    const doc = buildEntitySourceDoc(service, "traces", range, [
      { field: "service.name", value: "gateway" },
      { field: "service.namespace", value: "edge" },
    ]);
    expect(doc.pipeline?.slice(0, 3)).toEqual([
      { where: { field: "span_kind", op: "eq", value: "Server" } },
      { where: { field: "service.name", op: "eq", value: "gateway" } },
      { where: { field: "service.namespace", op: "eq", value: "edge" } },
    ]);
  });

  it("pins without a span-kind scope for an entity type that has none", () => {
    const doc = buildEntitySourceDoc(database, "traces", range, [
      { field: "db.namespace", value: "orders" },
    ]);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "db.namespace", op: "eq", value: "orders" },
    });
  });

  // A "(not set)" identity dimension pins to "absent on this record", not
  // to an unconstrained field — otherwise a two-dimension entity's KPIs
  // could be pulled from a different entity that happens to have a value
  // for the "(not set)" dimension.
  it("compiles a null-valued pin to a negated exists check, not an eq", () => {
    const doc = buildEntitySourceDoc(service, "traces", range, [
      { field: "service.name", value: "gateway" },
      { field: "service.namespace", value: null },
    ]);
    expect(doc.pipeline?.slice(0, 3)).toEqual([
      { where: { field: "span_kind", op: "eq", value: "Server" } },
      { where: { field: "service.name", op: "eq", value: "gateway" } },
      { where: { not: { field: "service.namespace", op: "exists" } } },
    ]);
  });

  // Per-source identity degradation (3.4/3.5): a source that carries only
  // some of the declared identity attributes must group by what it has, not
  // by the full tuple — grouping metrics by `host.name` when metrics never
  // carries it would put every process in one null-valued bucket.
  it("groups a source by its own degraded identity, not the full tuple", () => {
    const process: EntityTypeDef = {
      id: "process",
      label: "Processes",
      singular: "process",
      identity: ["process.pid", "host.name"],
      sources: ["metrics", "traces"],
      identityBySource: {
        metrics: ["process.pid"],
        traces: ["process.pid", "host.name"],
      },
    };

    const metricsDoc = buildEntitySourceDoc(process, "metrics", range);
    expect(
      (metricsDoc.pipeline?.[0] as { aggregate: { by: string[] } }).aggregate
        .by,
    ).toEqual(["process.pid"]);

    const tracesDoc = buildEntitySourceDoc(process, "traces", range);
    expect(
      (tracesDoc.pipeline?.[0] as { aggregate: { by: string[] } }).aggregate.by,
    ).toEqual(["process.pid", "host.name"]);
  });

  it("falls back to the full identity when no per-source identity was computed", () => {
    const doc = buildEntitySourceDoc(host, "logs", range);
    expect(
      (doc.pipeline?.[0] as { aggregate: { by: string[] } }).aggregate.by,
    ).toEqual(["host.name"]);
  });
});

describe("fetchCatalogEntities", () => {
  it("queries only traces for an entity type with no sources declared", async () => {
    vi.mocked(runIrQuery).mockResolvedValue({
      result: "table",
      columns: [],
      window: { start_ns: 1, end_ns: 2 },
      rows: [
        // No host.name on most spans — a real gap, not a real host.
        [null, 141, 0, 1_000_000, 14_000_000, "1700000000000000000"],
        ["ip-10-0-1-08", 5, 0, 900_000, 3_000_000, "1700000000000000000"],
      ],
    });

    const result = await fetchCatalogEntities(host, range);

    expect(runIrQuery).toHaveBeenCalledTimes(1);
    expect(result.entities).toEqual([
      {
        values: ["ip-10-0-1-08"],
        observations: [{ source: "traces", count: 5 }],
        lastNs: "1700000000000000000",
        red: { traces: 5, errors: 0, p50Ms: 0.9, p95Ms: 3 },
      },
    ]);
  });

  it("reports what each signal observed instead of one summed volume", async () => {
    vi.mocked(runIrQuery).mockImplementation(async (doc) => {
      if (doc.from === "traces") {
        return {
          result: "table",
          columns: [],
          window: { start_ns: 1, end_ns: 2 },
          rows: [
            ["ip-10-0-1-08", 5, 0, 900_000, 3_000_000, "1700000000000000000"],
          ],
        };
      }
      return {
        result: "table",
        columns: [],
        window: { start_ns: 1, end_ns: 2 },
        rows: [["ip-10-0-1-08", 3, "1700000000500000000"]],
      };
    });

    const result = await fetchCatalogEntities(hostMultiSource, range);

    // 5 spans and 3 log lines stay 5 spans and 3 log lines. Nothing in the
    // row is the number 8: summing a span count into a log-line count
    // produces a figure that describes neither.
    expect(result.entities[0]!.observations).toEqual([
      { source: "traces", count: 5 },
      { source: "logs", count: 3 },
    ]);
    expect(result.entities[0]!.lastNs).toBe("1700000000500000000");
  });

  it("omits trace-derived measurements for an entity never observed in traces", async () => {
    vi.mocked(runIrQuery).mockImplementation(async (doc) => {
      if (doc.from === "traces") {
        return {
          result: "table",
          columns: [],
          window: { start_ns: 1, end_ns: 2 },
          rows: [],
        };
      }
      return {
        result: "table",
        columns: [],
        window: { start_ns: 1, end_ns: 2 },
        rows: [["ip-10-0-2-09", 7, "1700000000600000000"]],
      };
    });

    const result = await fetchCatalogEntities(hostMultiSource, range);

    // A host seen only in logs has no span status and no span duration, so
    // it carries no RED at all — rather than a zeroed one that renders as a
    // real "0% errors, 0ms p95" measurement.
    expect(result.entities).toEqual([
      {
        values: ["ip-10-0-2-09"],
        observations: [{ source: "logs", count: 7 }],
        lastNs: "1700000000600000000",
      },
    ]);
    expect(result.entities[0]!.red).toBeUndefined();
  });

  it("counts errors against the traces observed, not total observations", async () => {
    // 1 error among 10 traces, plus 90 log lines for the same host — the
    // error is a rate of the 10 traces, not of 100 mixed records.
    vi.mocked(runIrQuery).mockImplementation(async (doc) => {
      if (doc.from === "traces") {
        return {
          result: "table",
          columns: [],
          window: { start_ns: 1, end_ns: 2 },
          rows: [
            ["ip-10-0-1-08", 10, 1, 900_000, 3_000_000, "1700000000000000000"],
          ],
        };
      }
      return {
        result: "table",
        columns: [],
        window: { start_ns: 1, end_ns: 2 },
        rows: [["ip-10-0-1-08", 90, "1700000000500000000"]],
      };
    });

    const result = await fetchCatalogEntities(hostMultiSource, range);

    expect(result.entities[0]!.red).toEqual({
      traces: 10,
      errors: 1,
      p50Ms: 0.9,
      p95Ms: 3,
    });
  });

  it("ranks by total observations without presenting the total as volume", async () => {
    vi.mocked(runIrQuery).mockImplementation(async (doc) => {
      if (doc.from === "traces") {
        return {
          result: "table",
          columns: [],
          window: { start_ns: 1, end_ns: 2 },
          rows: [
            ["ip-10-0-1-08", 5, 0, 900_000, 3_000_000, "1700000000000000000"],
          ],
        };
      }
      return {
        result: "table",
        columns: [],
        window: { start_ns: 1, end_ns: 2 },
        rows: [
          ["ip-10-0-1-08", 3, "1700000000500000000"],
          ["ip-10-0-2-09", 7, "1700000000600000000"],
        ],
      };
    });

    const result = await fetchCatalogEntities(hostMultiSource, range);

    // 5+3 outranks 7 — the total is a ranking key, and appears nowhere in
    // the row as a figure a reader could mistake for request volume.
    expect(result.entities.map((e) => e.values[0])).toEqual([
      "ip-10-0-1-08",
      "ip-10-0-2-09",
    ]);
  });

  it("aligns a degraded source's row onto the primary dimension, not the secondary one", async () => {
    // metrics carries only process.pid; traces carries pid and host.name.
    // metrics' single-column row must land on the primary dimension. It
    // cannot be merged with the traces row for the same pid — metrics never
    // reported a host, so the two rows are not known to share one — but its
    // pid must not be misread as a host name.
    const process: EntityTypeDef = {
      id: "process",
      label: "Processes",
      singular: "process",
      identity: ["process.pid", "host.name"],
      sources: ["traces", "metrics"],
      identityBySource: {
        traces: ["process.pid", "host.name"],
        metrics: ["process.pid"],
      },
    };
    vi.mocked(runIrQuery).mockImplementation(async (doc) => {
      if (doc.from === "traces") {
        return {
          result: "table",
          columns: [],
          window: { start_ns: 1, end_ns: 2 },
          rows: [
            [
              "4821",
              "ip-10-0-1-08",
              2,
              0,
              900_000,
              3_000_000,
              "1700000000000000000",
            ],
          ],
        };
      }
      return {
        result: "table",
        columns: [],
        window: { start_ns: 1, end_ns: 2 },
        rows: [["4821", 6, "1700000000900000000"]],
      };
    });

    const result = await fetchCatalogEntities(process, range);

    expect(result.entities).toHaveLength(2);
    const byValues = new Map(
      result.entities.map((e) => [JSON.stringify(e.values), e]),
    );
    expect(
      byValues.get(JSON.stringify(["4821", "ip-10-0-1-08"])),
    ).toBeDefined();
    expect(
      byValues.get(JSON.stringify(["4821", "ip-10-0-1-08"]))?.observations,
    ).toEqual([{ source: "traces", count: 2 }]);
    expect(byValues.get(JSON.stringify(["4821", null]))?.observations).toEqual([
      { source: "metrics", count: 6 },
    ]);
  });
});
