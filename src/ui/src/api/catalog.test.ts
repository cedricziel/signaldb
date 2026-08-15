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
    expect(result.groups).toEqual([
      {
        values: ["ip-10-0-1-08"],
        count: 5,
        errors: 0,
        p50Ms: 0.9,
        p95Ms: 3,
        lastNs: "1700000000000000000",
        traceCount: 5,
      },
    ]);
  });

  it("merges an entity discovered across multiple signals, keeping RED from traces only", async () => {
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
          // Only ever seen in logs — never a trace — but still a real host.
          ["ip-10-0-2-09", 7, "1700000000600000000"],
        ],
      };
    });

    const result = await fetchCatalogEntities(hostMultiSource, range);

    expect(runIrQuery).toHaveBeenCalledTimes(2);
    expect(result.groups).toEqual([
      // Sorted by merged count, descending: 8 beats 7.
      {
        values: ["ip-10-0-1-08"],
        count: 8,
        errors: 0,
        p50Ms: 0.9,
        p95Ms: 3,
        lastNs: "1700000000500000000",
        traceCount: 5,
      },
      {
        values: ["ip-10-0-2-09"],
        count: 7,
        errors: 0,
        p50Ms: 0,
        p95Ms: 0,
        lastNs: "1700000000600000000",
        traceCount: 0,
      },
    ]);
  });

  it("keeps the trace count distinct from total volume, for an accurate error rate", async () => {
    // 1 error among 10 traces, plus 90 log lines for the same host — the
    // error is a rate of the 10 traces, not the 100 merged records.
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

    expect(result.groups).toEqual([
      {
        values: ["ip-10-0-1-08"],
        count: 100,
        errors: 1,
        p50Ms: 0.9,
        p95Ms: 3,
        lastNs: "1700000000500000000",
        traceCount: 10,
      },
    ]);
  });
});
