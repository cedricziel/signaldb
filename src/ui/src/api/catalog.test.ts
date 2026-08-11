import { afterEach, describe, expect, it, vi } from "vitest";
import { buildEntityDoc, fetchCatalogEntities } from "./catalog";
import { client } from "./gen/client.gen";
import { ERROR_PATTERN, GROUP_BUDGET } from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
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

describe("buildEntityDoc", () => {
  it("asks the server for RED per entity, most frequent first", () => {
    const doc = buildEntityDoc(database, range, { key: "n", dir: "desc" });
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

  it("scopes an entity type with spanKindScope to that span kind", () => {
    const doc = buildEntityDoc(service, range, { key: "n", dir: "desc" });
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "span_kind", op: "eq", value: "Server" },
    });
  });

  it("defaults to sorting by count, descending", () => {
    const doc = buildEntityDoc(database, range);
    const orderStage = (doc.pipeline ?? []).find(
      (stage): stage is { order: unknown } => "order" in stage,
    );
    expect(orderStage?.order).toEqual([{ of: "n", dir: "desc" }]);
  });
});

describe("fetchCatalogEntities", () => {
  it("drops the null-identity bucket rather than showing it as a discovered entity", async () => {
    client.setConfig({ baseUrl: "http://localhost" });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            result: "table",
            columns: [],
            rows: [
              // No host.name on most spans — a real gap, not a real host.
              [null, 141, 0, 1_000_000, 14_000_000, "1700000000000000000"],
              ["ip-10-0-1-08", 5, 0, 900_000, 3_000_000, "1700000000000000000"],
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    const result = await fetchCatalogEntities(host, range);

    expect(result.groups).toEqual([
      {
        values: ["ip-10-0-1-08"],
        count: 5,
        errors: 0,
        p50Ms: 0.9,
        p95Ms: 3,
        lastNs: "1700000000000000000",
      },
    ]);
  });
});
