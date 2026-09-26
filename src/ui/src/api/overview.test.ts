import { afterEach, describe, expect, it, vi } from "vitest";
import * as queryIr from "./queryIr";
import {
  buildRecordCountDoc,
  buildSlowestEndpointsDoc,
  buildVersionSightingsDoc,
  decodeEndpoints,
  decodeVersionSightings,
  deploysFromSightings,
  envWhere,
  fetchIngestVolume,
  latestVersions,
  seriesByIdentity,
  type VersionSighting,
} from "./overview";

const RANGE = { fromMs: 1_000_000, toMs: 4_600_000 };
const MS = 1_000_000;

afterEach(() => vi.restoreAllMocks());

describe("environment scope", () => {
  it("is a deployment.environment.name equality, or nothing for all", () => {
    expect(envWhere("")).toEqual([]);
    expect(envWhere("prod")).toEqual([
      {
        where: {
          field: "deployment.environment.name",
          op: "eq",
          value: "prod",
        },
      },
    ]);
  });

  it("scopes the record-count and endpoint queries", () => {
    const counts = buildRecordCountDoc("logs", RANGE, "prod", 120);
    expect(counts.pipeline?.[0]).toEqual(envWhere("prod")[0]);
    expect(counts.pipeline?.[1]).toEqual({
      aggregate: { by: [], aggs: [{ fn: "count", as: "n" }], step: "120s" },
    });
    const endpoints = buildSlowestEndpointsDoc(RANGE, "prod", 6);
    expect(endpoints.pipeline).toEqual(
      expect.arrayContaining([
        { where: { field: "span_kind", op: "eq", value: "Server" } },
        envWhere("prod")[0],
        { order: [{ of: "p95", dir: "desc" }] },
        { limit: 6 },
      ]),
    );
    expect(buildVersionSightingsDoc(RANGE, "").pipeline?.[0]).toEqual({
      where: { field: "service.version", op: "exists" },
    });
  });
});

describe("deploys", () => {
  const s = (
    service: string,
    version: string,
    firstMs: number,
  ): VersionSighting => ({ service, version, firstMs, lastMs: firstMs + 10 });

  it("decodes sightings positionally, skipping malformed rows", () => {
    expect(
      decodeVersionSightings({
        rows: [
          ["cart", "v1", 5 * MS, 9 * MS],
          ["cart", null, 5 * MS, 9 * MS],
        ],
      } as never),
    ).toEqual([{ service: "cart", version: "v1", firstMs: 5, lastMs: 9 }]);
  });

  it("counts every later version of a service as a deploy, never the first", () => {
    const sightings = [
      s("cart", "v2", 300),
      s("cart", "v1", 100),
      s("search", "v9", 50),
      s("checkout", "a", 10),
      s("checkout", "b", 200),
      s("checkout", "c", 250),
    ];
    expect(deploysFromSightings(sightings)).toEqual([
      { service: "checkout", version: "b", atMs: 200 },
      { service: "checkout", version: "c", atMs: 250 },
      { service: "cart", version: "v2", atMs: 300 },
    ]);
    expect(latestVersions(sightings).get("cart")?.version).toBe("v2");
    expect(latestVersions(sightings).get("search")?.version).toBe("v9");
  });
});

describe("decoders", () => {
  it("reads endpoints as name, service, count, p95, p99", () => {
    expect(
      decodeEndpoints({
        rows: [["GET /x", "api", 12, 250 * MS, 900 * MS]],
      } as never),
    ).toEqual([
      { name: "GET /x", service: "api", count: 12, p95Ms: 250, p99Ms: 900 },
    ]);
  });

  it("keys series by the sanitized identity labels", () => {
    const byKey = seriesByIdentity(
      {
        series: [{ labels: { service_name: "cart" }, points: [[2 * MS, 4]] }],
      } as never,
      ["service.name", "service.namespace"],
    );
    expect([...byKey.entries()]).toEqual([
      ["cart\u001f(not set)", [{ tMs: 2, value: 4 }]],
    ]);
  });
});

describe("fetchIngestVolume", () => {
  it("sums histogram metrics into metrics and treats a failing source as empty", async () => {
    vi.spyOn(queryIr, "runIrQuery").mockImplementation(async (doc) => {
      const from = (doc as { from: string }).from;
      if (from === "profiles") throw new Error("profiles disabled");
      const v = { logs: 10, traces: 5, metrics: 2, metrics_histogram: 3 }[
        from
      ]!;
      return { series: [{ labels: {}, points: [[60 * 1e6, v]] }] } as never;
    });
    const series = await fetchIngestVolume(RANGE, "", 60);
    expect(series).toEqual([
      { key: "logs", points: [[60, 10]] },
      { key: "traces", points: [[60, 5]] },
      { key: "metrics", points: [[60, 5]] },
      { key: "profiles", points: [] },
    ]);
  });
});
