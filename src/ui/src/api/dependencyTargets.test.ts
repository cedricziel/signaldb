import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchDependencyTargets } from "./dependencyTargets";
import { client } from "./gen/client.gen";

beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function totalsResponse(total: number, n: number) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [
      { name: "service.name", type: "string" },
      { name: "total", type: "number" },
      { name: "n", type: "number" },
    ],
    rows: [["checkout", total, n]],
  };
}

function kindResponse(
  rows: [string | null, string | null, string | null, number, number, number][],
) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [
      { name: "service.name", type: "string" },
      { name: "target", type: "string" },
      { name: "op1", type: "string" },
      { name: "op2", type: "string" },
      { name: "total", type: "number" },
      { name: "n", type: "number" },
      { name: "p95", type: "number" },
    ],
    rows: rows.map(([target, op1, op2, total, n, p95]) => [
      "checkout",
      target,
      op1,
      op2,
      total,
      n,
      p95,
    ]),
  };
}

/** Request order matches `fetchDependencyTargets`: server totals, client
 * totals, then one query per kind (database, http, rpc, messaging). */
function mockSequence(bodies: unknown[]) {
  let call = 0;
  const fetchMock = vi.fn().mockImplementation(() => {
    const body = bodies[call % bodies.length];
    call += 1;
    return Promise.resolve(jsonResponse(body));
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("fetchDependencyTargets", () => {
  it("returns a row per target, request totals, and self time", async () => {
    mockSequence([
      totalsResponse(1_000, 100), // server: 1000ns total, 100 requests
      totalsResponse(400, 40), // client baseline: 400ns downstream
      kindResponse([["orders-db", "postgresql", "SELECT", 300, 30, 12]]), // database
      kindResponse([]), // http
      kindResponse([]), // rpc
      kindResponse([]), // messaging
    ]);

    const result = await fetchDependencyTargets("checkout", {
      fromMs: 0,
      toMs: 60_000,
    });

    expect(result.requestDurationNs).toBe(1_000);
    expect(result.requestCount).toBe(100);
    expect(result.selfDurationNs).toBe(600); // 1000 - 400
    expect(result.rows).toEqual([
      {
        key: "database:orders-db:postgresql · SELECT:0",
        kind: "database",
        target: "orders-db",
        operation: "postgresql · SELECT",
        durationNs: 300,
        count: 30,
        p95Ns: 12,
      },
    ]);
  });

  it("falls back to a single kind-labelled row when the target attribute is missing", async () => {
    mockSequence([
      totalsResponse(1_000, 100),
      totalsResponse(200, 20),
      kindResponse([]), // database
      kindResponse([[null, "GET", null, 200, 20, 8]]), // http, no server.address
      kindResponse([]), // rpc
      kindResponse([]), // messaging
    ]);

    const result = await fetchDependencyTargets("checkout", {
      fromMs: 0,
      toMs: 60_000,
    });

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({
      kind: "http",
      target: "HTTP",
      operation: "GET",
    });
  });

  it("filters each kind query by service, CLIENT kind, and its attribute", async () => {
    mockSequence([
      totalsResponse(0, 0),
      totalsResponse(0, 0),
      kindResponse([]),
    ]);

    await fetchDependencyTargets("checkout", { fromMs: 0, toMs: 60_000 });

    const fetchMock = vi.mocked(globalThis.fetch);
    const bodies = await Promise.all(
      fetchMock.mock.calls.map((c) => (c[0] as Request).clone().json()),
    );
    const dbQuery = bodies.find((b) =>
      b.pipeline.some(
        (stage: Record<string, unknown>) =>
          (stage.where as { field?: string } | undefined)?.field ===
          "db.system.name",
      ),
    );
    expect(dbQuery.pipeline).toContainEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
    expect(dbQuery.pipeline).toContainEqual({
      where: { field: "span_kind", op: "eq", value: "Client" },
    });
    expect(dbQuery.pipeline[3]).toEqual({
      aggregate: {
        by: [
          "service.name",
          "db.namespace",
          "db.system.name",
          "db.operation.name",
        ],
        aggs: [
          { fn: "sum", of: "duration", as: "total" },
          { fn: "count", as: "n" },
          { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
        ],
      },
    });
  });
});
