import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchDependencyBreakdown } from "./dependencyBreakdown";
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

function tableResponse(row: [string, number, number] | null) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [
      { name: "service.name", type: "string" },
      { name: "total", type: "number" },
      { name: "n", type: "number" },
    ],
    rows: row ? [row] : [],
  };
}

describe("fetchDependencyBreakdown", () => {
  it("sums duration per category and derives (other) from the baseline remainder", async () => {
    // Baseline (all CLIENT spans): 1000ns/10 calls. database: 400ns/4.
    // http: 300ns/3. rpc and messaging: nothing. other = 1000-700=300ns,
    // 10-7=3 calls.
    let call = 0;
    const responses = [
      tableResponse(["checkout", 1000, 10]), // baseline
      tableResponse(["checkout", 400, 4]), // database
      tableResponse(["checkout", 300, 3]), // http
      tableResponse(null), // rpc
      tableResponse(null), // messaging
    ];
    const fetchMock = vi.fn().mockImplementation(() => {
      const body = responses[call % responses.length];
      call += 1;
      return Promise.resolve(jsonResponse(body));
    });
    vi.stubGlobal("fetch", fetchMock);

    const categories = await fetchDependencyBreakdown("checkout", {
      fromMs: 0,
      toMs: 60_000,
    });

    expect(categories).toEqual([
      { key: "database", label: "Database", durationNs: 400, count: 4 },
      { key: "http", label: "HTTP", durationNs: 300, count: 3 },
      { key: "other", label: "Other", durationNs: 300, count: 3 },
    ]);
  });

  it("filters each category query by service, CLIENT kind, and attribute existence", async () => {
    // A fresh Response per call — parallel requests reading the same
    // instance's body would race, and only the first .json() would win.
    const fetchMock = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(jsonResponse(tableResponse(null))),
      );
    vi.stubGlobal("fetch", fetchMock);

    await fetchDependencyBreakdown("checkout", { fromMs: 0, toMs: 60_000 });

    const bodies = await Promise.all(
      fetchMock.mock.calls.map((c) => (c[0] as Request).clone().json()),
    );
    const databaseQuery = bodies.find((b) =>
      b.pipeline.some(
        (stage: Record<string, unknown>) =>
          (stage.where as { field?: string } | undefined)?.field ===
          "db.system.name",
      ),
    );
    expect(databaseQuery.pipeline).toContainEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
    expect(databaseQuery.pipeline).toContainEqual({
      where: { field: "span_kind", op: "eq", value: "Client" },
    });
    expect(databaseQuery.pipeline).toContainEqual({
      where: { field: "db.system.name", op: "exists" },
    });
  });

  it("returns nothing when there's no dependency traffic at all", async () => {
    // A fresh Response per call — parallel requests reading the same
    // instance's body would race, and only the first .json() would win.
    const fetchMock = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(jsonResponse(tableResponse(null))),
      );
    vi.stubGlobal("fetch", fetchMock);

    const categories = await fetchDependencyBreakdown("idle-service", {
      fromMs: 0,
      toMs: 60_000,
    });
    expect(categories).toEqual([]);
  });
});
