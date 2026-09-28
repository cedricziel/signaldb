import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  breakdownFromResponse,
  buildBreakdownDoc,
  buildKpisDoc,
  buildNetworkCorrelateDoc,
  buildNetworkRequestsDoc,
  buildResourcesDoc,
  buildRumAppsDoc,
  buildSessionsOverTimeDoc,
  kpisFromResponse,
  networkRowsFromResponses,
  resourcesFromResponse,
  rumAppsFromResponse,
  sessionsOverTimeFromResponse,
  vitalRowsFromResponse,
  vitalsByName,
} from "./rum";
import { resolveRange } from "../lib/time";

function table(rows: unknown[][]): QueryIrResponse {
  return { result: "table", rows } as QueryIrResponse;
}

/** A multi-query/formula response: one labeled series per `[formula, points]`
 * entry, as `kpisFromResponse`/`sessionsOverTimeFromResponse` expect. */
function multiSeries(entries: [string, [number, number][]][]): QueryIrResponse {
  return {
    result: "series",
    series: entries.map(([formula, points]) => ({
      labels: { formula },
      points,
    })),
  } as unknown as QueryIrResponse;
}

const range = resolveRange(
  { type: "relative", seconds: 3600 },
  1_700_000_000_000,
);

describe("buildRumAppsDoc", () => {
  it("scopes to RUM events (a session.id or a known event_name)", () => {
    const doc = buildRumAppsDoc(range) as { pipeline: unknown[] };
    expect(doc.pipeline[0]).toMatchObject({
      where: {
        or: expect.arrayContaining([{ field: "session.id", op: "exists" }]),
      },
    });
  });
});

describe("rumAppsFromResponse", () => {
  it("decodes apps busiest first, with their sdk language, env and version", () => {
    const res = table([
      ["storefront-web", 184210, "webjs", "production", "2026.09.26-3"],
      ["admin-web", 512, "webjs", "production", "2026.09.20-1"],
    ]);
    expect(rumAppsFromResponse(res)).toEqual([
      {
        serviceName: "storefront-web",
        sdkLanguage: "webjs",
        env: "production",
        version: "2026.09.26-3",
        count: 184210,
      },
      {
        serviceName: "admin-web",
        sdkLanguage: "webjs",
        env: "production",
        version: "2026.09.20-1",
        count: 512,
      },
    ]);
  });

  it("is empty with no RUM apps in the window", () => {
    expect(rumAppsFromResponse(table([]))).toEqual([]);
  });

  it("treats a missing sdk language, env or version as null, not a crash", () => {
    const res = table([["mystery-app", 3, null, null, null]]);
    expect(rumAppsFromResponse(res)[0]?.sdkLanguage).toBeNull();
  });
});

describe("buildKpisDoc", () => {
  const metrics = ["sessions", "sessions_with_errors", "page_views"] as const;

  it("bundles every metric into one multi-query document with an identity formula each", () => {
    const doc = buildKpisDoc("storefront-web", range, 30, metrics);
    expect(Object.keys(doc.queries)).toEqual([...metrics]);
    expect(doc.formulas).toEqual(metrics.map((m) => ({ name: m, expr: m })));
    expect(doc.result).toBe("series");
  });

  it("doubles the window for every sub-query, each with one bucketed aggregate output", () => {
    const doc = buildKpisDoc("storefront-web", range, 30, metrics);
    const span = range.toMs - range.fromMs;
    for (const m of metrics) {
      const sub = doc.queries[m] as {
        range: { from: string; to: string };
        pipeline: { aggregate?: { aggs: unknown[]; step: string } }[];
      };
      expect(Number(sub.range.from)).toBeLessThan(
        Number(sub.range.to) - span * 1_000_000,
      );
      const agg = sub.pipeline.find((s) => s.aggregate)!.aggregate!;
      expect(agg.aggs).toHaveLength(1);
    }
  });

  it("counts distinct sessions for sessions, scoped to exception for sessions_with_errors", () => {
    const doc = buildKpisDoc("storefront-web", range, 30, metrics);
    const sessionsAgg = (
      doc.queries.sessions!.pipeline!.find(
        (s) => (s as { aggregate?: unknown }).aggregate,
      ) as { aggregate: { aggs: { fn?: string; of?: string }[] } }
    ).aggregate.aggs[0]!;
    expect(sessionsAgg).toMatchObject({
      fn: "count_distinct",
      of: "session.id",
    });
    const errAgg = (
      doc.queries.sessions_with_errors!.pipeline!.find(
        (s) => (s as { aggregate?: unknown }).aggregate,
      ) as {
        aggregate: { aggs: { where?: unknown }[] };
      }
    ).aggregate.aggs[0]!;
    expect(errAgg.where).toEqual({
      field: "event_name",
      op: "eq",
      value: "exception",
    });
  });

  it("counts a plain (not distinct) browser.navigation for page_views", () => {
    const doc = buildKpisDoc("storefront-web", range, 30, metrics);
    const agg = (
      doc.queries.page_views!.pipeline!.find(
        (s) => (s as { aggregate?: unknown }).aggregate,
      ) as { aggregate: { aggs: { fn?: string; where?: unknown }[] } }
    ).aggregate.aggs[0]!;
    expect(agg.fn).toBe("count");
    expect(agg.where).toEqual({
      field: "event_name",
      op: "eq",
      value: "browser.navigation",
    });
  });
});

describe("kpisFromResponse", () => {
  const metrics = ["sessions", "sessions_with_errors", "page_views"] as const;

  it("decodes each metric's series by its formula label", () => {
    const res = multiSeries([
      ["sessions", [[1_700_000_000_000_000_000, 12]]],
      ["sessions_with_errors", [[1_700_000_000_000_000_000, 2]]],
      ["page_views", [[1_700_000_000_000_000_000, 40]]],
    ]);
    expect(kpisFromResponse(res, metrics)).toEqual({
      sessions: [{ tMs: 1_700_000_000_000, value: 12 }],
      sessions_with_errors: [{ tMs: 1_700_000_000_000, value: 2 }],
      page_views: [{ tMs: 1_700_000_000_000, value: 40 }],
    });
  });

  it("gives a metric with no matching series an empty array, not a missing key", () => {
    const res = multiSeries([["sessions", [[1_700_000_000_000_000_000, 12]]]]);
    expect(kpisFromResponse(res, metrics)).toEqual({
      sessions: [{ tMs: 1_700_000_000_000, value: 12 }],
      sessions_with_errors: [],
      page_views: [],
    });
  });
});

describe("buildSessionsOverTimeDoc / sessionsOverTimeFromResponse", () => {
  it("bundles total and with_errors into one multi-query document", () => {
    const doc = buildSessionsOverTimeDoc("storefront-web", range, 60);
    expect(Object.keys(doc.queries)).toEqual(["total", "with_errors"]);
    expect(doc.formulas).toEqual([
      { name: "total", expr: "total" },
      { name: "with_errors", expr: "with_errors" },
    ]);
  });

  it("decodes the response into total/withErrors series", () => {
    const res = multiSeries([
      ["total", [[1_700_000_000_000_000_000, 20]]],
      ["with_errors", [[1_700_000_000_000_000_000, 3]]],
    ]);
    expect(sessionsOverTimeFromResponse(res)).toEqual({
      total: [{ tMs: 1_700_000_000_000, value: 20 }],
      withErrors: [{ tMs: 1_700_000_000_000, value: 3 }],
    });
  });
});

describe("buildBreakdownDoc", () => {
  it("groups by the given field", () => {
    const doc = buildBreakdownDoc(
      "storefront-web",
      range,
      "resource.browser.mobile",
    ) as { pipeline: { aggregate?: { by: string[] } }[] };
    const agg = doc.pipeline.find((s) => s.aggregate)!.aggregate!;
    expect(agg.by).toEqual(["resource.browser.mobile"]);
  });

  it("requires the field to exist only when asked", () => {
    const withField = buildBreakdownDoc(
      "storefront-web",
      range,
      "resource.browser.brands",
      { requireField: true },
    ) as { pipeline: { where?: { field?: string; op?: string } }[] };
    expect(
      withField.pipeline.some(
        (s) =>
          s.where?.field === "resource.browser.brands" &&
          s.where.op === "exists",
      ),
    ).toBe(true);

    const withoutField = buildBreakdownDoc(
      "storefront-web",
      range,
      "resource.browser.mobile",
    ) as { pipeline: { where?: { field?: string } }[] };
    expect(
      withoutField.pipeline.some(
        (s) => s.where?.field === "resource.browser.mobile",
      ),
    ).toBe(false);
  });
});

describe("buildNetworkRequestsDoc / buildNetworkCorrelateDoc", () => {
  it("scopes the totals read to the app's client spans, grouped by method/url/server", () => {
    const doc = buildNetworkRequestsDoc("storefront-web", range) as {
      from: string;
      pipeline: { where?: unknown; aggregate?: { by: string[] } }[];
    };
    expect(doc.from).toBe("traces");
    expect(doc.pipeline[0]!.where).toMatchObject({
      and: expect.arrayContaining([
        { field: "service.name", op: "eq", value: "storefront-web" },
        { field: "span_kind", op: "eq", value: "Client" },
      ]),
    });
    const agg = doc.pipeline.find(
      (s) => (s as { aggregate?: unknown }).aggregate,
    )!.aggregate!;
    expect(agg.by).toEqual([
      "http.request.method",
      "url.full",
      "server.address",
    ]);
  });

  it("joins to the server-kind child via correlate, scoped to the app's client spans", () => {
    const doc = buildNetworkCorrelateDoc("storefront-web", range) as {
      pipeline: { correlate?: unknown; where?: { and?: unknown[] } }[];
    };
    expect(doc.pipeline[0]!.correlate).toEqual({ to: "parent", kind: "inner" });
    expect(doc.pipeline[1]!.where?.and).toEqual(
      expect.arrayContaining([
        { field: "parent.service.name", op: "eq", value: "storefront-web" },
        { field: "parent.span_kind", op: "eq", value: "Client" },
        { field: "span_kind", op: "eq", value: "Server" },
      ]),
    );
  });

  it("counts distinct client spans, not their server children, for the traced count", () => {
    const doc = buildNetworkCorrelateDoc("storefront-web", range) as {
      irVersion: number;
      pipeline: { aggregate?: { aggs: { fn?: string; of?: string }[] } }[];
    };
    expect(doc.irVersion).toBe(9);
    const agg = doc.pipeline.find((s) => s.aggregate)!.aggregate!;
    expect(agg.aggs[0]).toMatchObject({
      fn: "count_distinct",
      of: "parent.span_id",
    });
  });
});

describe("networkRowsFromResponses", () => {
  const totals = table([
    [
      "GET",
      "https://api.example.com/orders/48213",
      "api.example.com",
      100,
      240_000_000,
      4,
    ],
    [
      "GET",
      "https://api.example.com/orders/91820",
      "api.example.com",
      50,
      260_000_000,
      0,
    ],
    [
      "GET",
      "https://reviews.partner-cdn.com/widget",
      "reviews.partner-cdn.com",
      30,
      500_000_000,
      0,
    ],
  ]);
  const traced = table([
    [
      "GET",
      "https://api.example.com/orders/48213",
      "api.example.com",
      "orders-svc",
      90,
      120_000_000,
    ],
    [
      "GET",
      "https://api.example.com/orders/91820",
      "api.example.com",
      "orders-svc",
      50,
      130_000_000,
    ],
  ]);

  it("merges rows sharing the same method/origin/URL template, summing counts", () => {
    const rows = networkRowsFromResponses(totals, traced);
    const merged = rows.find((r) => r.template === "/orders/:id")!;
    expect(merged.calls).toBe(150);
    expect(merged.tracedCalls).toBe(140);
    expect(merged.errorCalls).toBe(4);
    expect(merged.backendService).toBe("orders-svc");
    expect(merged.totalP75Ms).toBeCloseTo(246.67, 1);
    expect(merged.backendP75Ms).toBeCloseTo(123.57, 1);
  });

  it("marks a group with zero traced calls as untraced, not errored", () => {
    const rows = networkRowsFromResponses(totals, traced);
    const cdn = rows.find((r) => r.origin === "reviews.partner-cdn.com")!;
    expect(cdn.tracedCalls).toBe(0);
    expect(cdn.backendService).toBeUndefined();
  });

  it("sums traced rows for one URL served by several backend services", () => {
    const split = table([
      [
        "GET",
        "https://api.example.com/cart",
        "api.example.com",
        "cart-v1",
        20,
        100_000_000,
      ],
      [
        "GET",
        "https://api.example.com/cart",
        "api.example.com",
        "cart-v2",
        60,
        200_000_000,
      ],
    ]);
    const totalsCart = table([
      [
        "GET",
        "https://api.example.com/cart",
        "api.example.com",
        100,
        300_000_000,
        0,
      ],
    ]);
    const [row] = networkRowsFromResponses(totalsCart, split);
    expect(row!.tracedCalls).toBe(80);
    expect(row!.backendService).toBe("cart-v2");
    expect(row!.backendP75Ms).toBeCloseTo(175, 1);
  });

  it("marks unmatched rows as unknown, not untraced, when the correlate read overflows its cap", () => {
    const overflow = table(
      Array.from({ length: 501 }, (_, i) => [
        "GET",
        `https://api.example.com/other/${i}`,
        "api.example.com",
        "other-svc",
        1,
        1_000_000,
      ]),
    );
    const rows = networkRowsFromResponses(totals, overflow);
    const cdn = rows.find((r) => r.origin === "reviews.partner-cdn.com")!;
    expect(cdn.tracedKnown).toBe(false);
    expect(networkRowsFromResponses(totals, traced)[0]!.tracedKnown).toBe(true);
  });

  it("flags the telemetry export endpoint as an SDK export", () => {
    const withExport = table([
      [
        "POST",
        "https://api.example.com/v1/traces",
        "api.example.com",
        500,
        15_000_000,
        0,
      ],
    ]);
    const rows = networkRowsFromResponses(withExport, table([]));
    expect(rows[0]!.isSdkExport).toBe(true);
  });
});

describe("buildResourcesDoc / resourcesFromResponse", () => {
  it("aggregates browser.resource_timing by initiator type", () => {
    const doc = buildResourcesDoc("storefront-web", range) as {
      pipeline: { aggregate?: { by: string[]; aggs: { fn: string }[] } }[];
    };
    const agg = doc.pipeline.find(
      (s) => (s as { aggregate?: unknown }).aggregate,
    )!.aggregate!;
    expect(agg.by).toEqual(["browser.resource_timing.initiator_type"]);
    expect(agg.aggs.map((a) => a.fn)).toEqual([
      "count",
      "sum",
      "quantile",
      "max",
    ]);
  });

  it("decodes count, transfer size, p75 duration and the largest transfer", () => {
    const res = table([["script", 1200, 48_000_000, 210, 812_000]]);
    expect(resourcesFromResponse(res)).toEqual([
      {
        initiatorType: "script",
        count: 1200,
        transferBytes: 48_000_000,
        p75Ms: 210,
        maxTransferBytes: 812_000,
      },
    ]);
  });
});

describe("vitalRowsFromResponse / vitalsByName", () => {
  it("decodes (name, rating) rows into a lowercase-name, rating-count map", () => {
    const res = table([
      ["lcp", "good", 68, 1697.75],
      ["lcp", "needs-improvement", 22, null],
      ["lcp", "poor", 10, null],
    ]);
    const byName = vitalsByName(vitalRowsFromResponse(res));
    expect(byName.get("lcp")?.counts).toEqual({
      good: 68,
      "needs-improvement": 22,
      poor: 10,
    });
  });

  it("has no entry for a vital with zero records", () => {
    expect(vitalsByName([]).get("inp")).toBeUndefined();
  });
});

describe("breakdownFromResponse", () => {
  it("decodes a group-by-one-dimension breakdown", () => {
    const res = table([
      [false, 300],
      [true, 40],
    ]);
    expect(breakdownFromResponse(res)).toEqual([
      { value: "false", count: 300 },
      { value: "true", count: 40 },
    ]);
  });

  it('keeps a null dimension as null, not the string "null"', () => {
    const res = table([[null, 5]]);
    expect(breakdownFromResponse(res)).toEqual([{ value: null, count: 5 }]);
  });
});
