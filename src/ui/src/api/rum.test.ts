import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  breakdownFromResponse,
  buildBreakdownDoc,
  buildKpisDoc,
  buildRumAppsDoc,
  buildSessionsOverTimeDoc,
  kpisFromResponse,
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
