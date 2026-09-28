// The Pages tab's decoders (task 2.1): per-route views/vitals/errors merged
// into one row, and the poor-share sort the route list uses.
import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  pagesFromResponses,
  routePoorShare,
  sortPagesByPoorShare,
  type RumPageRow,
} from "./rum";

function table(rows: unknown[][]): QueryIrResponse {
  return { result: "table", rows } as QueryIrResponse;
}

describe("pagesFromResponses", () => {
  const views = table([
    ["/orders/:id", "https://shop.example.com/orders/48213", 400],
    [null, "https://shop.example.com/orders/91820", 100],
    [null, null, 12],
  ]);
  const vitalRatings = table([
    [
      "/orders/:id",
      "https://shop.example.com/orders/48213",
      "lcp",
      "good",
      300,
    ],
    [
      "/orders/:id",
      "https://shop.example.com/orders/48213",
      "lcp",
      "poor",
      100,
    ],
  ]);
  const vitalP75 = table([
    ["/orders/:id", "https://shop.example.com/orders/48213", "lcp", 3100],
  ]);
  const errors = table([["/orders/:id", 40]]);

  it("merges every page-scoped route by url.template, falling back to url.full's derived template", () => {
    const rows = pagesFromResponses(views, vitalRatings, vitalP75, errors);
    const orders = rows.find((r) => r.route === "/orders/:id");
    expect(orders?.views).toBe(500);
  });

  it("buckets a record with neither field as the missing-route row", () => {
    const rows = pagesFromResponses(views, vitalRatings, vitalP75, errors);
    const missing = rows.find((r) => r.route === null);
    expect(missing?.views).toBe(12);
  });

  it("decodes the per-route vital p75 and rating counts", () => {
    const rows = pagesFromResponses(views, vitalRatings, vitalP75, errors);
    const orders = rows.find((r) => r.route === "/orders/:id");
    const lcp = orders?.vitals.get("lcp");
    expect(lcp?.p75).toBe(3100);
    expect(lcp?.counts).toEqual({ good: 300, poor: 100 });
  });

  it("computes error share only for routes with an explicit url.template on their exceptions", () => {
    const rows = pagesFromResponses(views, vitalRatings, vitalP75, errors);
    const orders = rows.find((r) => r.route === "/orders/:id");
    expect(orders?.errorShare).toBeCloseTo(40 / 500);
    const missing = rows.find((r) => r.route === null);
    expect(missing?.errorShare).toBeNull();
  });
});

describe("routePoorShare / sortPagesByPoorShare", () => {
  function page(route: string, poor: number, good: number): RumPageRow {
    return {
      route,
      views: poor + good,
      vitals: new Map([["lcp", { p75: 3000, counts: { good, poor } }]]),
      errorShare: null,
    };
  }

  it("is the worst vital's poor share across a route's vitals", () => {
    const row: RumPageRow = {
      route: "/checkout",
      views: 100,
      vitals: new Map([
        ["lcp", { p75: 3000, counts: { good: 80, poor: 20 } }],
        ["inp", { p75: 600, counts: { good: 40, poor: 60 } }],
      ]),
      errorShare: null,
    };
    expect(routePoorShare(row)).toBeCloseTo(0.6);
  });

  it("sorts routes worst-poor-share first", () => {
    const rows = [page("/a", 10, 90), page("/b", 60, 40), page("/c", 0, 100)];
    expect(sortPagesByPoorShare(rows).map((r) => r.route)).toEqual([
      "/b",
      "/a",
      "/c",
    ]);
  });
});
