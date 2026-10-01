// The Pages and Interactions tabs' decoders (tasks 2.1/2.3): per-route
// views/vitals/errors, navigation-timing breakdown blending, backend calls
// and click-target aggregation.
import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  backendCallsFromResponse,
  interactionsFromResponse,
  joinBackendCallsToNetworkService,
  loadBreakdownFromResponse,
  pagesFromResponses,
  routePoorShare,
  sortPagesByPoorShare,
  type RumPageRow,
  type RumRequestRow,
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
describe("loadBreakdownFromResponse", () => {
  const res = table([
    [
      "/orders/:id",
      "https://shop.example.com/orders/48213",
      300,
      10,
      40,
      50,
      90,
      120,
      260,
      280,
      420,
      600,
      620,
    ],
    [
      "/orders/:id",
      "https://shop.example.com/orders/91820",
      100,
      20,
      50,
      60,
      100,
      140,
      280,
      300,
      440,
      620,
      640,
    ],
    [
      "/cart",
      "https://shop.example.com/cart",
      50,
      5,
      15,
      20,
      30,
      40,
      60,
      70,
      90,
      110,
      130,
    ],
  ]);

  it("blends every url.full whose resolved route matches, weighted by navigation count", () => {
    const blended = loadBreakdownFromResponse(res, "/orders/:id");
    expect(blended).toBeDefined();
    // weighted mean of domain_lookup_start (10@300, 20@100) = 12.5
    expect(blended!.domainLookupStart).toBeCloseTo(12.5);
  });

  it("leaves a boundary with no recorded value undefined, not zero", () => {
    const sparse = table([
      ["/cart", null, 10, 5, 15, 20, 30, 40, 60, null, 90, 110, 130],
    ]);
    const blended = loadBreakdownFromResponse(sparse, "/cart");
    expect(blended!.responseEnd).toBeUndefined();
    expect(blended!.domInteractive).toBe(90);
  });

  it("returns undefined for a route with no navigation-timing record", () => {
    expect(loadBreakdownFromResponse(res, "/checkout")).toBeUndefined();
  });
});

describe("backendCallsFromResponse", () => {
  const res = table([
    ["https://api.example.com/api/orders/48213", 100, 220],
    ["https://api.example.com/api/orders/91820", 40, 260],
    ["https://api.example.com/api/cart", 50, 80],
  ]);

  it("templates the request URL and merges matching requests", () => {
    const rows = backendCallsFromResponse(res);
    expect(rows.map((r) => [r.template, r.calls])).toEqual([
      ["/api/orders/:id", 140],
      ["/api/cart", 50],
    ]);
  });

  it("weights the merged p75 by call count", () => {
    const rows = backendCallsFromResponse(res);
    // (220*100 + 260*40) / 140 = 231.43
    expect(rows[0]!.p75Ms).toBeCloseTo(231.43, 1);
  });
});

describe("joinBackendCallsToNetworkService", () => {
  it("matches by (origin, template) against the Network tab's own rows", () => {
    const networkRows: RumRequestRow[] = [
      {
        method: "GET",
        origin: "api.example.com",
        template: "/api/orders/:id",
        calls: 140,
        tracedCalls: 140,
        errorCalls: 0,
        totalP75Ms: 230,
        backendService: "orders-svc",
        isSdkExport: false,
        tracedKnown: true,
      },
    ];
    const joined = joinBackendCallsToNetworkService(
      [
        {
          origin: "api.example.com",
          template: "/api/orders/:id",
          calls: 140,
          p75Ms: 230,
        },
      ],
      networkRows,
    );
    expect(joined[0]!.backendService).toBe("orders-svc");
  });

  it("leaves the service unknown when the path is served by several", () => {
    const row = (method: string, backendService: string): RumRequestRow => ({
      method,
      origin: "api.example.com",
      template: "/api/orders/:id",
      calls: 10,
      tracedCalls: 10,
      errorCalls: 0,
      totalP75Ms: 100,
      backendService,
      isSdkExport: false,
      tracedKnown: true,
    });
    const joined = joinBackendCallsToNetworkService(
      [
        {
          origin: "api.example.com",
          template: "/api/orders/:id",
          calls: 1,
          p75Ms: 1,
        },
      ],
      [row("GET", "orders-read"), row("DELETE", "orders-write")],
    );
    expect(joined[0]!.backendService).toBeUndefined();
  });
});

describe("interactionsFromResponse", () => {
  const res = table([
    [
      "/orders/:id",
      "https://shop.example.com/orders/48213",
      "body > div.app > button.buy",
      "button",
      42,
    ],
    [null, "https://shop.example.com/orders/91820", null, "a", 8],
  ]);

  it("prefers the css selector target over the tag name", () => {
    const rows = interactionsFromResponse(res);
    const row = rows.find((r) => r.route === "/orders/:id");
    expect(row?.target).toBe("body > div.app > button.buy");
  });

  it("falls back to tag name and resolves the route from url.full", () => {
    const rows = interactionsFromResponse(res);
    const row = rows.find((r) => r.target === "a");
    expect(row?.route).toBe("/orders/:id");
    expect(row?.clicks).toBe(8);
  });
});
