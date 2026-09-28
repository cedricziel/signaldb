import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  buildSessionsListDoc,
  filterSessionsRows,
  sessionsFromResponse,
  type RumSessionRow,
} from "./rumSessions";
import { resolveRange } from "../lib/time";

function table(rows: unknown[][]): QueryIrResponse {
  return { result: "table", rows } as QueryIrResponse;
}

const range = resolveRange(
  { type: "relative", seconds: 3600 },
  1_700_000_000_000,
);

describe("buildSessionsListDoc", () => {
  it("scopes to the app and to records carrying a session.id", () => {
    const doc = buildSessionsListDoc("storefront-web", range) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "storefront-web" },
    });
    expect(doc.pipeline[1]).toEqual({
      where: { field: "session.id", op: "exists" },
    });
  });

  it("groups by session.id with the verified aggregate outputs", () => {
    const doc = buildSessionsListDoc("storefront-web", range) as {
      pipeline: { aggregate?: { by: string[]; aggs: { as: string }[] } }[];
    };
    const agg = doc.pipeline.find((s) => s.aggregate)!.aggregate!;
    expect(agg.by).toEqual(["session.id"]);
    expect(agg.aggs.map((a) => a.as)).toEqual([
      "first_ts",
      "last_ts",
      "views",
      "errors",
      "slow",
      "entry",
      "exit",
      "user",
      "ua",
      "mobile",
    ]);
  });
});

describe("sessionsFromResponse", () => {
  it("decodes rows, converting timestamps to ms and deriving duration", () => {
    const rows = sessionsFromResponse(
      table([
        [
          "sess-1",
          1_700_000_000_000_000_000,
          1_700_000_060_000_000_000,
          3,
          1,
          1,
          "/checkout",
          "/thanks",
          "user-42",
          "Mozilla/5.0 Chrome/128.0.0.0 Safari/537.36",
          false,
        ],
      ]),
    );
    expect(rows).toEqual<RumSessionRow[]>([
      {
        sessionId: "sess-1",
        firstMs: 1_700_000_000_000,
        lastMs: 1_700_000_060_000,
        durationMs: 60_000,
        views: 3,
        errors: 1,
        slow: 1,
        entry: "/checkout",
        exit: "/thanks",
        userId: "user-42",
        browser: "Chrome",
        mobile: false,
      },
    ]);
  });

  it("shows a null entry/exit when first/last() resolved to null", () => {
    const rows = sessionsFromResponse(
      table([
        [
          "sess-2",
          1_700_000_000_000_000_000,
          1_700_000_000_000_000_000,
          0,
          0,
          0,
          null,
          null,
          null,
          null,
          null,
        ],
      ]),
    );
    expect(rows[0]).toMatchObject({
      entry: null,
      exit: null,
      userId: null,
      browser: null,
      mobile: null,
    });
  });
});

describe("filterSessionsRows", () => {
  const base: RumSessionRow = {
    sessionId: "s",
    firstMs: 0,
    lastMs: 0,
    durationMs: 0,
    views: 1,
    errors: 0,
    slow: 0,
    entry: null,
    exit: null,
    userId: null,
    browser: null,
    mobile: null,
  };
  const rows: RumSessionRow[] = [
    { ...base, sessionId: "clean" },
    { ...base, sessionId: "errored", errors: 2 },
    { ...base, sessionId: "slow", slow: 1 },
  ];

  it("with no filters, keeps every session", () => {
    expect(filterSessionsRows(rows, {}).map((r) => r.sessionId)).toEqual([
      "clean",
      "errored",
      "slow",
    ]);
  });

  it("'With errors' keeps only sessions with at least one exception", () => {
    expect(
      filterSessionsRows(rows, { onlyErrors: true }).map((r) => r.sessionId),
    ).toEqual(["errored"]);
  });

  it("'Slow load' keeps only sessions with at least one poor LCP", () => {
    expect(
      filterSessionsRows(rows, { onlySlow: true }).map((r) => r.sessionId),
    ).toEqual(["slow"]);
  });
});
