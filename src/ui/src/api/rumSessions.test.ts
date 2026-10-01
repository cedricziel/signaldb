import { afterEach, describe, expect, it, vi } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  buildSessionIdLookupDoc,
  buildSessionLookupDoc,
  buildSessionsListDoc,
  fetchSessions,
  filterSessionsRows,
  sessionIdsFromResponse,
  sessionLookupFromResponse,
  sessionsFromResponse,
  sessionsTextFilterWhere,
  type RumSessionRow,
} from "./rumSessions";
import * as queryIr from "./queryIr";
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

  it("has no extra where stage with no session.id filter", () => {
    const doc = buildSessionsListDoc("storefront-web", range) as {
      pipeline: unknown[];
    };
    expect(doc.pipeline).toHaveLength(4);
  });

  it("scopes to session.id in [...] when a filter narrowed the sessions, with no attribute where", () => {
    const doc = buildSessionsListDoc("storefront-web", range, [
      "sess-1",
      "sess-2",
    ]) as { pipeline: Record<string, unknown>[] };
    expect(doc.pipeline).toHaveLength(5);
    expect(doc.pipeline[2]).toEqual({
      where: { field: "session.id", op: "in", value: ["sess-1", "sess-2"] },
    });
  });
});

describe("buildSessionIdLookupDoc", () => {
  it("is null for blank filter text — no lookup read when there's nothing to filter", () => {
    expect(buildSessionIdLookupDoc("storefront-web", range, "  ")).toBeNull();
  });

  it("scopes to the app, the text filter, and aggregates down to session ids", () => {
    const doc = buildSessionIdLookupDoc(
      "storefront-web",
      range,
      "user.plan=enterprise",
    ) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "storefront-web" },
    });
    expect(doc.pipeline[1]).toEqual({
      where: { field: "session.id", op: "exists" },
    });
    // The attribute filter narrows which *records* count toward a
    // session's inclusion — unlike the list read, this pipeline never
    // aggregates anything but the id itself, so filtering records here
    // can't corrupt a views/duration/entry/exit figure the way it would
    // on the list aggregate.
    expect(doc.pipeline[2]).toEqual({
      where: { field: "user.plan", op: "eq", value: "enterprise" },
    });
    const agg = (
      doc.pipeline.find((s) => s.aggregate) as {
        aggregate: { by: string[]; aggs: { as: string }[] };
      }
    ).aggregate;
    expect(agg.by).toEqual(["session.id"]);
    expect(agg.aggs.map((a) => a.as)).toEqual(["last_ts"]);
    expect(doc.pipeline.find((s) => s.topk)).toEqual({
      topk: { n: 100, of: "last_ts" },
    });
  });
});

describe("sessionIdsFromResponse", () => {
  it("extracts the session.id column", () => {
    expect(
      sessionIdsFromResponse(
        table([
          ["sess-1", 1],
          ["sess-2", 2],
        ]),
      ),
    ).toEqual(["sess-1", "sess-2"]);
  });

  it("is empty for no rows", () => {
    expect(sessionIdsFromResponse(table([]))).toEqual([]);
  });
});

describe("sessionsTextFilterWhere", () => {
  it("returns null for blank input", () => {
    expect(sessionsTextFilterWhere("")).toBeNull();
    expect(sessionsTextFilterWhere("   ")).toBeNull();
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

describe("fetchSessions", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("issues one request when no filter is set", async () => {
    const spy = vi
      .spyOn(queryIr, "runIrQuery")
      .mockResolvedValue(table([]) as never);

    await fetchSessions("storefront-web", range);

    expect(spy).toHaveBeenCalledTimes(1);
  });

  it("looks up matching session ids first, then scopes the list read to them", async () => {
    const spy = vi
      .spyOn(queryIr, "runIrQuery")
      .mockResolvedValueOnce(table([["sess-1", 1]]) as never)
      .mockResolvedValueOnce(table([]) as never);

    await fetchSessions("storefront-web", range, "user.plan=enterprise");

    expect(spy).toHaveBeenCalledTimes(2);
    const lookupDoc = spy.mock.calls[0]![0] as {
      pipeline: Record<string, unknown>[];
    };
    expect(lookupDoc.pipeline[2]).toEqual({
      where: { field: "user.plan", op: "eq", value: "enterprise" },
    });
    const listDoc = spy.mock.calls[1]![0] as {
      pipeline: Record<string, unknown>[];
    };
    expect(listDoc.pipeline).toContainEqual({
      where: { field: "session.id", op: "in", value: ["sess-1"] },
    });
    // The list read carries no attribute where of its own — the filter
    // already did its job selecting sessions in the first read.
    expect(listDoc.pipeline).not.toContainEqual({
      where: { field: "user.plan", op: "eq", value: "enterprise" },
    });
  });

  it("skips the list read entirely when nothing matched the filter", async () => {
    const spy = vi
      .spyOn(queryIr, "runIrQuery")
      .mockResolvedValueOnce(table([]) as never);

    const rows = await fetchSessions(
      "storefront-web",
      range,
      "user.plan=enterprise",
    );

    expect(rows).toEqual([]);
    expect(spy).toHaveBeenCalledTimes(1);
  });
});

describe("buildSessionLookupDoc", () => {
  it("filters to the exact session.id and caps at one row", () => {
    const doc = buildSessionLookupDoc("sess-abc123", range) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "session.id", op: "eq", value: "sess-abc123" },
    });
    expect(doc.pipeline[2]).toEqual({ limit: 1 });
  });
});

describe("sessionLookupFromResponse", () => {
  it("returns the session's app when found", () => {
    expect(sessionLookupFromResponse(table([["storefront-web", 12]]))).toBe(
      "storefront-web",
    );
  });

  it("returns null when the session doesn't exist", () => {
    expect(sessionLookupFromResponse(table([]))).toBeNull();
  });
});
