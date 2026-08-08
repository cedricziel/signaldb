import { describe, expect, it } from "vitest";
import {
  ERROR_PATTERN,
  GROUP_BUDGET,
  ROOT_SPAN_SENTINEL,
  buildGroupDoc,
  groupsFromIrResponse,
} from "./traceGroups";

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

describe("buildGroupDoc", () => {
  it("asks the server for RED per group, most frequent first", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans");
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "table",
      pipeline: [
        {
          aggregate: {
            by: ["span.name"],
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
              { fn: "quantile", of: "duration.nanos", arg: 0.5, as: "p50" },
              { fn: "quantile", of: "duration.nanos", arg: 0.95, as: "p95" },
              { fn: "max", of: "start_time_unix_nano", as: "last" },
            ],
          },
        },
        { order: [{ of: "n", dir: "desc" }] },
        // One more than displayed, so truncation is detectable.
        { limit: GROUP_BUDGET + 1 },
      ],
    });
  });

  it("counts traces by restricting the trace grain to root spans", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "traces");
    expect(doc.pipeline?.[0]).toEqual({
      where: {
        field: "parent_span_id",
        op: "eq",
        value: ROOT_SPAN_SENTINEL,
      },
    });
  });

  it("omits the root predicate at span grain", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans");
    expect(JSON.stringify(doc.pipeline)).not.toContain("parent_span_id");
  });

  it("groups by every picked dimension, in order", () => {
    const doc = buildGroupDoc(
      ["span.name", "service.name"],
      range,
      [],
      "spans",
    );
    const stage = doc.pipeline?.[0] as {
      aggregate: { by: string[] };
    };
    expect(stage.aggregate.by).toEqual(["span.name", "service.name"]);
  });

  it("applies the active filters so the aggregates match what is shown", () => {
    const doc = buildGroupDoc(
      ["span.name"],
      range,
      [{ field: "service.name", value: "checkout" }],
      "spans",
    );
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
  });

  // Sorting is part of the query, not a re-sort of the page: the page holds
  // the top GROUP_BUDGET groups *under the current sort*, so ranking it
  // locally would answer "slowest of the most frequent", not "slowest".
  it("orders by count, descending, by default", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans");
    expect(doc.pipeline?.at(-2)).toEqual({ order: [{ of: "n", dir: "desc" }] });
  });

  it("pushes a p95 sort into the query", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans", {
      key: "p95",
      dir: "desc",
    });
    expect(doc.pipeline?.at(-2)).toEqual({
      order: [{ of: "p95", dir: "desc" }],
    });
  });

  it("orders by a grouping dimension by name", () => {
    const doc = buildGroupDoc(
      ["span.name", "service.name"],
      range,
      [],
      "spans",
      {
        key: "dim:1",
        dir: "asc",
      },
    );
    expect(doc.pipeline?.at(-2)).toEqual({
      order: [{ of: "service.name", dir: "asc" }],
    });
  });

  it("falls back to the count for an unknown sort key", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans", {
      key: "sideways",
      dir: "asc",
    });
    expect(doc.pipeline?.at(-2)).toEqual({ order: [{ of: "n", dir: "asc" }] });
  });

  // The whole point of the change: the budget buys groups, and the aggregates
  // are computed server-side over the window regardless of it.
  it("spends its row budget on groups, not on records", () => {
    const doc = buildGroupDoc(["span.name"], range, [], "spans");
    const limits = (doc.pipeline ?? []).filter((s) => "limit" in s);
    expect(limits).toEqual([{ limit: GROUP_BUDGET + 1 }]);
  });
});

describe("groupsFromIrResponse", () => {
  /** A `table` envelope shaped as the server returns it: physical names. */
  const table = (rows: (string | number | null)[][]) => ({
    result: "table",
    window: { start_ns: 1, end_ns: 2 },
    columns: [
      { name: "span_name", type: "string" },
      { name: "n", type: "int64" },
      { name: "errors", type: "int64" },
      { name: "p50", type: "float64" },
      { name: "p95", type: "float64" },
      { name: "last", type: "int64" },
    ],
    rows,
  });

  it("reads values by position, not by the logical name that was sent", () => {
    const res = groupsFromIrResponse(
      table([["GET /a", 48, 3, 12_000_000, 91_000_000, 1700]]),
      1,
    );
    expect(res.groups).toEqual([
      {
        values: ["GET /a"],
        count: 48,
        errors: 3,
        p50Ms: 12,
        p95Ms: 91,
        lastNs: "1700",
      },
    ]);
  });

  it("keeps a group with no errors rather than dropping it", () => {
    const res = groupsFromIrResponse(
      table([["GET /a", 48, 0, 1e6, 2e6, 1700]]),
      1,
    );
    expect(res.groups[0]!.errors).toBe(0);
  });

  it("carries a null dimension value through as an absent value", () => {
    const res = groupsFromIrResponse(table([[null, 7, 0, 1e6, 2e6, 1700]]), 1);
    expect(res.groups[0]!.values).toEqual([null]);
  });

  it("flags truncation when the server returned more than the budget", () => {
    const rows = Array.from({ length: GROUP_BUDGET + 1 }, (_, i) => [
      `g${i}`,
      1,
      0,
      1e6,
      2e6,
      1700,
    ]);
    const res = groupsFromIrResponse(table(rows), 1);
    expect(res.truncated).toBe(true);
    expect(res.groups).toHaveLength(GROUP_BUDGET);
  });

  it("does not flag truncation when the group set fits", () => {
    const res = groupsFromIrResponse(
      table([["GET /a", 1, 0, 1e6, 2e6, 1700]]),
      1,
    );
    expect(res.truncated).toBe(false);
  });

  // The deployment may spell an error `Error` or `STATUS_CODE_ERROR`; the
  // scope must catch both, exactly as `normalizeStatus()` does.
  it("matches every spelling of an error status", () => {
    const re = new RegExp(ERROR_PATTERN.replace("(?i)", ""), "i");
    expect(re.test("Error")).toBe(true);
    expect(re.test("STATUS_CODE_ERROR")).toBe(true);
    expect(re.test("Ok")).toBe(false);
    expect(re.test("Unspecified")).toBe(false);
  });

  it("reads two grouping dimensions", () => {
    const res = groupsFromIrResponse(
      {
        result: "table",
        window: { start_ns: 1, end_ns: 2 },
        columns: [
          { name: "span_name", type: "string" },
          { name: "service_name", type: "string" },
          { name: "n", type: "int64" },
          { name: "errors", type: "int64" },
          { name: "p50", type: "float64" },
          { name: "p95", type: "float64" },
          { name: "last", type: "int64" },
        ],
        rows: [["GET /a", "checkout", 5, 1, 1e6, 2e6, 1700]],
      },
      2,
    );
    expect(res.groups[0]!.values).toEqual(["GET /a", "checkout"]);
    expect(res.groups[0]!.count).toBe(5);
  });
});
