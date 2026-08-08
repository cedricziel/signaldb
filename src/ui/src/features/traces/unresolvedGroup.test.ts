import { describe, expect, it } from "vitest";
import { ROOT_SPAN_SENTINEL, type TraceGroup } from "../../api/traceGroups";
import { buildWindowTotalDoc, looksUnresolved } from "./unresolvedGroup";

function group(values: (string | null)[], count: number): TraceGroup {
  return { values, count, errors: 0, p50Ms: 1, p95Ms: 1, lastNs: "1" };
}

describe("looksUnresolved", () => {
  it("is true for a single group with every value null", () => {
    expect(looksUnresolved([group([null], 500)])).toBe(true);
  });

  it("is true for a composite dimension all-null single group", () => {
    expect(looksUnresolved([group([null, null], 500)])).toBe(true);
  });

  it("is false when the single group has a real value", () => {
    expect(looksUnresolved([group(["GET /health"], 500)])).toBe(false);
  });

  it("is false when a composite group has one real and one null value", () => {
    // A partially-resolved composite grouping is not the unresolved shape —
    // only "every value null" counts.
    expect(looksUnresolved([group(["GET /health", null], 500)])).toBe(false);
  });

  it("is false when a null group sits alongside real groups", () => {
    expect(
      looksUnresolved([group([null], 3), group(["GET /health"], 40)]),
    ).toBe(false);
  });

  it("is false for an empty result", () => {
    expect(looksUnresolved([])).toBe(false);
  });
});

describe("buildWindowTotalDoc", () => {
  const range = { fromMs: 1_000_000, toMs: 4_600_000 };

  it("counts the whole window, ungrouped, with no order or limit", () => {
    const doc = buildWindowTotalDoc(range, [], "spans");
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "table",
      pipeline: [{ aggregate: { by: [], aggs: [{ fn: "count", as: "n" }] } }],
    });
  });

  it("scopes to root spans at trace grain, same as the group query", () => {
    const doc = buildWindowTotalDoc(range, [], "traces");
    expect(doc.pipeline?.[0]).toEqual({
      where: {
        field: "parent_span_id",
        op: "eq",
        value: ROOT_SPAN_SENTINEL,
      },
    });
  });

  it("omits the root predicate at span grain", () => {
    const doc = buildWindowTotalDoc(range, [], "spans");
    expect(JSON.stringify(doc.pipeline)).not.toContain("parent_span_id");
  });

  it("applies the same active filters as the group query", () => {
    const doc = buildWindowTotalDoc(
      range,
      [{ field: "service.name", value: "checkout" }],
      "spans",
    );
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
  });

  it("never orders or limits — every group must be counted", () => {
    const doc = buildWindowTotalDoc(range, [], "spans");
    const stages = JSON.stringify(doc.pipeline);
    expect(stages).not.toContain('"order"');
    expect(stages).not.toContain('"limit"');
  });
});
