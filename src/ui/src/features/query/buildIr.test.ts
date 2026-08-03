import { describe, expect, it } from "vitest";

import { buildIrDocument } from "./buildIr";

describe("buildIrDocument", () => {
  // Task 9.1 — the builder appends stage objects and emits a valid IR document.
  it("appends where then aggregate stages", () => {
    const doc = buildIrDocument({
      source: "logs",
      result: "series",
      range: { from: "now-1h", to: "now" },
      filters: [{ field: "service.name", op: "eq", value: "api" }],
      aggregate: {
        by: ["service_name"],
        aggs: [{ fn: "count", as: "n" }],
        step: "1m",
      },
    });

    expect(doc.irVersion).toBe(1);
    expect(doc.from).toBe("logs");
    expect(doc.result).toBe("series");
    expect(doc.pipeline).toHaveLength(2);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "api" },
    });
    expect(doc.pipeline?.[1]).toEqual({
      aggregate: {
        by: ["service_name"],
        aggs: [{ fn: "count", as: "n" }],
        step: "1m",
      },
    });
  });

  it("wraps multiple filters in `and` and negates with `not`", () => {
    const doc = buildIrDocument({
      source: "logs",
      result: "rows",
      range: { from: "now-1h", to: "now" },
      filters: [
        { field: "a", op: "eq", value: 1 },
        { field: "b", op: "regex", value: "x", negate: true },
      ],
    });
    expect(doc.pipeline?.[0]).toEqual({
      where: {
        and: [
          { field: "a", op: "eq", value: 1 },
          { not: { field: "b", op: "regex", value: "x" } },
        ],
      },
    });
  });

  it("emits structured objects, not a dialect string", () => {
    const doc = buildIrDocument({
      source: "traces",
      result: "rows",
      range: { from: "now-1h", to: "now" },
      filters: [],
      fields: ["span_name"],
    });
    expect(doc.pipeline).toEqual([]);
    expect(doc.fields).toEqual(["span_name"]);
    // The emitted document round-trips through JSON with no embedded query text.
    expect(JSON.stringify(doc)).not.toMatch(/\{[a-z_]+=/); // no `{label="..."}` selector
  });

  it("omits the value for an `exists` predicate", () => {
    const doc = buildIrDocument({
      source: "logs",
      result: "rows",
      range: { from: "now-1h", to: "now" },
      filters: [{ field: "trace_id", op: "exists" }],
    });
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "trace_id", op: "exists" },
    });
  });
});
