import { describe, expect, it } from "vitest";
import {
  FACET_VALUE_LIMIT,
  buildFacetDoc,
  facetFromIrResponse,
} from "./traceFacets";

describe("buildFacetDoc", () => {
  const range = { fromMs: 1_000_000, toMs: 4_600_000 };

  it("asks for counts per value, most frequent first", () => {
    expect(buildFacetDoc("service.name", range, [])).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "table",
      pipeline: [
        {
          aggregate: {
            by: ["service.name"],
            aggs: [{ fn: "count", as: "n" }],
          },
        },
        { order: [{ of: "n", dir: "desc" }] },
        // One more than displayed, so truncation is detectable.
        { limit: FACET_VALUE_LIMIT + 1 },
      ],
    });
  });

  // Counts must describe the window, never the row-limited trace list.
  it("carries no row limit from the trace list", () => {
    const doc = buildFacetDoc("status.code", range, []);
    expect(JSON.stringify(doc.pipeline)).not.toContain("500");
  });

  it("applies the active filters so counts match what is shown", () => {
    const doc = buildFacetDoc("status.code", range, [
      { field: "service.name", value: "checkout" },
    ]);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
  });

  it("does not let a facet filter itself", () => {
    const doc = buildFacetDoc("service.name", range, [
      { field: "service.name", value: "checkout" },
      { field: "status", value: "error" },
    ]);
    // Only the other facet's filter narrows this one, so its own alternatives
    // stay visible and switchable.
    const stages = JSON.stringify(doc.pipeline);
    expect(stages).toContain("status.code");
    expect(stages).not.toContain("checkout");
  });
});

describe("facetFromIrResponse", () => {
  const table = (rows: [string | null, number][]) => ({
    result: "table",
    window: { start_ns: 0, end_ns: 1 },
    columns: [
      { name: "service_name", type: "string" },
      { name: "n", type: "int64" },
    ],
    rows: rows.map(([v, n]) => [v, n] as unknown[]),
  });

  it("reads value/count pairs", () => {
    expect(
      facetFromIrResponse(
        table([
          ["signaldb", 2903],
          ["signaldb-ui", 980],
        ]),
      ),
    ).toEqual({
      values: [
        { value: "signaldb", count: 2903 },
        { value: "signaldb-ui", count: 980 },
      ],
      truncated: false,
      resolved: true,
    });
  });

  it("reports truncation and trims to the display limit", () => {
    const rows = Array.from(
      { length: FACET_VALUE_LIMIT + 1 },
      (_, i): [string, number] => [`v${i}`, 100 - i],
    );
    const facet = facetFromIrResponse(table(rows));
    expect(facet.values).toHaveLength(FACET_VALUE_LIMIT);
    expect(facet.truncated).toBe(true);
  });

  it("renders a null group as an explicit absent-value label", () => {
    const facet = facetFromIrResponse(table([[null, 12]]));
    expect(facet.values).toEqual([{ value: null, count: 12 }]);
    expect(facet.resolved).toBe(true);
  });

  // #1070: an unresolvable field returns HTTP 200 with a single group labelled
  // with the string "null" — indistinguishable from a real result unless we
  // check for it, and it would show the window total under a bogus facet.
  it('marks a lone "null"-string group as unresolved', () => {
    const facet = facetFromIrResponse(table([["null", 21_503]]));
    expect(facet.resolved).toBe(false);
    expect(facet.values).toEqual([]);
  });

  it("does not mistake a real null-string value among others", () => {
    const facet = facetFromIrResponse(
      table([
        ["null", 5],
        ["signaldb", 900],
      ]),
    );
    expect(facet.resolved).toBe(true);
    expect(facet.values).toHaveLength(2);
  });

  it("tolerates an empty table", () => {
    expect(
      facetFromIrResponse({
        result: "table",
        window: { start_ns: 0, end_ns: 1 },
      }),
    ).toEqual({ values: [], truncated: false, resolved: true });
  });
});
