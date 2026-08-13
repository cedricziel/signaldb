import { describe, expect, it } from "vitest";
import {
  applyErrorFilters,
  errorFacetValues,
  ERROR_FACET_FIELDS,
  errorFacetLabel,
  type ErrorFilter,
} from "./errorFacets";
import type { ErrorGroup } from "../api/errors";

function group(overrides: Partial<ErrorGroup> = {}): ErrorGroup {
  return {
    source: "traces",
    exceptionType: "std::io::Error",
    exceptionMessage: "boom",
    serviceName: "signaldb",
    count: 3,
    firstNs: "1000",
    lastNs: "2000",
    ...overrides,
  };
}

const groups: ErrorGroup[] = [
  group({
    exceptionType: "std::io::Error",
    serviceName: "signaldb",
    source: "traces",
    count: 3,
  }),
  group({
    exceptionType: "std::io::Error",
    serviceName: "signaldb-ui",
    source: "logs",
    count: 2,
  }),
  group({
    exceptionType: "ValueError",
    serviceName: "signaldb-ui",
    source: "logs",
    count: 9,
  }),
];

describe("ERROR_FACET_FIELDS / errorFacetLabel", () => {
  it("declares a stable order with a human label per field", () => {
    expect(ERROR_FACET_FIELDS).toEqual([
      "exceptionType",
      "serviceName",
      "source",
    ]);
    expect(errorFacetLabel("exceptionType")).toBe("Type");
    expect(errorFacetLabel("serviceName")).toBe("Service");
    expect(errorFacetLabel("source")).toBe("Source");
  });
});

describe("errorFacetValues", () => {
  it("sums occurrence counts per distinct value, ranked by count", () => {
    expect(errorFacetValues(groups, "exceptionType", [])).toEqual([
      { value: "ValueError", count: 9 },
      { value: "std::io::Error", count: 5 },
    ]);
  });

  it("is narrowed by filters on other facets but not by its own", () => {
    const filters: ErrorFilter[] = [{ field: "source", value: "logs" }];
    // exceptionType facet is narrowed by the source=logs filter: only the
    // two logs groups count now, not the traces one.
    expect(errorFacetValues(groups, "exceptionType", filters)).toEqual([
      { value: "ValueError", count: 9 },
      { value: "std::io::Error", count: 2 },
    ]);
    // source facet's own filter does not narrow itself — both values stay
    // visible so the user can switch between them.
    expect(errorFacetValues(groups, "source", filters)).toEqual([
      { value: "logs", count: 11 },
      { value: "traces", count: 3 },
    ]);
  });

  it("excludes groups with a null value for the field", () => {
    const withNull = [...groups, group({ serviceName: null })];
    const values = errorFacetValues(withNull, "serviceName", []);
    expect(values.reduce((sum, v) => sum + v.count, 0)).toBe(14);
  });
});

describe("applyErrorFilters", () => {
  it("keeps only groups matching every active filter", () => {
    const filters: ErrorFilter[] = [
      { field: "source", value: "logs" },
      { field: "exceptionType", value: "std::io::Error" },
    ];
    const result = applyErrorFilters(groups, filters);
    expect(result).toEqual([
      group({
        exceptionType: "std::io::Error",
        serviceName: "signaldb-ui",
        source: "logs",
        count: 2,
      }),
    ]);
  });

  it("returns every group when there are no filters", () => {
    expect(applyErrorFilters(groups, [])).toEqual(groups);
  });

  it("a group with a null value never matches a filter on that field", () => {
    const nullish = group({ serviceName: null });
    const result = applyErrorFilters(
      [nullish],
      [{ field: "serviceName", value: "signaldb" }],
    );
    expect(result).toEqual([]);
  });
});
