import { describe, expect, it } from "vitest";
import {
  FACET_FIELDS,
  compileTraceQL,
  traceFilterFromParam,
  traceFilterToParam,
  upsertTraceFilter,
  type TraceFilter,
} from "./traceFilters";

describe("compileTraceQL", () => {
  it("compiles nothing for an empty filter set", () => {
    expect(compileTraceQL([])).toBe("");
  });

  it("scopes a resource attribute", () => {
    expect(compileTraceQL([{ field: "service.name", value: "checkout" }])).toBe(
      '{ resource.service.name = "checkout" }',
    );
  });

  it("leaves intrinsics unscoped", () => {
    expect(compileTraceQL([{ field: "name", value: "GET /pay" }])).toBe(
      '{ name = "GET /pay" }',
    );
    expect(compileTraceQL([{ field: "status", value: "error" }])).toBe(
      "{ status = error }",
    );
  });

  it("combines several filters with &&", () => {
    expect(
      compileTraceQL([
        { field: "service.name", value: "checkout" },
        { field: "status", value: "error" },
      ]),
    ).toBe('{ resource.service.name = "checkout" && status = error }');
  });

  it("escapes quotes and backslashes in values", () => {
    expect(compileTraceQL([{ field: "name", value: 'say "hi"\\now' }])).toBe(
      '{ name = "say \\"hi\\"\\\\now" }',
    );
  });

  it("drops filters on fields that are not facetable", () => {
    expect(
      compileTraceQL([
        { field: "nonsense", value: "x" },
        { field: "service.name", value: "api" },
      ]),
    ).toBe('{ resource.service.name = "api" }');
  });
});

describe("FACET_FIELDS", () => {
  // Only the fields /api/search/tags can actually enumerate — see #1073.
  it("offers exactly the enumerable fields", () => {
    expect(FACET_FIELDS.map((f) => f.field)).toEqual([
      "service.name",
      "name",
      "status",
    ]);
  });

  it("names the IR field each facet aggregates by", () => {
    expect(FACET_FIELDS.map((f) => f.irField)).toEqual([
      "service.name",
      "span.name",
      "status.code",
    ]);
  });
});

describe("trace filter URL params", () => {
  it("round-trips a filter", () => {
    const f: TraceFilter = { field: "service.name", value: "checkout" };
    const parsed = traceFilterFromParam(traceFilterToParam(f));
    expect(parsed).toEqual(f);
  });

  it("round-trips a value containing the separator", () => {
    const f: TraceFilter = { field: "name", value: "GET /a|b" };
    expect(traceFilterFromParam(traceFilterToParam(f))).toEqual(f);
  });

  it("rejects a malformed or unknown-field param", () => {
    expect(traceFilterFromParam("garbage")).toBeNull();
    expect(traceFilterFromParam("nonsense|x")).toBeNull();
  });
});

describe("upsertTraceFilter", () => {
  it("adds a new field", () => {
    expect(
      upsertTraceFilter([], { field: "service.name", value: "api" }),
    ).toEqual([{ field: "service.name", value: "api" }]);
  });

  it("replaces the value for a field already filtered", () => {
    expect(
      upsertTraceFilter([{ field: "service.name", value: "api" }], {
        field: "service.name",
        value: "web",
      }),
    ).toEqual([{ field: "service.name", value: "web" }]);
  });

  it("keeps filters on other fields", () => {
    expect(
      upsertTraceFilter([{ field: "status", value: "error" }], {
        field: "service.name",
        value: "api",
      }),
    ).toEqual([
      { field: "status", value: "error" },
      { field: "service.name", value: "api" },
    ]);
  });
});
