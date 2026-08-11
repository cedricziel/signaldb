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

  it("scopes a span attribute, not a resource attribute", () => {
    // db.namespace is set on the span by db-client instrumentation, not on
    // the resource — a `resource.` selector would silently match nothing.
    expect(compileTraceQL([{ field: "db.namespace", value: "orders" }])).toBe(
      '{ span.db.namespace = "orders" }',
    );
  });

  it("leaves intrinsics unscoped", () => {
    expect(compileTraceQL([{ field: "name", value: "GET /pay" }])).toBe(
      '{ name = "GET /pay" }',
    );
    expect(compileTraceQL([{ field: "status", value: "error" }])).toBe(
      "{ status = error }",
    );
    expect(compileTraceQL([{ field: "kind", value: "server" }])).toBe(
      "{ kind = server }",
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
  it("offers the curated fields with a defined UI treatment", () => {
    expect(FACET_FIELDS.map((f) => f.field)).toEqual([
      "service.name",
      "name",
      "status",
      "kind",
      "db.namespace",
    ]);
  });

  it("names the IR field each facet aggregates by", () => {
    expect(FACET_FIELDS.map((f) => f.irField)).toEqual([
      "service.name",
      "span.name",
      "status.code",
      "span_kind",
      "db.namespace",
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
