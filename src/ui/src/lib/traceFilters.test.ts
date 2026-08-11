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
    // db.namespace and messaging.destination.name are set on the span by
    // the calling instrumentation, not on the resource — a `resource.`
    // selector would silently match nothing.
    expect(compileTraceQL([{ field: "db.namespace", value: "orders" }])).toBe(
      '{ span.db.namespace = "orders" }',
    );
    expect(
      compileTraceQL([
        { field: "messaging.destination.name", value: "orders.v2" },
      ]),
    ).toBe('{ span.messaging.destination.name = "orders.v2" }');
  });

  it("scopes host/k8s/container/process identity as resource attributes", () => {
    // Unlike db.namespace/messaging.*, these describe the process emitting
    // telemetry, not an individual operation — OTel models them as resource
    // attributes.
    for (const [field, selector] of [
      ["host.name", "resource.host.name"],
      ["k8s.pod.name", "resource.k8s.pod.name"],
      ["k8s.namespace.name", "resource.k8s.namespace.name"],
      ["k8s.node.name", "resource.k8s.node.name"],
      ["container.name", "resource.container.name"],
      ["process.pid", "resource.process.pid"],
    ] as const) {
      expect(compileTraceQL([{ field, value: "x" }])).toBe(
        `{ ${selector} = "x" }`,
      );
    }
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
      "messaging.destination.name",
      "host.name",
      "k8s.pod.name",
      "k8s.namespace.name",
      "k8s.node.name",
      "container.name",
      "process.pid",
    ]);
  });

  it("aggregates non-intrinsic facets by their own field name", () => {
    for (const f of FACET_FIELDS) {
      if (["name", "status", "kind"].includes(f.field)) continue;
      expect(f.irField).toBe(f.field);
    }
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
