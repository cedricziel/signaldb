import { describe, expect, it } from "vitest";
import {
  DEFAULT_KIND_FILTERS,
  FACET_FIELDS,
  KIND_VALUES,
  compileTraceQL,
  facetField,
  filterStages,
  removeTraceFilter,
  traceFilterFromParam,
  traceFilterToParam,
  traceFiltersForUrl,
  upsertTraceFilter,
  withDefaultTraceFilters,
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
describe("multi-value facets (kind)", () => {
  it("lists every span kind as a fixed value set, Server/Client/Producer/Consumer by default", () => {
    expect(KIND_VALUES).toEqual([
      "Server",
      "Client",
      "Internal",
      "Producer",
      "Consumer",
    ]);
    expect(DEFAULT_KIND_FILTERS).toEqual(
      ["Server", "Client", "Producer", "Consumer"].map((value) => ({
        field: "kind",
        value,
      })),
    );
    expect(facetField("kind")?.multi).toBe(true);
    expect(facetField("service.name")?.multi).toBeUndefined();
  });

  it("upsert adds a second value for a multi facet instead of replacing", () => {
    const one = upsertTraceFilter([], { field: "kind", value: "Server" });
    const two = upsertTraceFilter(one, { field: "kind", value: "Client" });
    expect(two).toEqual([
      { field: "kind", value: "Server" },
      { field: "kind", value: "Client" },
    ]);
    // Idempotent for a value already present.
    expect(upsertTraceFilter(two, { field: "kind", value: "Client" })).toEqual(
      two,
    );
  });

  it("removing the last kind selects all kinds rather than none", () => {
    const only = [{ field: "kind", value: "Server" }];
    expect(removeTraceFilter(only, { field: "kind", value: "Server" })).toEqual(
      KIND_VALUES.map((value) => ({ field: "kind", value })),
    );
    // Removing one of several just drops it; other fields are untouched.
    expect(
      removeTraceFilter(
        [
          { field: "service.name", value: "api" },
          { field: "kind", value: "Server" },
          { field: "kind", value: "Client" },
        ],
        { field: "kind", value: "Server" },
      ),
    ).toEqual([
      { field: "service.name", value: "api" },
      { field: "kind", value: "Client" },
    ]);
  });

  it("applies the default kinds when the state has no kind filter, and strips them from the URL", () => {
    expect(withDefaultTraceFilters([])).toEqual(DEFAULT_KIND_FILTERS);
    const svc = { field: "service.name", value: "api" };
    expect(withDefaultTraceFilters([svc])).toEqual([
      svc,
      ...DEFAULT_KIND_FILTERS,
    ]);
    const custom = [svc, { field: "kind", value: "Internal" }];
    expect(withDefaultTraceFilters(custom)).toEqual(custom);

    expect(traceFiltersForUrl([svc, ...DEFAULT_KIND_FILTERS])).toEqual([svc]);
    // Order-insensitive equality with the default set.
    expect(traceFiltersForUrl([...DEFAULT_KIND_FILTERS].reverse())).toEqual([]);
    expect(traceFiltersForUrl(custom)).toEqual(custom);
  });

  it("compiles several values of one field into a single `in` predicate", () => {
    const stages = filterStages([
      { field: "service.name", value: "api" },
      { field: "kind", value: "Server" },
      { field: "kind", value: "Client" },
    ]);
    expect(stages).toEqual([
      { where: { field: "service.name", op: "eq", value: "api" } },
      { where: { field: "span_kind", op: "in", value: ["Server", "Client"] } },
    ]);
    // A facet's own field is left out when counting its values.
    expect(
      filterStages(
        [
          { field: "service.name", value: "api" },
          { field: "kind", value: "Server" },
        ],
        "span_kind",
      ),
    ).toEqual([{ where: { field: "service.name", op: "eq", value: "api" } }]);
  });

  it("compiles several values of one field as an OR group in TraceQL", () => {
    expect(
      compileTraceQL([
        { field: "service.name", value: "api" },
        { field: "kind", value: "server" },
        { field: "kind", value: "client" },
      ]),
    ).toBe(
      '{ resource.service.name = "api" && (kind = server || kind = client) }',
    );
  });
});
