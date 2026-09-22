import { describe, expect, it } from "vitest";
import {
  DEFAULT_KIND_FILTERS,
  FACET_FIELDS,
  KIND_VALUES,
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

  it("round-trips an absent-value filter", () => {
    const f: TraceFilter = { field: "service.name", value: "", op: "absent" };
    expect(traceFilterToParam(f)).toBe("service.name|absent");
    expect(traceFilterFromParam("service.name|absent")).toEqual(f);
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
});

describe('absent-value filters (op: "absent")', () => {
  it('compiles to a `not exists` predicate, not `eq ""`', () => {
    expect(
      filterStages([{ field: "service.name", value: "", op: "absent" }]),
    ).toEqual([{ not: { field: "service.name", op: "exists" } }]);
  });

  it("leaves the rest of the group's stages untouched", () => {
    expect(
      filterStages([
        { field: "host.name", value: "", op: "absent" },
        { field: "kind", value: "Server" },
      ]),
    ).toEqual([
      { not: { field: "host.name", op: "exists" } },
      { where: { field: "span_kind", op: "eq", value: "Server" } },
    ]);
  });
});
