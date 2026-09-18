import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../api/gen";
import { attributePivots } from "./attrPivots";
import { compositeKey } from "./traceGroups";
import { semanticsFromResolution, type AttributeSemantics } from "./semantics";

const hit = (over: Partial<AttributeHit> = {}): AttributeHit => ({
  key: "service.name",
  brief: "",
  type: "string",
  group_id: "registry.service",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  ...over,
});

const semOf = (h: AttributeHit): AttributeSemantics =>
  semanticsFromResolution({ key: h.key, hits: [h], primary: h })!;

describe("attributePivots", () => {
  it("returns nothing for an unresolved key", () => {
    expect(
      attributePivots("service.name", "checkout", undefined, new Map(), "traces"),
    ).toEqual([]);
  });

  it("returns nothing for a key with only a descriptive role", () => {
    const sem = semOf(
      hit({
        entity_roles: [{ namespace: "otel", entity: "service", role: "descriptive" }],
      }),
    );
    expect(
      attributePivots(
        "service.name",
        "checkout",
        sem,
        new Map([["service.name", "checkout"]]),
        "traces",
      ),
    ).toEqual([]);
  });

  it("offers a logs pivot from the span panel for an identifying key", () => {
    const sem = semOf(
      hit({
        entity_roles: [{ namespace: "otel", entity: "service", role: "identifying" }],
      }),
    );
    const bag = new Map([["service.name", "checkout"]]);
    const pivots = attributePivots("service.name", "checkout", sem, bag, "traces");
    const logs = pivots.find((p) => p.kind === "logs");
    expect(logs).toEqual({
      kind: "logs",
      label: "logs ↗",
      ariaLabel: "Logs with service.name = checkout",
      patch: {
        signal: "logs",
        trace: "",
        raw: "",
        search: "",
        group: "",
        traceFilters: [],
        filters: [{ label: "service_name", op: "=", value: "checkout" }],
      },
    });
  });

  it("offers no logs pivot from the log detail (the reverse direction)", () => {
    const sem = semOf(
      hit({
        entity_roles: [{ namespace: "otel", entity: "service", role: "identifying" }],
      }),
    );
    const bag = new Map([["service.name", "checkout"]]);
    const pivots = attributePivots("service.name", "checkout", sem, bag, "logs");
    expect(pivots.find((p) => p.kind === "logs")).toBeUndefined();
  });

  it("offers a catalog pivot once only the first identity key is in the bag, with a null secondary", () => {
    // service's identity is ["service.name", "service.namespace"].
    const sem = semOf(
      hit({
        entity_roles: [{ namespace: "otel", entity: "service", role: "identifying" }],
      }),
    );
    const withoutNamespace = new Map([["service.name", "checkout"]]);
    const catalogWithoutNamespace = attributePivots(
      "service.name",
      "checkout",
      sem,
      withoutNamespace,
      "traces",
    ).find((p) => p.kind === "catalog");
    expect(catalogWithoutNamespace).toEqual({
      kind: "catalog",
      label: "catalog ↗",
      ariaLabel: "Open service checkout in the catalog",
      patch: {
        signal: "catalog",
        trace: "",
        group: "",
        search: "",
        traceFilters: [],
        filters: [],
        raw: "",
        catalogEntity: "service",
        // Missing secondary identity value becomes the catalog's "(not
        // set)" marker rather than blocking the pivot outright.
        catalogPrimary: compositeKey(["checkout", null]),
        catalogSecondary: "",
      },
    });

    const withNamespace = new Map([
      ["service.name", "checkout"],
      ["service.namespace", "shop"],
    ]);
    const catalog = attributePivots(
      "service.name",
      "checkout",
      sem,
      withNamespace,
      "traces",
    ).find((p) => p.kind === "catalog");
    expect(catalog?.patch.catalogPrimary).toBe(compositeKey(["checkout", "shop"]));
  });

  it("offers no traces pivot from the log detail for a key that isn't a traces facet", () => {
    const sem = semOf(
      hit({
        key: "app.order.id",
        entity_roles: [{ namespace: "custom", entity: "order", role: "identifying" }],
      }),
    );
    const bag = new Map([["app.order.id", "o-1"]]);
    // "order" is also not a curated catalog entity type, so this is empty.
    expect(attributePivots("app.order.id", "o-1", sem, bag, "logs")).toEqual([]);
  });

  it("offers traces and catalog pivots from the log detail for a facetable identifying key", () => {
    const sem = semOf(
      hit({
        key: "host.name",
        entity_roles: [{ namespace: "otel", entity: "host", role: "identifying" }],
      }),
    );
    const bag = new Map([["host.name", "node-1"]]);
    const pivots = attributePivots("host.name", "node-1", sem, bag, "logs");

    expect(pivots.find((p) => p.kind === "traces")).toEqual({
      kind: "traces",
      label: "traces ↗",
      ariaLabel: "Traces with host.name = node-1",
      patch: {
        signal: "traces",
        trace: "",
        search: "",
        group: "",
        // Logs-only params must not ride into /traces.
        filters: [],
        raw: "",
        traceFilters: [{ field: "host.name", value: "node-1" }],
      },
    });
    expect(pivots.find((p) => p.kind === "catalog")).toEqual({
      kind: "catalog",
      label: "catalog ↗",
      ariaLabel: "Open host node-1 in the catalog",
      patch: {
        signal: "catalog",
        trace: "",
        group: "",
        search: "",
        traceFilters: [],
        filters: [],
        raw: "",
        catalogEntity: "host",
        catalogPrimary: "node-1",
        catalogSecondary: "",
      },
    });
  });

  it("offers a catalog pivot for k8s.pod.name with just the pod name, null secondary until k8s.namespace.name joins the bag", () => {
    const sem = semOf(
      hit({
        key: "k8s.pod.name",
        entity_roles: [{ namespace: "otel", entity: "k8s.pod", role: "identifying" }],
      }),
    );
    const withoutNamespace = new Map([["k8s.pod.name", "web-1"]]);
    const catalogWithoutNamespace = attributePivots(
      "k8s.pod.name",
      "web-1",
      sem,
      withoutNamespace,
      "logs",
    ).find((p) => p.kind === "catalog");
    expect(catalogWithoutNamespace?.patch.catalogPrimary).toBe(
      compositeKey(["web-1", null]),
    );

    const withNamespace = new Map([
      ["k8s.pod.name", "web-1"],
      ["k8s.namespace.name", "prod"],
    ]);
    const catalog = attributePivots(
      "k8s.pod.name",
      "web-1",
      sem,
      withNamespace,
      "logs",
    ).find((p) => p.kind === "catalog");
    expect(catalog?.patch).toEqual({
      signal: "catalog",
      trace: "",
      group: "",
      search: "",
      traceFilters: [],
      filters: [],
      raw: "",
      catalogEntity: "k8s_pod",
      catalogPrimary: compositeKey(["web-1", "prod"]),
      catalogSecondary: "",
    });
  });

  it("disambiguates the label when a key identifies more than one catalog-backed entity", () => {
    const sem = semOf(
      hit({
        key: "host.name",
        entity_roles: [
          { namespace: "otel", entity: "host", role: "identifying" },
          { namespace: "otel", entity: "k8s.node", role: "identifying" },
        ],
      }),
    );
    const bag = new Map([
      ["host.name", "node-1"],
      ["k8s.node.name", "node-1"],
    ]);
    const catalogPivots = attributePivots(
      "host.name",
      "node-1",
      sem,
      bag,
      "logs",
    ).filter((p) => p.kind === "catalog");
    expect(catalogPivots.map((p) => p.label)).toEqual([
      "catalog: host ↗",
      "catalog: node ↗",
    ]);
    // The aria-label — what the row action's accessible name actually is —
    // is unaffected: it already names the entity.
    expect(catalogPivots.map((p) => p.ariaLabel)).toEqual([
      "Open host node-1 in the catalog",
      "Open node node-1 in the catalog",
    ]);
  });
});
