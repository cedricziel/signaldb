import { describe, expect, it } from "vitest";
import {
  diffRegistries,
  filterByName,
  humanizeGroupId,
  indexRegistry,
} from "./registryIndex";
import { ACME_DOCUMENT, OTEL_DOCUMENT } from "./testFixtures";

describe("indexRegistry", () => {
  it("lists attribute definitions, entities and metrics from the groups", () => {
    const index = indexRegistry(OTEL_DOCUMENT);
    expect(index.attributes.map((a) => a.key)).toEqual([
      "k8s.namespace.name",
      "k8s.pod.label",
      "k8s.pod.name",
      "k8s.pod.uid",
      "service.name",
    ]);
    expect(index.attributes[0]?.groupTitle).toBe("Kubernetes Attributes");
    expect(index.entities.map((e) => e.name)).toEqual(["k8s.pod", "service"]);
    expect(index.metrics.map((m) => m.name)).toEqual(["k8s.pod.cpu.time"]);
  });

  it("does not count entity refs as attribute definitions", () => {
    const index = indexRegistry(ACME_DOCUMENT);
    expect(index.attributes.map((a) => a.key)).toEqual([
      "acme.order.id",
      "acme.order.total",
      "service.name",
    ]);
  });

  it("tolerates a document without groups", () => {
    const index = indexRegistry({ name: "x" });
    expect(index.attributes).toEqual([]);
    expect(index.fingerprints.size).toBe(0);
  });
});

describe("humanizeGroupId", () => {
  it("drops the registry prefix and title-cases the segments", () => {
    expect(humanizeGroupId("registry.k8s.pod")).toBe("K8s Pod");
    expect(humanizeGroupId("entity.service")).toBe("Entity Service");
  });
});

describe("filterByName", () => {
  it("matches case-insensitively and lists prefix matches first", () => {
    const items = ["k8s.pod.name", "service.name", "K8S.namespace.name"];
    expect(filterByName(items, (s) => s, "name")).toEqual([
      "k8s.pod.name",
      "service.name",
      "K8S.namespace.name",
    ]);
    expect(filterByName(items, (s) => s, "K8S")).toEqual([
      "k8s.pod.name",
      "K8S.namespace.name",
    ]);
    expect(filterByName(items, (s) => s, "  ")).toBe(items);
  });
});

describe("diffRegistries", () => {
  it("reports added, changed and removed definitions by kind/name", () => {
    const before = indexRegistry(ACME_DOCUMENT);
    const after = indexRegistry({
      ...ACME_DOCUMENT,
      groups: [
        {
          id: "registry.acme.order",
          type: "attribute_group",
          attributes: [
            { id: "acme.order.id", type: "string", brief: "Changed." },
            { id: "acme.order.sku", type: "string" },
          ],
        },
        ACME_DOCUMENT.groups[1],
      ],
    });
    expect(diffRegistries(before, after)).toEqual({
      added: ["attributes/acme.order.sku"],
      changed: ["attributes/acme.order.id"],
      removed: [
        "attributes/acme.order.total",
        "attributes/service.name",
        "metrics/acme.orders.placed",
      ],
    });
  });
});
