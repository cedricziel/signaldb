import { describe, expect, it } from "vitest";
import type { AttributeHit, AttributeResolution } from "../api/gen";
import {
  groupBySemanticTitle,
  humanizeNamespace,
  semanticsFromResolution,
  semanticTitle,
} from "./semantics";

const hit = (over: Partial<AttributeHit> = {}): AttributeHit => ({
  key: "k8s.pod.uid",
  brief: "The UID of the Pod.",
  type: "string",
  group_id: "registry.k8s.pod",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  ...over,
});

const semOf = (h: AttributeHit) =>
  semanticsFromResolution({ key: h.key, hits: [h], primary: h })!;

describe("humanizeNamespace", () => {
  it("drops the leaf and title-cases the prefix", () => {
    expect(humanizeNamespace("k8s.pod.uid")).toBe("K8s Pod");
    expect(humanizeNamespace("http.request.method")).toBe("Http Request");
  });

  it("uses the whole key when there is no prefix", () => {
    expect(humanizeNamespace("name")).toBe("Name");
  });

  it("splits underscores as words too", () => {
    expect(humanizeNamespace("service_name")).toBe("Service Name");
  });
});

describe("semanticTitle", () => {
  it("prefers the group display name", () => {
    expect(
      semanticTitle(hit({ group_display_name: "Kubernetes Attributes" })),
    ).toBe("Kubernetes Attributes");
  });

  it("falls back to the humanized namespace prefix", () => {
    expect(semanticTitle(hit({ group_display_name: null }))).toBe("K8s Pod");
  });
});

describe("semanticsFromResolution", () => {
  it("returns undefined for an unresolved key", () => {
    const res: AttributeResolution = { key: "app.x", hits: [], primary: null };
    expect(semanticsFromResolution(res)).toBeUndefined();
  });

  it("splits primary and alternatives, in precedence order", () => {
    const custom = hit({ namespace: "acme", source: "custom", brief: "ours" });
    const otel = hit({ brief: "theirs" });
    const sem = semanticsFromResolution({
      key: "k8s.pod.uid",
      hits: [custom, otel],
      primary: custom,
    });
    expect(sem?.primary.namespace).toBe("acme");
    expect(sem?.alternatives.map((h) => h.namespace)).toEqual(["otel"]);
  });

  it("flags deprecation from any hit, preferring the primary's rename", () => {
    const primary = hit({ brief: "ours", namespace: "acme" });
    const dep = hit({
      deprecated: { renamed_to: "k8s.pod.id", reason: "renamed" },
    });
    const sem = semanticsFromResolution({
      key: "k8s.pod.uid",
      hits: [primary, dep],
      primary,
    });
    expect(sem?.deprecated?.renamed_to).toBe("k8s.pod.id");
  });
});

describe("groupBySemanticTitle", () => {
  const entries: [string, string][] = [
    ["app.order.id", "1"],
    ["k8s.pod.uid", "p"],
    ["k8s.pod.name", "n"],
    ["service.name", "s"],
  ];

  it("returns one untitled group when nothing resolved", () => {
    expect(groupBySemanticTitle(entries, new Map())).toEqual([
      { title: null, entries },
    ]);
  });

  it("groups known keys under their title and the rest under Other, last", () => {
    const sem = new Map([
      ["k8s.pod.uid", semOf(hit({ group_display_name: "Kubernetes" }))],
      [
        "k8s.pod.name",
        semOf(hit({ key: "k8s.pod.name", group_display_name: "Kubernetes" })),
      ],
      [
        "service.name",
        semOf(hit({ key: "service.name", group_display_name: "Service" })),
      ],
    ]);
    expect(groupBySemanticTitle(entries, sem)).toEqual([
      {
        title: "Kubernetes",
        entries: [
          ["k8s.pod.uid", "p"],
          ["k8s.pod.name", "n"],
        ],
      },
      { title: "Service", entries: [["service.name", "s"]] },
      { title: "Other", entries: [["app.order.id", "1"]] },
    ]);
  });
});
