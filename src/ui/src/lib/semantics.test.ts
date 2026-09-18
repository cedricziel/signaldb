import { describe, expect, it } from "vitest";
import type { AttributeHit, AttributeResolution } from "../api/gen";
import {
  foldSingletonGroups,
  groupBySemanticTitle,
  humanizeNamespace,
  plainBrief,
  semanticsFromResolution,
  semanticTitle,
  type TitledGroup,
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

  it("excludes the primary from alternatives even when the server sent a distinct clone of it", () => {
    // Mirrors the server: `primary: hits.first().cloned()` clones the first
    // hit, so after JSON round-tripping `hits[0]` and `primary` are distinct
    // objects with equal fields, not the same reference.
    const primary = hit();
    const clonedIntoHits = { ...hit() };
    const sem = semanticsFromResolution({
      key: "k8s.pod.uid",
      hits: [clonedIntoHits],
      primary,
    });
    expect(sem?.alternatives).toEqual([]);
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

describe("plainBrief", () => {
  it("strips a markdown link down to its label", () => {
    expect(
      plainBrief(
        "[HTTP response status code](https://tools.ietf.org/html/rfc7231#section-6).",
      ),
    ).toBe("HTTP response status code.");
  });

  it("removes backticks, including nested runs", () => {
    expect(plainBrief("Deprecated, use `db.system.name` instead.")).toBe(
      "Deprecated, use db.system.name instead.",
    );
    expect(plainBrief("``nested`` backticks")).toBe("nested backticks");
  });

  it("collapses whitespace runs and trims", () => {
    expect(plainBrief("  a   b\n\tc  ")).toBe("a b c");
  });

  it("returns an empty string for empty or missing input", () => {
    expect(plainBrief("")).toBe("");
    expect(plainBrief(null)).toBe("");
    expect(plainBrief(undefined)).toBe("");
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

describe("foldSingletonGroups", () => {
  const g = <V>(title: string | null, entries: [string, V][]): TitledGroup<V> => ({
    title,
    entries,
  });

  it("keeps a group with two or more entries as-is", () => {
    const groups = [
      g("Kubernetes", [
        ["k8s.pod.uid", "p"],
        ["k8s.pod.name", "n"],
      ]),
      g("Other", [["app.order.id", "1"]]),
    ];
    expect(foldSingletonGroups(groups)).toEqual(groups);
  });

  it("folds a singleton titled group into a new trailing Other, sorted by key", () => {
    const groups = [
      g("Kubernetes", [
        ["k8s.pod.uid", "p"],
        ["k8s.pod.name", "n"],
      ]),
      g("Service", [["service.name", "s"]]),
    ];
    expect(foldSingletonGroups(groups)).toEqual([
      g("Kubernetes", [
        ["k8s.pod.uid", "p"],
        ["k8s.pod.name", "n"],
      ]),
      g("Other", [["service.name", "s"]]),
    ]);
  });

  it("merges a folded singleton into an existing Other, interleaved alphabetically", () => {
    const groups = [
      g("Kubernetes", [
        ["k8s.pod.uid", "p"],
        ["k8s.pod.name", "n"],
      ]),
      g("Service", [["service.name", "s"]]),
      g("Other", [["app.order.id", "1"], ["zzz.custom", "z"]]),
    ];
    expect(foldSingletonGroups(groups)).toEqual([
      g("Kubernetes", [
        ["k8s.pod.uid", "p"],
        ["k8s.pod.name", "n"],
      ]),
      g("Other", [
        ["app.order.id", "1"],
        ["service.name", "s"],
        ["zzz.custom", "z"],
      ]),
    ]);
  });

  it("flattens to a single unheaded group, sorted by key, when nothing but Other remains after folding", () => {
    const groups = [
      g("Kubernetes", [["k8s.pod.uid", "p"]]),
      g("Service", [["service.name", "s"]]),
      g("Other", [["app.order.id", "1"]]),
    ];
    expect(foldSingletonGroups(groups)).toEqual([
      g(null, [
        ["app.order.id", "1"],
        ["k8s.pod.uid", "p"],
        ["service.name", "s"],
      ]),
    ]);
  });

  it("passes through the already-flat untitled group unchanged", () => {
    const groups = [g<string>(null, [["app.order.id", "1"]])];
    expect(foldSingletonGroups(groups)).toEqual(groups);
  });

  it("returns [] for empty input", () => {
    expect(foldSingletonGroups([])).toEqual([]);
  });
});
