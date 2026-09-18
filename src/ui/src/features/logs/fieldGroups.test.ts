import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../../api/gen";
import { semanticsFromResolution, type SemanticsMap } from "../../lib/semantics";
import { groupFields } from "./fieldGroups";

function hit(key: string, overrides: Partial<AttributeHit> = {}): AttributeHit {
  return {
    key,
    brief: "",
    group_id: "g",
    type: "string",
    namespace: "otel",
    version: "1.0.0",
    source: "bundled",
    ...overrides,
  };
}

/** Build one resolved (or deprecated) semantics entry from a bare key. */
function resolved(
  key: string,
  overrides: Partial<AttributeHit> = {},
): NonNullable<ReturnType<typeof semanticsFromResolution>> {
  const primary = hit(key, overrides);
  const sem = semanticsFromResolution({ key, hits: [primary], primary });
  if (!sem) throw new Error(`expected ${key} to resolve`);
  return sem;
}

function semanticsMap(
  entries: [string, ReturnType<typeof resolved>][],
): SemanticsMap {
  return new Map(entries);
}

describe("groupFields", () => {
  it("returns one flat, untitled group when nothing resolved", () => {
    const labels = ["level", "b_field", "a_field"];
    expect(groupFields(labels, new Map())).toEqual([
      { id: "all", title: "", labels },
    ]);
  });

  it("pins the Line group first, in fixed order, for present line labels only", () => {
    const labels = ["event.name", "service_name", "level", "other_field"];
    const semantics = semanticsMap([
      ["other_field", resolved("other_field", { group_display_name: "Zeta Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups[0]).toEqual({
      id: "line",
      title: "Line",
      // Fixed order (level, detected_level, service_name, service.name,
      // event.name), not input order — and only the labels present.
      labels: ["level", "service_name", "event.name"],
    });
  });

  it("omits the Line group when none of its labels are present", () => {
    const labels = ["foo"];
    const semantics = semanticsMap([
      ["foo", resolved("foo", { group_display_name: "Foo Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups.some((g) => g.id === "line")).toBe(false);
  });

  it("groups remaining resolved labels by title, sorted by title and alphabetically within a group", () => {
    const labels = ["k8s.pod.uid", "k8s.node.name", "cloud.region"];
    const semantics = semanticsMap([
      ["k8s.pod.uid", resolved("k8s.pod.uid", { group_display_name: "Kubernetes Attributes" })],
      ["k8s.node.name", resolved("k8s.node.name", { group_display_name: "Kubernetes Attributes" })],
      ["cloud.region", resolved("cloud.region", { group_display_name: "Cloud Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups).toEqual([
      { id: "t-cloud", title: "Cloud", labels: ["cloud.region"] },
      {
        id: "t-kubernetes",
        title: "Kubernetes",
        labels: ["k8s.node.name", "k8s.pod.uid"],
      },
    ]);
  });

  it("strips a trailing ' Attributes' from the displayed title but keeps it out of the id twice", () => {
    const labels = ["foo.bar"];
    const semantics = semanticsMap([
      ["foo.bar", resolved("foo.bar", { group_display_name: "Foo Bar Attributes" })],
    ]);
    expect(groupFields(labels, semantics)).toEqual([
      { id: "t-foo-bar", title: "Foo Bar", labels: ["foo.bar"] },
    ]);
  });

  it("namespace-derived ids never collide with the sentinel group ids", () => {
    const labels = ["other.field", "line.field", "deprecated.field", "all.field"];
    const semantics = semanticsMap([
      ["other.field", resolved("other.field", { group_display_name: "Other" })],
      ["line.field", resolved("line.field", { group_display_name: "Line" })],
      [
        "deprecated.field",
        resolved("deprecated.field", { group_display_name: "Deprecated" }),
      ],
      ["all.field", resolved("all.field", { group_display_name: "All" })],
    ]);
    const groups = groupFields(labels, semantics);
    const ids = groups.map((g) => g.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toEqual(
      expect.arrayContaining(["t-other", "t-line", "t-deprecated", "t-all"]),
    );
  });

  it("puts resolved-but-deprecated labels in a trailing Deprecated group", () => {
    const labels = ["old.key", "cloud.region"];
    const semantics = semanticsMap([
      [
        "old.key",
        resolved("old.key", {
          group_display_name: "Cloud Attributes",
          deprecated: { renamed_to: "new.key" },
        }),
      ],
      ["cloud.region", resolved("cloud.region", { group_display_name: "Cloud Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups).toEqual([
      { id: "t-cloud", title: "Cloud", labels: ["cloud.region"] },
      { id: "deprecated", title: "Deprecated", labels: ["old.key"] },
    ]);
  });

  it("puts unresolved labels in a trailing Other group", () => {
    const labels = ["cloud.region", "mystery_field"];
    const semantics = semanticsMap([
      ["cloud.region", resolved("cloud.region", { group_display_name: "Cloud Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups).toEqual([
      { id: "t-cloud", title: "Cloud", labels: ["cloud.region"] },
      { id: "other", title: "Other", labels: ["mystery_field"] },
    ]);
  });

  it("omits empty groups (no Deprecated, no Other, when nothing qualifies)", () => {
    const labels = ["cloud.region"];
    const semantics = semanticsMap([
      ["cloud.region", resolved("cloud.region", { group_display_name: "Cloud Attributes" })],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups.map((g) => g.id)).toEqual(["t-cloud"]);
  });

  it("composes Line, title groups, Deprecated, and Other together in order", () => {
    const labels = ["level", "cloud.region", "old.key", "mystery_field"];
    const semantics = semanticsMap([
      ["cloud.region", resolved("cloud.region", { group_display_name: "Cloud Attributes" })],
      [
        "old.key",
        resolved("old.key", {
          group_display_name: "Cloud Attributes",
          deprecated: { renamed_to: "new.key" },
        }),
      ],
    ]);
    const groups = groupFields(labels, semantics);
    expect(groups.map((g) => g.id)).toEqual([
      "line",
      "t-cloud",
      "deprecated",
      "other",
    ]);
  });
});
