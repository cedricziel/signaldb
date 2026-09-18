import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../../api/gen";
import { semanticsFromResolution, type SemanticsMap } from "../../lib/semantics";
import { splitLogScopes } from "./logScopes";

const hit = (over: Partial<AttributeHit> = {}): AttributeHit => ({
  key: "k8s.pod.name",
  brief: "",
  type: "string",
  group_id: "registry.k8s.pod",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  ...over,
});

function semanticsMap(entries: [string, AttributeHit][]): SemanticsMap {
  const map = new Map();
  for (const [key, primary] of entries) {
    const sem = semanticsFromResolution({ key, hits: [primary], primary });
    if (sem) map.set(key, sem);
  }
  return map;
}

describe("splitLogScopes", () => {
  it("puts every stream label under resource, regardless of semantics", () => {
    const labels: [string, string][] = [
      ["level", "error"],
      ["service_name", "checkout"],
    ];
    const { line, resource } = splitLogScopes(labels, [], new Map());
    expect(line).toEqual([]);
    expect(resource).toEqual(labels);
  });

  it("puts a metadata key with a resolved entity role under resource", () => {
    const semantics = semanticsMap([
      [
        "k8s.pod.name",
        hit({
          entity_roles: [{ namespace: "otel", entity: "k8s.pod", role: "identifying" }],
        }),
      ],
    ]);
    const { line, resource } = splitLogScopes(
      [],
      [["k8s.pod.name", "web-1"]],
      semantics,
    );
    expect(line).toEqual([]);
    expect(resource).toEqual([["k8s.pod.name", "web-1"]]);
  });

  it("puts a metadata key with no entity role under line", () => {
    const semantics = semanticsMap([
      ["code.function.name", hit({ key: "code.function.name" })],
    ]);
    const { line, resource } = splitLogScopes(
      [],
      [["code.function.name", "handle"]],
      semantics,
    );
    expect(line).toEqual([["code.function.name", "handle"]]);
    expect(resource).toEqual([]);
  });

  it("puts an unresolved metadata key under line", () => {
    const { line, resource } = splitLogScopes(
      [],
      [["app.order.id", "o-1"]],
      new Map(),
    );
    expect(line).toEqual([["app.order.id", "o-1"]]);
    expect(resource).toEqual([]);
  });

  it("keeps trace_id/span_id on the line even if the registry ever gave them a role", () => {
    const semantics = semanticsMap([
      [
        "trace_id",
        hit({
          key: "trace_id",
          entity_roles: [{ namespace: "otel", entity: "trace", role: "identifying" }],
        }),
      ],
      [
        "span_id",
        hit({
          key: "span_id",
          entity_roles: [{ namespace: "otel", entity: "span", role: "identifying" }],
        }),
      ],
    ]);
    const { line, resource } = splitLogScopes(
      [],
      [
        ["span_id", "s-1"],
        ["trace_id", "t-1"],
      ],
      semantics,
    );
    expect(line).toEqual([
      ["span_id", "s-1"],
      ["trace_id", "t-1"],
    ]);
    expect(resource).toEqual([]);
  });

  it("interleaves labels and resource-role metadata into one resource scope", () => {
    const semantics = semanticsMap([
      [
        "k8s.pod.name",
        hit({
          entity_roles: [{ namespace: "otel", entity: "k8s.pod", role: "identifying" }],
        }),
      ],
    ]);
    const { line, resource } = splitLogScopes(
      [["service_name", "checkout"]],
      [
        ["k8s.pod.name", "web-1"],
        ["code.function.name", "handle"],
      ],
      semantics,
    );
    expect(line).toEqual([["code.function.name", "handle"]]);
    expect(resource).toEqual([
      ["service_name", "checkout"],
      ["k8s.pod.name", "web-1"],
    ]);
  });
});
