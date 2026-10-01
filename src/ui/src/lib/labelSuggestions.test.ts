import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../api/gen";
import { mergeLabelSuggestions, toLokiLabel } from "./labelSuggestions";

const hit = (
  key: string,
  brief: string,
  over: Partial<AttributeHit> = {},
): AttributeHit => ({
  key,
  brief,
  type: "string",
  group_id: "registry.http",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
  ...over,
});

describe("mergeLabelSuggestions", () => {
  it("lists registry hits with briefs, marking observed ones, then observed-only keys", () => {
    const out = mergeLabelSuggestions(
      "http.re",
      [
        hit("http.request.method", "HTTP request method."),
        hit("http.response.status_code", "HTTP response status code."),
      ],
      ["http.request.method", "http.retries", "level"],
    );
    expect(out).toEqual([
      {
        key: "http.request.method",
        brief: "HTTP request method.",
        namespace: "otel",
        source: "bundled",
        deprecatedTo: null,
        deprecated: false,
        seen: true,
      },
      {
        key: "http.response.status_code",
        brief: "HTTP response status code.",
        namespace: "otel",
        source: "bundled",
        deprecatedTo: null,
        deprecated: false,
        seen: false,
      },
      {
        key: "http.retries",
        brief: null,
        namespace: null,
        source: null,
        deprecatedTo: null,
        deprecated: false,
        seen: true,
      },
    ]);
  });

  it("orders non-deprecated registry hits before deprecated ones, each keeping the server's order", () => {
    const out = mergeLabelSuggestions(
      "http.",
      [
        hit("http.status_code", "Deprecated.", {
          deprecated: { renamed_to: "http.response.status_code" },
        }),
        hit("http.request.method", "HTTP request method."),
        hit("http.host", "Deprecated.", { deprecated: { reason: "old" } }),
        hit("http.response.status_code", "HTTP response status code."),
      ],
      [],
    );
    expect(out.map((s) => s.key)).toEqual([
      "http.request.method",
      "http.response.status_code",
      "http.status_code",
      "http.host",
    ]);
    expect(out.map((s) => s.deprecated)).toEqual([
      false,
      false,
      true,
      true,
    ]);
    expect(
      out.find((s) => s.key === "http.status_code")?.deprecatedTo,
    ).toBe("http.response.status_code");
    expect(out.find((s) => s.key === "http.host")?.deprecatedTo).toBeNull();
  });

  it("matches observed underscore-flattened labels against a dotted prefix", () => {
    const out = mergeLabelSuggestions("service.na", [], ["service_name"]);
    expect(out.map((s) => s.key)).toEqual(["service_name"]);
  });

  it("returns nothing for a blank prefix", () => {
    expect(mergeLabelSuggestions("  ", [hit("a", "b")], ["a"])).toEqual([]);
  });

  it("caps the list", () => {
    const observed = Array.from({ length: 30 }, (_, i) => `k.${i}`);
    expect(
      mergeLabelSuggestions("k.", [], observed).length,
    ).toBeLessThanOrEqual(12);
  });
});

describe("toLokiLabel", () => {
  it("flattens dots to underscores", () => {
    expect(toLokiLabel("service.name")).toBe("service_name");
    expect(toLokiLabel("k8s.pod.uid")).toBe("k8s_pod_uid");
  });

  it("leaves an already-flat label alone", () => {
    expect(toLokiLabel("level")).toBe("level");
  });
});
