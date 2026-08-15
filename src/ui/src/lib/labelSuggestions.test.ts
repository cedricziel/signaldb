import { describe, expect, it } from "vitest";
import type { AttributeHit } from "../api/gen";
import { mergeLabelSuggestions } from "./labelSuggestions";

const hit = (key: string, brief: string): AttributeHit => ({
  key,
  brief,
  type: "string",
  group_id: "registry.http",
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
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
        seen: true,
      },
      {
        key: "http.response.status_code",
        brief: "HTTP response status code.",
        namespace: "otel",
        seen: false,
      },
      { key: "http.retries", brief: null, namespace: null, seen: true },
    ]);
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
