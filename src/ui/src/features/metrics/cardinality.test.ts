import { describe, expect, it } from "vitest";
import type { DiscoveredField } from "../../api/gen";
import {
  cardinalityLabel,
  indexFields,
  isHighCardinality,
  optionLabel,
} from "./cardinality";

const field = (patch: Partial<DiscoveredField> = {}): DiscoveredField => ({
  name: "x",
  type: "string",
  filterable: true,
  origin: "declared",
  cardinality: { estimate: 10, at_least: false },
  ...patch,
});

describe("isHighCardinality", () => {
  it("is false for unknown or low-cardinality labels", () => {
    expect(isHighCardinality(undefined)).toBe(false);
    expect(isHighCardinality(field({ cardinality: null }))).toBe(false);
    expect(
      isHighCardinality(
        field({ cardinality: { estimate: 12, at_least: false } }),
      ),
    ).toBe(false);
  });

  it("flags counts at or above the threshold", () => {
    expect(
      isHighCardinality(
        field({ cardinality: { estimate: 1000, at_least: false } }),
      ),
    ).toBe(true);
    expect(
      isHighCardinality(
        field({ cardinality: { estimate: 5000, at_least: false } }),
      ),
    ).toBe(true);
  });

  it("always flags a capped estimate", () => {
    expect(
      isHighCardinality(
        field({ cardinality: { estimate: 10, at_least: true } }),
      ),
    ).toBe(true);
  });
});

describe("cardinalityLabel", () => {
  it("returns null for unknown cardinality", () => {
    expect(cardinalityLabel(undefined)).toBeNull();
    expect(cardinalityLabel(field({ cardinality: null }))).toBeNull();
  });

  it("uses ≈ for estimates and ≥ for a capped collector estimate", () => {
    expect(
      cardinalityLabel(
        field({ cardinality: { estimate: 240, at_least: false } }),
      ),
    ).toBe("≈240 values");
    expect(
      cardinalityLabel(
        field({ cardinality: { estimate: 10000, at_least: true } }),
      ),
    ).toBe("≥10000 values");
  });
});

describe("optionLabel", () => {
  it("is undefined when cardinality is unknown", () => {
    expect(optionLabel(undefined)).toBeUndefined();
  });

  it("appends a warning marker only when high", () => {
    expect(
      optionLabel(field({ cardinality: { estimate: 12, at_least: false } })),
    ).toBe("≈12 values");
    expect(
      optionLabel(field({ cardinality: { estimate: 2000, at_least: false } })),
    ).toBe("≈2000 values ⚠");
  });
});

describe("indexFields", () => {
  it("keys fields by name", () => {
    const idx = indexFields([
      field({ name: "service" }),
      field({ name: "pod" }),
    ]);
    expect(idx.get("service")?.name).toBe("service");
    expect(idx.get("pod")?.name).toBe("pod");
    expect(idx.get("missing")).toBeUndefined();
  });
});
