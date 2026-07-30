import { describe, expect, it } from "vitest";
import type { LabelStat } from "../../api/prom";
import {
  cardinalityLabel,
  indexLabelStats,
  isHighCardinality,
  optionLabel,
} from "./cardinality";

const stat = (patch: Partial<LabelStat>): LabelStat => ({
  name: "x",
  distinct_estimate: 10,
  presence: 1,
  capped: false,
  ...patch,
});

describe("isHighCardinality", () => {
  it("is false for unknown or low-cardinality labels", () => {
    expect(isHighCardinality(undefined)).toBe(false);
    expect(isHighCardinality(stat({ distinct_estimate: 12 }))).toBe(false);
  });

  it("flags counts at or above the threshold", () => {
    expect(isHighCardinality(stat({ distinct_estimate: 1000 }))).toBe(true);
    expect(isHighCardinality(stat({ distinct_estimate: 5000 }))).toBe(true);
  });

  it("always flags a capped estimate", () => {
    expect(
      isHighCardinality(stat({ distinct_estimate: 10, capped: true })),
    ).toBe(true);
  });
});

describe("cardinalityLabel", () => {
  it("returns null for unknown cardinality", () => {
    expect(cardinalityLabel(undefined)).toBeNull();
  });

  it("uses ≈ for estimates and ≥ for capped", () => {
    expect(cardinalityLabel(stat({ distinct_estimate: 240 }))).toBe(
      "≈240 values",
    );
    expect(
      cardinalityLabel(stat({ distinct_estimate: 10000, capped: true })),
    ).toBe("≥10000 values");
  });
});

describe("optionLabel", () => {
  it("is undefined when cardinality is unknown", () => {
    expect(optionLabel(undefined)).toBeUndefined();
  });

  it("appends a warning marker only when high", () => {
    expect(optionLabel(stat({ distinct_estimate: 12 }))).toBe("≈12 values");
    expect(optionLabel(stat({ distinct_estimate: 2000 }))).toBe(
      "≈2000 values ⚠",
    );
  });
});

describe("indexLabelStats", () => {
  it("keys stats by name", () => {
    const idx = indexLabelStats([
      stat({ name: "service" }),
      stat({ name: "pod" }),
    ]);
    expect(idx.get("service")?.name).toBe("service");
    expect(idx.get("pod")?.name).toBe("pod");
    expect(idx.get("missing")).toBeUndefined();
  });
});
