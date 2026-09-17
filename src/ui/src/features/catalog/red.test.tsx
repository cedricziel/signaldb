import { describe, expect, it } from "vitest";
import { redErrorClass, redErrorRate } from "./red";
import type { EntityRed } from "../../api/catalog";

function red(overrides: Partial<EntityRed> = {}): EntityRed {
  return { traces: 500, errors: 0, p50Ms: 1, p95Ms: 2, ...overrides };
}

describe("redErrorRate", () => {
  it("renders a dash for an entity with no trace measurement at all", () => {
    expect(redErrorRate(undefined)).toBe("–");
  });

  it("renders a dash for a measured, genuinely clean rate", () => {
    expect(redErrorRate(red({ errors: 0 }))).toBe("–");
  });

  // The bug: a nonzero error count that rounds to "0%" still carried the
  // red "this had errors" styling, reading as a contradiction.
  it("renders <1% — not a misleading 0% — for a nonzero rate that rounds to zero", () => {
    expect(redErrorRate(red({ errors: 1, traces: 500 }))).toBe("<1%");
  });

  it("rounds a rate above the cutoff to the nearest whole percent", () => {
    expect(redErrorRate(red({ errors: 50, traces: 500 }))).toBe("10%");
  });
});

describe("redErrorClass", () => {
  it("is unset for no measurement or a clean rate", () => {
    expect(redErrorClass(undefined)).toBe(false);
    expect(redErrorClass(red({ errors: 0 }))).toBe(false);
  });

  // Even a rate small enough to render "<1%" is still a real error and
  // keeps the red styling — the fix corrects the text, not the styling rule.
  it("is set for any nonzero error count", () => {
    expect(redErrorClass(red({ errors: 1, traces: 500 }))).toBe(true);
  });
});
