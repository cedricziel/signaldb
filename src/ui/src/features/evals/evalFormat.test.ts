import { describe, expect, it } from "vitest";
import { emptyStats, foldStats, type EvalStats } from "./evalModel";
import { fmtMeanOrLabel } from "./evalFormat";

describe("fmtMeanOrLabel", () => {
  const labelled = (label: string, n: number, acc: EvalStats = emptyStats()) =>
    foldStats(acc, {
      label,
      error: null,
      n,
      high: 0,
      low: 0,
      scoreSum: 0,
      scored: 0,
    });

  it("prints the mean when the evaluator scores", () => {
    expect(fmtMeanOrLabel({ ...emptyStats(), scoreSum: 1.7, scored: 2 })).toBe(
      "0.85",
    );
  });

  it("prints the most common label and its share for a label evaluator", () => {
    expect(fmtMeanOrLabel(labelled("unsafe", 1, labelled("safe", 3)))).toBe(
      "safe 75%",
    );
  });

  it("prints a dash with neither score nor label", () => {
    expect(fmtMeanOrLabel(emptyStats())).toBe("—");
  });
});
