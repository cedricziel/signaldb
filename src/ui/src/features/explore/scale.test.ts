import { describe, expect, it } from "vitest";
import {
  barHeight,
  isScale,
  scaleFraction,
  splitSegments,
  valueAtFraction,
} from "./scale";

describe("scaleFraction", () => {
  it("maps linearly against the maximum", () => {
    expect(scaleFraction(0, 100, "linear")).toBe(0);
    expect(scaleFraction(50, 100, "linear")).toBe(0.5);
    expect(scaleFraction(100, 100, "linear")).toBe(1);
  });

  it("compresses the top of the range in log mode", () => {
    // log1p(v)/log1p(max) — the measured production window.
    expect(scaleFraction(525, 373_329, "log")).toBeCloseTo(0.489, 2);
    expect(scaleFraction(10_240, 373_329, "log")).toBeCloseTo(0.722, 2);
    expect(scaleFraction(373_329, 373_329, "log")).toBe(1);
  });

  it("keeps zero at the floor in both modes", () => {
    expect(scaleFraction(0, 373_329, "log")).toBe(0);
    expect(scaleFraction(0, 373_329, "linear")).toBe(0);
  });

  it("treats a non-positive maximum as an empty window", () => {
    expect(scaleFraction(0, 0, "linear")).toBe(0);
    expect(scaleFraction(5, 0, "log")).toBe(0);
  });
});

describe("barHeight", () => {
  const available = 68;

  it("renders an empty bucket at zero", () => {
    expect(barHeight(0, 373_329, available, "linear")).toBe(0);
  });

  it("floors a non-empty bucket at one pixel", () => {
    expect(barHeight(1, 373_329, available, "linear")).toBe(1);
  });

  // The regression this change exists to fix: under the old renderer both of
  // these collapsed onto the per-segment floor and were indistinguishable.
  it("distinguishes buckets two orders of magnitude apart", () => {
    const small = barHeight(525, 373_329, available, "linear");
    const large = barHeight(10_240, 373_329, available, "linear");
    expect(large).toBeGreaterThan(small);
  });

  it("never exceeds the available height", () => {
    expect(barHeight(373_329, 373_329, available, "linear")).toBe(available);
    expect(barHeight(373_329, 373_329, available, "log")).toBe(available);
  });
});

describe("splitSegments", () => {
  it("divides the bar by each series' linear share", () => {
    const segs = splitSegments({ info: 75, error: 25 }, ["info", "error"], 40);
    expect(segs).toEqual([
      { key: "info", px: 30 },
      { key: "error", px: 10 },
    ]);
  });

  it("sums to exactly the bar height, including in log mode", () => {
    const bar = barHeight(10_240, 373_329, 68, "log");
    const segs = splitSegments(
      { debug: 1, info: 10_000, warn: 200, error: 39 },
      ["debug", "info", "warn", "error"],
      bar,
    );
    const sum = segs.reduce((a, s) => a + s.px, 0);
    expect(sum).toBeCloseTo(bar, 10);
  });

  it("keeps the caller's series order and drops absent series", () => {
    const segs = splitSegments(
      { error: 5, debug: 5 },
      ["debug", "info", "error"],
      20,
    );
    expect(segs.map((s) => s.key)).toEqual(["debug", "error"]);
  });

  it("returns nothing for an empty bucket", () => {
    expect(splitSegments({}, ["info"], 0)).toEqual([]);
    expect(splitSegments({ info: 0 }, ["info"], 0)).toEqual([]);
  });
});

describe("isScale", () => {
  it("accepts the known scales and rejects anything else", () => {
    expect(isScale("linear")).toBe(true);
    expect(isScale("log")).toBe(true);
    expect(isScale("logarithmic")).toBe(false);
    expect(isScale("")).toBe(false);
  });
});

describe("valueAtFraction", () => {
  it("inverts the linear mapping", () => {
    expect(valueAtFraction(0.5, 400, "linear")).toBe(200);
    expect(valueAtFraction(1, 400, "linear")).toBe(400);
    expect(valueAtFraction(0, 400, "linear")).toBe(0);
  });

  // The gridline halfway up a log plot is nowhere near half the maximum;
  // labelling it max/2 made the axis lie in log mode.
  it("inverts the log mapping", () => {
    expect(valueAtFraction(1, 373_329, "log")).toBeCloseTo(373_329, 0);
    expect(valueAtFraction(0.5, 373_329, "log")).toBeCloseTo(610, 0);
    expect(valueAtFraction(0, 373_329, "log")).toBe(0);
  });

  it("round-trips against scaleFraction", () => {
    for (const scale of ["linear", "log"] as const) {
      const v = valueAtFraction(0.5, 373_329, scale);
      expect(scaleFraction(v, 373_329, scale)).toBeCloseTo(0.5, 6);
    }
  });
});

describe("splitSegments minimum visibility", () => {
  // A 0.03% error share must not vanish: on live data 6 errors against
  // 18,600 unset spans rounded to sub-pixel and disappeared entirely.
  it("keeps a tiny segment visible", () => {
    const segs = splitSegments(
      { unset: 18_600, error: 6 },
      ["unset", "error"],
      60,
    );
    const err = segs.find((s) => s.key === "error")!;
    expect(err.px).toBeGreaterThanOrEqual(1);
  });

  it("takes the space from the largest segment, not from the bar", () => {
    const bar = 60;
    const segs = splitSegments(
      { unset: 18_600, error: 6 },
      ["unset", "error"],
      bar,
    );
    expect(segs.reduce((a, s) => a + s.px, 0)).toBeCloseTo(bar, 6);
  });

  it("leaves comfortable segments proportional", () => {
    const segs = splitSegments({ info: 75, error: 25 }, ["info", "error"], 40);
    expect(segs).toEqual([
      { key: "info", px: 30 },
      { key: "error", px: 10 },
    ]);
  });

  it("shares a bar too small for every series to meet the floor", () => {
    const segs = splitSegments(
      { debug: 1, info: 1, warn: 1, error: 1 },
      ["debug", "info", "warn", "error"],
      2,
    );
    expect(segs).toHaveLength(4);
    expect(segs.reduce((a, s) => a + s.px, 0)).toBeCloseTo(2, 6);
  });

  it("still drops series absent from the bucket", () => {
    const segs = splitSegments({ unset: 100 }, ["ok", "unset", "error"], 30);
    expect(segs.map((s) => s.key)).toEqual(["unset"]);
    expect(segs[0]!.px).toBeCloseTo(30, 6);
  });
});
