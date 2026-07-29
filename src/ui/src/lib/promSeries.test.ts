import { describe, expect, it } from "vitest";
import { alignSeries, seriesColorVar } from "./promSeries";

describe("alignSeries", () => {
  it("aligns series onto a shared sorted time axis with null gaps", () => {
    const data = alignSeries([
      {
        labels: {},
        points: [
          [1000, 1],
          [3000, 3],
        ],
      },
      {
        labels: {},
        points: [
          [2000, 2],
          [3000, 30],
        ],
      },
    ]);
    expect(data).toEqual([
      [1000, 2000, 3000],
      [1, null, 3],
      [null, 2, 30],
    ]);
  });

  it("returns just the axis for no series", () => {
    expect(alignSeries([])).toEqual([[]]);
  });
});

describe("seriesColorVar", () => {
  it("cycles the palette", () => {
    expect(seriesColorVar(0)).toBe("var(--accent)");
    expect(seriesColorVar(6)).toBe("var(--accent)");
    expect(seriesColorVar(1)).not.toBe(seriesColorVar(2));
  });
});
