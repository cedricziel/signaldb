import { describe, expect, it } from "vitest";
import {
  alignSeries,
  seriesColorVar,
  seriesDash,
  SERIES_COLOR_VARS,
} from "./promSeries";

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
  it("has a twelve-color categorical palette, plus --accent/--info", () => {
    expect(SERIES_COLOR_VARS).toHaveLength(14);
  });

  it("cycles the full palette before repeating", () => {
    expect(seriesColorVar(0)).toBe("var(--accent)");
    expect(seriesColorVar(SERIES_COLOR_VARS.length)).toBe("var(--accent)");
    expect(seriesColorVar(1)).not.toBe(seriesColorVar(2));
  });

  it("gives every one of the 12 series colors its own distinct value", () => {
    const vars = Array.from({ length: SERIES_COLOR_VARS.length }, (_, i) =>
      seriesColorVar(i),
    );
    expect(new Set(vars).size).toBe(SERIES_COLOR_VARS.length);
  });
});

describe("seriesDash", () => {
  it("is solid for every series in the first pass through the palette", () => {
    for (let i = 0; i < SERIES_COLOR_VARS.length; i++) {
      expect(seriesDash(i)).toBeUndefined();
    }
  });

  it("varies the dash pattern once colors repeat", () => {
    const firstPass = seriesDash(0);
    const secondPass = seriesDash(SERIES_COLOR_VARS.length);
    expect(firstPass).toBeUndefined();
    expect(secondPass).toBeDefined();

    const thirdPass = seriesDash(SERIES_COLOR_VARS.length * 2);
    expect(thirdPass).toBeDefined();
    expect(thirdPass).not.toEqual(secondPass);
  });
});
