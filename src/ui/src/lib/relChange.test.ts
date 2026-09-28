import { describe, expect, it } from "vitest";
import { relChange } from "./relChange";

describe("relChange", () => {
  it("reads a rise as bad when upIsBad", () => {
    expect(relChange(110, 100, true)).toEqual({
      text: "+10%",
      tone: "bad",
      direction: "up",
    });
  });

  it("reads the same rise as good when a rise is good", () => {
    expect(relChange(110, 100, false)).toMatchObject({ tone: "good" });
  });

  it("reads a fall the opposite way from a rise", () => {
    expect(relChange(90, 100, true)).toMatchObject({
      tone: "good",
      direction: "down",
    });
  });

  it("rounds a sub-percent change to ±0%, neutral", () => {
    expect(relChange(100.2, 100, true)).toEqual({
      text: "±0%",
      tone: "neutral",
      direction: "flat",
    });
  });

  it("has no change figure with nothing to compare against", () => {
    expect(relChange(10, 0, true)).toBeUndefined();
  });
});
