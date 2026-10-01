import { describe, expect, it } from "vitest";
import { errorRatePercent, pctChange, ppChange } from "./entityKpiFormat";

describe("pctChange", () => {
  it("reports a percent increase, rounded, with a plus sign", () => {
    expect(pctChange(11, 10)).toEqual({
      text: "+10% vs prev",
      direction: "up",
    });
  });

  it("reports a percent decrease with a minus sign", () => {
    expect(pctChange(9, 10)).toEqual({
      text: "-10% vs prev",
      direction: "down",
    });
  });

  it("reports flat when it rounds to no change", () => {
    expect(pctChange(10.01, 10)).toEqual({
      text: "flat vs prev",
      direction: "flat",
    });
  });

  it("reports new (not +Infinity%) when the previous period was zero", () => {
    expect(pctChange(5, 0)).toEqual({ text: "new vs prev", direction: "up" });
  });

  it("reports flat when both periods were zero", () => {
    expect(pctChange(0, 0)).toEqual({
      text: "flat vs prev",
      direction: "flat",
    });
  });
});

describe("ppChange", () => {
  it("reports a percentage-point increase", () => {
    expect(ppChange(12, 10)).toEqual({ text: "+2pp vs prev", direction: "up" });
  });

  it("reports a percentage-point decrease", () => {
    expect(ppChange(8, 10)).toEqual({
      text: "-2pp vs prev",
      direction: "down",
    });
  });

  it("reports flat when it rounds to no change", () => {
    expect(ppChange(10.4, 10)).toEqual({
      text: "flat vs prev",
      direction: "flat",
    });
  });
});

describe("errorRatePercent", () => {
  it("renders a clean zero as 0%, not a dash", () => {
    expect(errorRatePercent(0)).toBe("0%");
  });

  it("renders a tiny nonzero rate as <1% rather than a misleading 0%", () => {
    expect(errorRatePercent(0.001)).toBe("<1%");
  });

  it("renders a whole percent rounded", () => {
    expect(errorRatePercent(0.052)).toBe("5%");
  });
});
