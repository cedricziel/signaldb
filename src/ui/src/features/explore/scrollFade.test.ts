import { describe, expect, it } from "vitest";
import { computeEdgeOverflow } from "./scrollFade";

describe("computeEdgeOverflow", () => {
  it("reports neither edge when the content fits", () => {
    expect(
      computeEdgeOverflow({
        scrollLeft: 0,
        scrollWidth: 300,
        clientWidth: 300,
      }),
    ).toEqual({ left: false, right: false });
  });

  it("reports only the right edge at the start of an overflowing strip", () => {
    expect(
      computeEdgeOverflow({
        scrollLeft: 0,
        scrollWidth: 600,
        clientWidth: 300,
      }),
    ).toEqual({ left: false, right: true });
  });

  it("reports only the left edge at the end of an overflowing strip", () => {
    expect(
      computeEdgeOverflow({
        scrollLeft: 300,
        scrollWidth: 600,
        clientWidth: 300,
      }),
    ).toEqual({ left: true, right: false });
  });

  it("reports both edges in the middle of an overflowing strip", () => {
    expect(
      computeEdgeOverflow({
        scrollLeft: 150,
        scrollWidth: 600,
        clientWidth: 300,
      }),
    ).toEqual({ left: true, right: true });
  });

  it("absorbs sub-pixel rounding at the exact edges", () => {
    expect(
      computeEdgeOverflow({
        scrollLeft: 0.3,
        scrollWidth: 600.4,
        clientWidth: 300.2,
      }),
    ).toEqual({ left: false, right: true });
    expect(
      computeEdgeOverflow({
        scrollLeft: 300.2,
        scrollWidth: 600.4,
        clientWidth: 300.2,
      }),
    ).toEqual({ left: true, right: false });
  });
});
