import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { bucketize, Histogram, normalizeLevel } from "./Histogram";

describe("normalizeLevel", () => {
  it("maps synonyms onto canonical levels", () => {
    expect(normalizeLevel("ERROR")).toBe("error");
    expect(normalizeLevel("err")).toBe("error");
    expect(normalizeLevel("fatal")).toBe("error");
    expect(normalizeLevel("WARNING")).toBe("warn");
    expect(normalizeLevel("Information")).toBe("info");
    expect(normalizeLevel("trace")).toBe("debug");
    expect(normalizeLevel("weird")).toBe("other");
  });
});

describe("bucketize", () => {
  it("merges series into time-sorted buckets with totals", () => {
    const buckets = bucketize([
      {
        level: "error",
        points: [
          [2000, 3],
          [1000, 1],
        ],
      },
      { level: "INFO", points: [[1000, 10]] },
    ]);
    expect(buckets).toEqual([
      { tMs: 1000, counts: { error: 1, info: 10 }, total: 11 },
      { tMs: 2000, counts: { error: 3 }, total: 3 },
    ]);
  });

  it("collapses same-normalized levels into one count", () => {
    const buckets = bucketize([
      { level: "warn", points: [[1000, 2]] },
      { level: "warning", points: [[1000, 5]] },
    ]);
    expect(buckets[0]?.counts).toEqual({ warn: 7 });
  });
});

describe("Histogram", () => {
  it("renders one bar per bucket with level segments", () => {
    render(
      <Histogram
        series={[
          {
            level: "error",
            points: [
              [1000, 2],
              [2000, 4],
            ],
          },
          { level: "info", points: [[1000, 6]] },
        ]}
      />,
    );
    const bars = screen.getAllByTestId("histo-bar");
    expect(bars).toHaveLength(2);
    expect(bars[0]?.querySelector('[data-level="info"]')).toBeTruthy();
    expect(bars[0]?.querySelector('[data-level="error"]')).toBeTruthy();
    expect(bars[1]?.querySelector('[data-level="info"]')).toBeNull();
  });

  it("shows an empty state without data", () => {
    render(<Histogram series={[]} />);
    expect(screen.getByText(/No log volume/)).toBeInTheDocument();
  });
});
