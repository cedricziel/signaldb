import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Histogram, normalizeLevel, toVolumeSeries } from "./Histogram";

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

describe("toVolumeSeries", () => {
  it("keys each series by its canonical level", () => {
    expect(
      toVolumeSeries([
        { level: "ERROR", points: [[1000, 3]] },
        { level: "nonsense", points: [[1000, 1]] },
      ]),
    ).toEqual([
      { key: "error", points: [[1000, 3]] },
      { key: "other", points: [[1000, 1]] },
    ]);
  });
});

describe("Histogram", () => {
  const rangeMs = { fromMs: 0, toMs: 300_000 };

  it("stacks level series across the padded window", () => {
    render(
      <Histogram
        series={[
          { level: "info", points: [[60_000, 10]] },
          { level: "error", points: [[60_000, 2]] },
        ]}
        rangeMs={rangeMs}
        stepMs={60_000}
        scale="linear"
      />,
    );
    expect(screen.getAllByTestId("svol-col")).toHaveLength(6);
    const segs = within(screen.getAllByTestId("svol-col")[1]!).getAllByTestId(
      "svol-seg",
    );
    // Bottom-to-top: info below error.
    expect(segs.map((s) => s.dataset["seriesKey"])).toEqual(["info", "error"]);
  });

  it("merges levels that normalize onto the same key", () => {
    render(
      <Histogram
        series={[
          { level: "warn", points: [[60_000, 2]] },
          { level: "warning", points: [[60_000, 5]] },
        ]}
        rangeMs={rangeMs}
        stepMs={60_000}
        scale="linear"
      />,
    );
    expect(screen.getAllByTestId("svol-col")[1]).toHaveAccessibleName(
      /7 lines/,
    );
  });

  it("reports an empty window", () => {
    render(
      <Histogram
        series={[]}
        rangeMs={rangeMs}
        stepMs={60_000}
        scale="linear"
      />,
    );
    expect(screen.getByText(/no volume in range/i)).toBeInTheDocument();
  });
});
