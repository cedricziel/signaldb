import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
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
    expect(screen.getByText(/no volume in this window/i)).toBeInTheDocument();
  });
});

describe("Histogram bucket control", () => {
  it("forwards the bucket-width control to the shared chart", async () => {
    const onStepChange = vi.fn();
    render(
      <Histogram
        series={[{ level: "info", points: [[60_000, 10]] }]}
        rangeMs={{ fromMs: 0, toMs: 300_000 }}
        stepMs={60_000}
        scale="linear"
        step="1m"
        stepOptions={["1m", "5m"]}
        onStepChange={onStepChange}
      />,
    );
    await userEvent.selectOptions(screen.getByLabelText(/bucket width/i), "5m");
    expect(onStepChange).toHaveBeenCalledWith("5m");
  });
});

describe("Histogram bucket tooltip", () => {
  const rangeMs = { fromMs: 0, toMs: 300_000 };
  const series = [
    { level: "info", points: [[60_000, 10]] as [number, number][] },
    { level: "error", points: [[60_000, 2]] as [number, number][] },
  ];

  it("shows each level with its swatch and the bucket total on hover", () => {
    render(
      <Histogram
        series={series}
        rangeMs={rangeMs}
        stepMs={60_000}
        scale="linear"
      />,
    );
    const col = screen.getAllByTestId("svol-col")[1]!;
    fireEvent.mouseEnter(col, { clientX: 100, clientY: 20 });
    const tip = screen.getByRole("tooltip");
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows.map((r) => r.textContent)).toEqual([
      "info10 lines",
      "error2 lines",
    ]);
    expect(
      within(rows[1]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("var(--err-bar)");
    expect(within(tip).getByTestId("viz-tip-footer")).toHaveTextContent(
      "total12 lines",
    );
    expect(col).toHaveAttribute("aria-describedby", tip.id);
  });

  it("reaches the same tooltip by focusing a bar", () => {
    render(
      <Histogram
        series={series}
        rangeMs={rangeMs}
        stepMs={60_000}
        scale="linear"
      />,
    );
    fireEvent.focus(screen.getAllByTestId("svol-col")[1]!);
    expect(screen.getByRole("tooltip")).toHaveTextContent("12 lines");
  });
});
