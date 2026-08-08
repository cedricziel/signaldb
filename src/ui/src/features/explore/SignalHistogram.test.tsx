import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import {
  bucketizeSeries,
  compactCount,
  padBuckets,
  SignalHistogram,
  type VolumeSeries,
} from "./SignalHistogram";

const ORDER = ["info", "error"];
const COLORS = { info: "blue", error: "red" };

function renderChart(series: VolumeSeries[], props = {}) {
  return render(
    <SignalHistogram
      series={series}
      order={ORDER}
      colors={COLORS}
      rangeMs={{ fromMs: 0, toMs: 300_000 }}
      stepMs={60_000}
      scale="linear"
      unit="lines"
      label="Log volume"
      {...props}
    />,
  );
}

describe("bucketizeSeries", () => {
  it("merges series into time-sorted buckets with totals", () => {
    expect(
      bucketizeSeries([
        {
          key: "error",
          points: [
            [2000, 3],
            [1000, 1],
          ],
        },
        { key: "info", points: [[1000, 10]] },
      ]),
    ).toEqual([
      { tMs: 1000, counts: { error: 1, info: 10 }, total: 11 },
      { tMs: 2000, counts: { error: 3 }, total: 3 },
    ]);
  });

  it("sums repeated keys at the same timestamp", () => {
    const buckets = bucketizeSeries([
      { key: "info", points: [[1000, 2]] },
      { key: "info", points: [[1000, 5]] },
    ]);
    expect(buckets[0]?.counts).toEqual({ info: 7 });
  });
});

describe("padBuckets", () => {
  it("zero-fills the window on the epoch-aligned grid", () => {
    const padded = padBuckets(
      [{ tMs: 120_000, counts: { info: 5 }, total: 5 }],
      0,
      300_000,
      60_000,
    );
    expect(padded.map((b) => b.tMs)).toEqual([
      0, 60_000, 120_000, 180_000, 240_000, 300_000,
    ]);
    expect(padded.map((b) => b.total)).toEqual([0, 0, 5, 0, 0, 0]);
  });
});

describe("SignalHistogram", () => {
  const series: VolumeSeries[] = [
    {
      key: "info",
      points: [
        [60_000, 525],
        [120_000, 10_240],
        [180_000, 373_329],
      ],
    },
    { key: "error", points: [[180_000, 12]] },
  ];

  it("renders one column per bucket across the whole window", () => {
    renderChart(series);
    expect(screen.getAllByTestId("svol-col")).toHaveLength(6);
  });

  it("distinguishes buckets two orders of magnitude apart", () => {
    renderChart(series);
    const cols = screen.getAllByTestId("svol-col");
    const barPx = (i: number) =>
      parseFloat(within(cols[i]!).getByTestId("svol-bar").style.height || "0");
    // 525 vs 10,240 against a 373,329 max — the collapsed case.
    expect(barPx(2)).toBeGreaterThan(barPx(1));
    expect(barPx(1)).toBeGreaterThan(0);
  });

  it("stacks segments in the caller's order with their colours", () => {
    renderChart(series);
    const peak = screen.getAllByTestId("svol-col")[3]!;
    const segs = within(peak).getAllByTestId("svol-seg");
    expect(segs.map((s) => s.dataset["seriesKey"])).toEqual(["info", "error"]);
    expect(segs[0]!.style.background).toBe("blue");
  });

  it("labels the vertical axis with the window maximum", () => {
    renderChart(series);
    expect(screen.getByTestId("svol-ymax")).toHaveTextContent("373K lines");
  });

  it("labels each end of the time axis distinctly", () => {
    renderChart(series);
    const axis = screen.getByTestId("svol-xaxis");
    const [start, end] = within(axis).getAllByRole("presentation");
    expect(start!.textContent).not.toBe(end!.textContent);
  });

  it("shows an empty state when nothing is in the window", () => {
    renderChart([]);
    expect(screen.getByText(/no volume in range/i)).toBeInTheDocument();
  });

  it("compresses the range in log mode", () => {
    const { rerender } = renderChart(series);
    const linear = parseFloat(
      within(screen.getAllByTestId("svol-col")[1]!).getByTestId("svol-bar")
        .style.height || "0",
    );
    rerender(
      <SignalHistogram
        series={series}
        order={ORDER}
        colors={COLORS}
        rangeMs={{ fromMs: 0, toMs: 300_000 }}
        stepMs={60_000}
        scale="log"
        unit="lines"
        label="Log volume"
      />,
    );
    const log = parseFloat(
      within(screen.getAllByTestId("svol-col")[1]!).getByTestId("svol-bar")
        .style.height || "0",
    );
    expect(log).toBeGreaterThan(linear);
  });
});

describe("SignalHistogram interaction", () => {
  const series: VolumeSeries[] = [
    {
      key: "info",
      points: [
        [60_000, 525],
        [180_000, 373_329],
      ],
    },
    { key: "error", points: [[60_000, 12]] },
  ];

  it("gives every bucket a full-height hit target", () => {
    renderChart(series, { height: 84 });
    for (const col of screen.getAllByTestId("svol-col")) {
      // The column, not the drawn bar, is what the pointer has to hit.
      expect(col).toHaveStyle({ height: "100%" });
    }
  });

  it("reveals every series and the total on hover", async () => {
    const user = userEvent.setup();
    renderChart(series);
    await user.hover(screen.getAllByTestId("svol-col")[1]!);
    const tip = screen.getByRole("status");
    expect(within(tip).getByText("info")).toBeInTheDocument();
    expect(within(tip).getByText("525 lines")).toBeInTheDocument();
    expect(within(tip).getByText("error")).toBeInTheDocument();
    expect(within(tip).getByText("12 lines")).toBeInTheDocument();
    expect(within(tip).getByText("537 lines")).toBeInTheDocument();
  });

  it("interrogates a minimum-height bar just as easily", async () => {
    const user = userEvent.setup();
    renderChart(series);
    const cols = screen.getAllByTestId("svol-col");
    // 537 against a 373,329 max renders at the 1px floor.
    expect(
      parseFloat(within(cols[1]!).getByTestId("svol-bar").style.height),
    ).toBe(1);
    await user.hover(cols[1]!);
    expect(
      within(screen.getByRole("status")).getByText("537 lines"),
    ).toBeInTheDocument();
  });

  it("hides the tooltip when the pointer leaves", async () => {
    const user = userEvent.setup();
    renderChart(series);
    const col = screen.getAllByTestId("svol-col")[1]!;
    await user.hover(col);
    expect(screen.getByRole("status")).toBeInTheDocument();
    await user.unhover(col);
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("reaches the same detail from the keyboard", async () => {
    const user = userEvent.setup();
    renderChart(series);
    await user.tab();
    expect(screen.getAllByTestId("svol-col")[0]).toHaveFocus();
    await user.tab();
    expect(screen.getAllByTestId("svol-col")[1]).toHaveFocus();
    expect(
      within(screen.getByRole("status")).getByText("537 lines"),
    ).toBeInTheDocument();
  });

  it("names each bucket for assistive technology", () => {
    renderChart(series);
    expect(screen.getAllByTestId("svol-col")[1]).toHaveAccessibleName(
      /537 lines/,
    );
  });
});

describe("SignalHistogram scale control", () => {
  const series: VolumeSeries[] = [{ key: "info", points: [[60_000, 5]] }];

  it("is absent unless the caller can handle a change", () => {
    renderChart(series);
    expect(screen.queryByRole("button", { name: /log scale/i })).toBeNull();
  });

  it("reports the active scale and toggles it", async () => {
    const user = userEvent.setup();
    const onScaleChange = vi.fn();
    renderChart(series, { onScaleChange });
    const toggle = screen.getByRole("button", { name: /log scale/i });
    expect(toggle).toHaveAttribute("aria-pressed", "false");
    await user.click(toggle);
    expect(onScaleChange).toHaveBeenCalledWith("log");
  });

  it("toggles back to linear when already logarithmic", async () => {
    const user = userEvent.setup();
    const onScaleChange = vi.fn();
    renderChart(series, { onScaleChange, scale: "log" });
    const toggle = screen.getByRole("button", { name: /log scale/i });
    expect(toggle).toHaveAttribute("aria-pressed", "true");
    await user.click(toggle);
    expect(onScaleChange).toHaveBeenCalledWith("linear");
  });
});

describe("SignalHistogram y-axis", () => {
  const series: VolumeSeries[] = [
    {
      key: "info",
      points: [
        [60_000, 1],
        [120_000, 373_329],
      ],
    },
  ];

  it("labels the midpoint gridline for the active scale", () => {
    const { rerender } = renderChart(series);
    expect(screen.getByText("187K lines")).toBeInTheDocument();
    rerender(
      <SignalHistogram
        series={series}
        order={ORDER}
        colors={COLORS}
        rangeMs={{ fromMs: 0, toMs: 300_000 }}
        stepMs={60_000}
        scale="log"
        unit="lines"
        label="Log volume"
      />,
    );
    // Halfway up a log plot is ~610, not half the maximum.
    expect(screen.queryByText("187K lines")).toBeNull();
    expect(screen.getByText("610 lines")).toBeInTheDocument();
  });
});

describe("SignalHistogram tooltip placement", () => {
  const series: VolumeSeries[] = [
    {
      key: "info",
      points: [
        [60_000, 5],
        [120_000, 9],
      ],
    },
  ];

  it("follows the pointer", () => {
    renderChart(series);
    const col = screen.getAllByTestId("svol-col")[1]!;
    fireEvent.mouseEnter(col, { clientX: 220, clientY: 40 });
    const tip = screen.getByRole("status");
    // jsdom reports a zero-origin container, so offsets equal the client point.
    expect(tip.style.left).toBe("220px");
    expect(tip.style.top).toBe("40px");

    fireEvent.mouseMove(col, { clientX: 260, clientY: 55 });
    expect(screen.getByRole("status").style.left).toBe("260px");
  });

  it("flips to the left of the pointer past the midline", () => {
    renderChart(series);
    const col = screen.getAllByTestId("svol-col")[1]!;
    // The suite stubs a 1200px-wide container (src/test/setup.ts).
    fireEvent.mouseEnter(col, { clientX: 900, clientY: 30 });
    expect(screen.getByRole("status").style.transform).toContain("-100%");
  });

  it("anchors to the column when focused without a pointer", () => {
    renderChart(series);
    fireEvent.focus(screen.getAllByTestId("svol-col")[1]!);
    // Anchored to the column's horizontal centre, not to a pointer.
    expect(screen.getByRole("status").style.left).toBe("600px");
  });
});

describe("compactCount", () => {
  // Intl's `notation: "compact"` is locale-dependent — in `de` it does not
  // compact at all — so the axis uses an explicit, locale-stable formatter.
  it("compacts deterministically regardless of locale", () => {
    expect(compactCount(0)).toBe("0");
    expect(compactCount(610)).toBe("610");
    expect(compactCount(999)).toBe("999");
    expect(compactCount(1000)).toBe("1K");
    expect(compactCount(1500)).toBe("1.5K");
    expect(compactCount(9949)).toBe("9.9K");
    expect(compactCount(186_665)).toBe("187K");
    expect(compactCount(373_329)).toBe("373K");
    expect(compactCount(1_500_000)).toBe("1.5M");
    expect(compactCount(2_400_000_000)).toBe("2.4B");
  });
});
