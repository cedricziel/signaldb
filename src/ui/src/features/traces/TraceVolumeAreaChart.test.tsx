import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TraceVolumeAreaChart } from "./TraceVolumeAreaChart";
import type { VolumeSeries } from "../explore/SignalHistogram";
import { formatTimeBucket } from "../../lib/vizFormat";

const SERIES: VolumeSeries[] = [
  {
    key: "ok",
    points: [
      [60_000, 5],
      [120_000, 9],
    ],
  },
  { key: "error", points: [[120_000, 2]] },
];

function renderChart() {
  return render(
    <TraceVolumeAreaChart
      series={SERIES}
      order={["ok", "unset", "error"]}
      colors={{ ok: "green", unset: "grey", error: "red" }}
      rangeMs={{ fromMs: 0, toMs: 180_000 }}
      stepMs={60_000}
      unit="spans"
      label="Span volume"
    />,
  );
}

describe("TraceVolumeAreaChart tooltip", () => {
  it("has one hit target per bucket across the padded window", () => {
    renderChart();
    expect(screen.getAllByTestId("trace-area-bucket")).toHaveLength(4);
  });

  it("shows the bucket's time range, per-series values, and total on hover", () => {
    renderChart();
    const bucket = screen.getAllByTestId("trace-area-bucket")[2]!;
    fireEvent.pointerMove(bucket, { clientX: 300, clientY: 20 });
    const tip = screen.getByRole("tooltip");
    expect(
      within(tip).getByText(formatTimeBucket(120_000, 60_000)),
    ).toBeInTheDocument();
    const rows = within(tip).getAllByTestId("viz-tip-row");
    expect(rows.map((r) => r.textContent)).toEqual([
      "ok9 spans",
      "error2 spans",
    ]);
    expect(
      within(rows[1]!).getByTestId("viz-tip-swatch").style.background,
    ).toBe("red");
    expect(within(tip).getByTestId("viz-tip-footer")).toHaveTextContent(
      "total11 spans",
    );
    expect(bucket).toHaveAttribute("aria-describedby", tip.id);
  });

  it("shows nothing over an empty bucket", () => {
    renderChart();
    fireEvent.pointerMove(screen.getAllByTestId("trace-area-bucket")[0]!, {
      clientX: 50,
      clientY: 20,
    });
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("hides the tooltip when the pointer leaves", () => {
    renderChart();
    const bucket = screen.getAllByTestId("trace-area-bucket")[1]!;
    fireEvent.pointerMove(bucket, { clientX: 200, clientY: 20 });
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
    fireEvent.pointerLeave(bucket);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("reaches the same detail from the keyboard", () => {
    renderChart();
    const bucket = screen.getAllByTestId("trace-area-bucket")[1]!;
    expect(bucket).toHaveAttribute("tabindex", "0");
    fireEvent.focus(bucket);
    expect(screen.getByRole("tooltip")).toHaveTextContent("5 spans");
    fireEvent.blur(bucket);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});
