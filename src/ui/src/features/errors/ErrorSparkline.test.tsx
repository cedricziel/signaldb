import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ErrorSparkline } from "./ErrorSparkline";
import type { VolumeSeries } from "../explore/SignalHistogram";
import { formatTimeBucket } from "../../lib/vizFormat";

describe("ErrorSparkline", () => {
  it("renders one bar per bucket across the padded range", () => {
    const series: VolumeSeries[] = [{ key: "s0", points: [[60_000, 3]] }];
    render(
      <ErrorSparkline
        series={series}
        rangeMs={{ fromMs: 0, toMs: 180_000 }}
        stepMs={60_000}
      />,
    );
    // 0, 60_000, 120_000, 180_000 -> 4 buckets.
    expect(screen.getAllByTestId("sparkline-bar")).toHaveLength(4);
  });

  it("shows an empty state when there is no data in range", () => {
    render(
      <ErrorSparkline
        series={[]}
        rangeMs={{ fromMs: 0, toMs: 180_000 }}
        stepMs={60_000}
      />,
    );
    expect(screen.getByText(/No occurrences in range/)).toBeInTheDocument();
    expect(screen.queryByTestId("sparkline-bar")).not.toBeInTheDocument();
  });

  describe("point tooltip", () => {
    const series: VolumeSeries[] = [
      {
        key: "s0",
        points: [
          [60_000, 3],
          [120_000, 7],
        ],
      },
    ];
    const renderChart = () =>
      render(
        <ErrorSparkline
          series={series}
          rangeMs={{ fromMs: 0, toMs: 180_000 }}
          stepMs={60_000}
        />,
      );

    it("shows the bucket's time range and occurrence count on hover", () => {
      renderChart();
      const hit = screen.getAllByTestId("sparkline-hit")[2]!;
      fireEvent.pointerMove(hit, { clientX: 150, clientY: 10 });
      const tip = screen.getByRole("tooltip");
      expect(
        within(tip).getByText(formatTimeBucket(120_000, 60_000)),
      ).toBeInTheDocument();
      expect(within(tip).getByTestId("viz-tip-row")).toHaveTextContent(
        "occurrences7",
      );
      expect(hit).toHaveAttribute("aria-describedby", tip.id);
    });

    it("shows nothing over an empty bucket", () => {
      renderChart();
      fireEvent.pointerMove(screen.getAllByTestId("sparkline-hit")[0]!, {
        clientX: 10,
        clientY: 10,
      });
      expect(screen.queryByRole("tooltip")).toBeNull();
    });

    it("focuses populated buckets and shows their tooltip", () => {
      renderChart();
      const hits = screen.getAllByTestId("sparkline-hit");
      expect(hits[0]).not.toHaveAttribute("tabindex");
      expect(hits[1]).toHaveAttribute("tabindex", "0");
      fireEvent.focus(hits[1]!);
      expect(screen.getByRole("tooltip")).toHaveTextContent("occurrences3");
      fireEvent.blur(hits[1]!);
      expect(screen.queryByRole("tooltip")).toBeNull();
    });
  });
});
