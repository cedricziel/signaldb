import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ErrorSparkline } from "./ErrorSparkline";
import type { VolumeSeries } from "../explore/SignalHistogram";

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
});
