import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TraceVolumeHeatmap } from "./TraceVolumeHeatmap";

describe("TraceVolumeHeatmap", () => {
  const props = {
    heatmap: {
      window: { start_ns: 0, end_ns: 120_000_000_000 },
      x: { step_ns: 60_000_000_000, align: "epoch" },
      y: { of: "duration", type: "duration_ns", bounds: [10_000_000, 100_000_000], overflow: true },
      value: "count",
      cells: [
        { time_bucket_ns: 0, duration_bucket: 0, count: 2 },
        { time_bucket_ns: 0, duration_bucket: 1, count: 1 },
        { time_bucket_ns: 60_000_000_000, duration_bucket: 2, count: 4 },
      ],
    },
    label: "Span latency",
  };

  it("uses latency buckets as rows and counts as intensity", () => {
    render(<TraceVolumeHeatmap {...props} />);

    const cell = screen.getByLabelText(/0.*10 ms.*2 spans/i);
    expect(cell).toHaveAttribute("data-intensity", "0.500");
    expect(screen.getByText(/intensity represents span count/i)).toBeInTheDocument();
  });

  it("renders sparse cells as empty", () => {
    render(<TraceVolumeHeatmap {...props} />);

    const missing = screen.getByLabelText(/10 ms.*100 ms.*no spans/i);
    expect(missing).toHaveAttribute("data-count", "0");
    expect(missing).toHaveAttribute("fill-opacity", "0");
  });
});
