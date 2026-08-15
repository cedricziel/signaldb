import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { TraceVolumeHeatmap } from "./TraceVolumeHeatmap";
import { formatTimeBucket } from "../../lib/vizFormat";

describe("TraceVolumeHeatmap", () => {
  const props = {
    heatmap: {
      window: { start_ns: 0, end_ns: 120_000_000_000 },
      x: { step_ns: 60_000_000_000, align: "epoch" },
      y: {
        of: "duration",
        type: "duration_ns",
        bounds: [10_000_000, 100_000_000],
        overflow: true,
      },
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
    expect(
      screen.getByText(/intensity represents span count/i),
    ).toBeInTheDocument();
  });

  it("renders sparse cells as empty", () => {
    render(<TraceVolumeHeatmap {...props} />);

    const missing = screen.getByLabelText(/10 ms.*100 ms.*no spans/i);
    expect(missing).toHaveAttribute("data-count", "0");
    expect(missing).toHaveAttribute("fill-opacity", "0");
  });

  it("matches epoch-nanosecond cells by bucket index", () => {
    const epoch = 1_700_000_000_000_000_000;
    render(
      <TraceVolumeHeatmap
        {...props}
        heatmap={{
          ...props.heatmap,
          window: { start_ns: epoch + 1, end_ns: epoch + 120_000_000_001 },
          cells: [{ time_bucket_ns: epoch, duration_bucket: 0, count: 2 }],
        }}
      />,
    );

    expect(screen.getByLabelText(/2 spans/i)).toHaveAttribute(
      "data-count",
      "2",
    );
  });

  describe("cell tooltip", () => {
    it("shows the time bucket, latency range, count, and share of the column", () => {
      render(<TraceVolumeHeatmap {...props} />);
      const cell = screen.getByLabelText(/0.*10 ms.*2 spans/i);
      fireEvent.pointerMove(cell, { clientX: 100, clientY: 20 });
      const tip = screen.getByRole("tooltip");
      expect(
        within(tip).getByText(formatTimeBucket(0, 60_000)),
      ).toBeInTheDocument();
      const rows = within(tip).getAllByTestId("viz-tip-row");
      expect(rows.map((r) => r.textContent)).toEqual([
        "latency0 µs – 10 ms",
        "spans2",
        // Two of the column's three spans.
        "share of column66.7%",
      ]);
      expect(cell).toHaveAttribute("aria-describedby", tip.id);
    });

    it("labels the overflow row with an open range", () => {
      render(<TraceVolumeHeatmap {...props} />);
      fireEvent.pointerMove(screen.getByLabelText(/4 spans/i), {
        clientX: 400,
        clientY: 200,
      });
      expect(screen.getByRole("tooltip")).toHaveTextContent("100 ms+");
    });

    it("shows nothing over an empty cell", () => {
      render(<TraceVolumeHeatmap {...props} />);
      fireEvent.pointerMove(screen.getByLabelText(/10 ms.*100 ms.*no spans/i), {
        clientX: 100,
        clientY: 100,
      });
      expect(screen.queryByRole("tooltip")).toBeNull();
    });

    it("focuses only populated cells and shows their tooltip", () => {
      render(<TraceVolumeHeatmap {...props} />);
      const populated = screen.getByLabelText(/2 spans/i);
      expect(populated).toHaveAttribute("tabindex", "0");
      expect(
        screen.getByLabelText(/10 ms.*100 ms.*no spans/i),
      ).not.toHaveAttribute("tabindex");
      fireEvent.focus(populated);
      expect(screen.getByRole("tooltip")).toHaveTextContent("66.7%");
      fireEvent.blur(populated);
      expect(screen.queryByRole("tooltip")).toBeNull();
    });
  });
});
