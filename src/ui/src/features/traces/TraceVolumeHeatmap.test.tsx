import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { VolumeSeries } from "../explore/SignalHistogram";
import { TraceVolumeHeatmap } from "./TraceVolumeHeatmap";

describe("TraceVolumeHeatmap", () => {
  const props: {
    series: VolumeSeries[];
    latency: VolumeSeries[];
    order: string[];
    colors: Record<string, string>;
    rangeMs: { fromMs: number; toMs: number };
    stepMs: number;
    label: string;
  } = {
    series: [
      { key: "ok", points: [[0, 2], [60_000, 1]] },
      { key: "error", points: [[0, 1]] },
    ],
    latency: [
      { key: "ok", points: [[0, 10], [60_000, 0]] },
      { key: "error", points: [[0, 100]] },
    ],
    order: ["ok", "unset", "error"],
    colors: { ok: "green", unset: "gray", error: "red" },
    rangeMs: { fromMs: 0, toMs: 60_000 },
    stepMs: 60_000,
    label: "Span latency",
  };

  it("encodes average latency in the cell label and intensity", () => {
    render(<TraceVolumeHeatmap {...props} />);

    const error = screen.getByLabelText(/error,.*average latency 100 ms/i);
    expect(error).toHaveAttribute("data-intensity", "1.000");
    expect(screen.getByText(/intensity represents average latency/i)).toBeInTheDocument();
  });

  it("keeps zero-count buckets empty even when their average is zero", () => {
    render(<TraceVolumeHeatmap {...props} />);

    const missing = screen.getByLabelText(/error,.*no data/i);
    expect(missing).toHaveAttribute("data-count", "0");
    expect(missing).toHaveAttribute("fill-opacity", "0");
    const zeroLatency = screen.getByLabelText(/ok,.*average latency 0 µs/i);
    expect(zeroLatency).toHaveAttribute("data-count", "1");
    expect(zeroLatency).toHaveAttribute("fill-opacity", "0.12");
  });
});
