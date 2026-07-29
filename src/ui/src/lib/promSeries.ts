// Align Prometheus series onto a shared time axis for uPlot, which wants
// columnar data: [timestamps, series1, series2, …] with null gaps.

import type { PromSeries } from "../api/prom";

export type AlignedData = [number[], ...(number | null)[][]];

export function alignSeries(series: PromSeries[]): AlignedData {
  const timestamps = [
    ...new Set(series.flatMap((s) => s.points.map(([t]) => t))),
  ].sort((a, b) => a - b);
  const columns = series.map((s) => {
    const byTime = new Map(s.points);
    return timestamps.map((t) => byTime.get(t) ?? null);
  });
  return [timestamps, ...columns];
}

/** Cycle through the theme's categorical series colors. */
export const SERIES_COLOR_VARS = [
  "--accent",
  "--info",
  "--svc-b",
  "--svc-c",
  "--svc-e",
  "--svc-d",
] as const;

export function seriesColorVar(index: number): string {
  return `var(${SERIES_COLOR_VARS[index % SERIES_COLOR_VARS.length]})`;
}
