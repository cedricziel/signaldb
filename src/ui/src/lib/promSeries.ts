// Align Prometheus series onto a shared time axis for uPlot, which wants
// columnar data: [timestamps, series1, series2, …] with null gaps.

import type { PromSeries } from "../api/ir/metrics";

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

/**
 * Cycle through the theme's categorical series colors: `--accent`/`--info`
 * first (already the "this app" and "informational" hues elsewhere), then
 * the full twelve-color `--svc-*` palette. Past 12 series, `seriesDash`
 * below varies the line's dash pattern so two series sharing a color still
 * read as distinct.
 */
export const SERIES_COLOR_VARS = [
  "--accent",
  "--info",
  "--svc-a",
  "--svc-b",
  "--svc-c",
  "--svc-d",
  "--svc-e",
  "--svc-f",
  "--svc-g",
  "--svc-h",
  "--svc-i",
  "--svc-j",
  "--svc-k",
  "--svc-l",
] as const;

export function seriesColorVar(index: number): string {
  return `var(${SERIES_COLOR_VARS[index % SERIES_COLOR_VARS.length]})`;
}

/**
 * uPlot dash pattern (`strokeDasharray`-style segment lengths in px), varied
 * once the color cycle itself repeats — the 15th series (index 14, cycle 1)
 * gets the same color as the 1st but a dashed line instead of solid, so nearby
 * series sharing a color are still distinguishable at a glance. `undefined`
 * (solid) for the first pass through the palette.
 */
export function seriesDash(index: number): number[] | undefined {
  const cycle = Math.floor(index / SERIES_COLOR_VARS.length);
  if (cycle === 0) return undefined;
  const PATTERNS = [
    [6, 4],
    [2, 3],
    [1, 3, 6, 3],
  ];
  return PATTERNS[(cycle - 1) % PATTERNS.length];
}
