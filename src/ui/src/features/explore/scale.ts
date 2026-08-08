/**
 * Vertical-scale maths for the stacked signal-volume charts.
 *
 * Two rules matter here, and both exist because the previous renderer broke
 * them:
 *
 * 1. The minimum-height floor belongs to the **bar**, not to each stacked
 *    segment. Flooring per segment multiplied the floor by the number of
 *    series present, which made a 525-line bucket and a 10,240-line bucket
 *    render 0.8px apart.
 * 2. A logarithmic scale applies to the bucket **total** only. Segments take
 *    their linear share of the resulting bar, because `log(a) + log(b)` is not
 *    `log(a + b)` — log-transforming each segment would leave the stack no
 *    longer summing to the bar, misstating the composition.
 */

/** Vertical mapping from a bucket total to a fraction of the plot height. */
export type Scale = "linear" | "log";

export const DEFAULT_SCALE: Scale = "linear";

/** Smallest rendered height for a bucket that holds anything at all. */
export const MIN_BAR_PX = 1;

export function isScale(value: string): value is Scale {
  return value === "linear" || value === "log";
}

/**
 * Map a bucket total onto `[0, 1]` against the window maximum. Zero always
 * maps to zero so an empty bucket stays empty in either mode; a non-positive
 * maximum means an empty window and yields zero throughout.
 */
export function scaleFraction(
  value: number,
  max: number,
  scale: Scale,
): number {
  if (max <= 0 || value <= 0) return 0;
  if (scale === "log") return Math.log1p(value) / Math.log1p(max);
  return value / max;
}

/**
 * Inverse of {@link scaleFraction}: the value a gridline at `fraction` of the
 * plot height represents. Axis labels must go through this — on a log plot the
 * halfway gridline is nowhere near half the maximum, so labelling it `max / 2`
 * misstates the axis.
 */
export function valueAtFraction(
  fraction: number,
  max: number,
  scale: Scale,
): number {
  if (max <= 0 || fraction <= 0) return 0;
  if (scale === "log") return Math.expm1(fraction * Math.log1p(max));
  return fraction * max;
}

/**
 * Rendered height in pixels for one bucket. A non-empty bucket never falls
 * below {@link MIN_BAR_PX}, so low-volume buckets stay visible without the
 * floor swamping the values just above it.
 */
export function barHeight(
  total: number,
  max: number,
  available: number,
  scale: Scale,
): number {
  if (total <= 0 || available <= 0) return 0;
  const px = scaleFraction(total, max, scale) * available;
  return Math.min(available, Math.max(MIN_BAR_PX, px));
}

export interface Segment {
  key: string;
  px: number;
}

/**
 * Divide a bar's height across its series by each one's linear share of the
 * bucket total, in the caller's series order. Series absent from the bucket
 * are dropped. The returned heights sum to `barPx`.
 */
export function splitSegments(
  counts: Record<string, number>,
  order: string[],
  barPx: number,
): Segment[] {
  const total = order.reduce((sum, key) => sum + (counts[key] ?? 0), 0);
  if (total <= 0 || barPx <= 0) return [];
  return order
    .filter((key) => (counts[key] ?? 0) > 0)
    .map((key) => ({ key, px: barPx * ((counts[key] ?? 0) / total) }));
}
