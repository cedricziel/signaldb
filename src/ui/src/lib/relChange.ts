// Relative change between a current and previous figure — shared by every
// KPI card that compares a window against the one before it (the System
// Overview's request/error/latency cards, the Real users KPI strip). Moved
// out of `features/overview/overviewModel.ts` (where it started private) so
// `features/rum` could reuse it rather than keep its own near-identical copy.

export type RelChangeTone = "good" | "bad" | "neutral";

export interface RelChange {
  text: string;
  tone: RelChangeTone;
  /** Which way the figure moved — `flat` within rounding of zero. Not read
   * by the Overview page's own card (it only shows `text`/`tone`), but a
   * `KpiCard`-style caller (e.g. Real users) needs it for the change
   * indicator's direction. */
  direction: "up" | "down" | "flat";
}

/** Relative change as a rounded percentage; `upIsBad` picks which direction
 * reads red (an error rate rising) vs green (throughput rising). `undefined`
 * with no previous figure to compare against — a `previous` of exactly `0`
 * is "nothing to compare against" (an empty prior window), not "infinite
 * increase". */
export function relChange(
  current: number,
  previous: number,
  upIsBad: boolean,
): RelChange | undefined {
  if (previous === 0) return undefined;
  const pct = Math.round(((current - previous) / previous) * 100);
  if (pct === 0) return { text: "±0%", tone: "neutral", direction: "flat" };
  const up = pct > 0;
  return {
    text: `${up ? "+" : "−"}${Math.abs(pct)}%`,
    tone: up === upIsBad ? "bad" : "good",
    direction: up ? "up" : "down",
  };
}
