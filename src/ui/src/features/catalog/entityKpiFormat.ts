/**
 * "vs previous period" change figures for the entity detail page's KPI
 * cards. Rounds to whole units before deciding direction, so a change that
 * rounds to zero reads as "flat" rather than a wrong-signed "+0%"/"-0%".
 */

export interface KpiChangeFigure {
  text: string;
  direction: "up" | "down" | "flat";
}

/** A relative percent change (rate, duration). A zero previous period has no
 * ratio to report — "new vs prev" rather than a misleading "+Infinity%". */
export function pctChange(current: number, previous: number): KpiChangeFigure {
  if (previous === 0) {
    return current === 0
      ? { text: "flat vs prev", direction: "flat" }
      : { text: "new vs prev", direction: "up" };
  }
  const pct = Math.round(((current - previous) / previous) * 100);
  if (pct === 0) return { text: "flat vs prev", direction: "flat" };
  return {
    text: `${pct > 0 ? "+" : ""}${pct}% vs prev`,
    direction: pct > 0 ? "up" : "down",
  };
}

/** An absolute percentage-point change (an error rate already expressed as
 * a 0-100 percent), for when a relative ratio would exaggerate small moves. */
export function ppChange(
  currentPercent: number,
  previousPercent: number,
): KpiChangeFigure {
  const pp = Math.round(currentPercent - previousPercent);
  if (pp === 0) return { text: "flat vs prev", direction: "flat" };
  return {
    text: `${pp > 0 ? "+" : ""}${pp}pp vs prev`,
    direction: pp > 0 ? "up" : "down",
  };
}

/** An error-rate fraction (0-1) as a whole-percent string. Unlike
 * `formatErrorRate` in `lib/vizFormat.ts` (which reads a dash for "no
 * measurement at all"), the caller already has a measured current period
 * here, so a clean rate renders `0%`, not a dash. */
export function errorRatePercent(rate: number): string {
  if (rate === 0) return "0%";
  if (rate < 0.005) return "<1%";
  return `${Math.round(rate * 100)}%`;
}
