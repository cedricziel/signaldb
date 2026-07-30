// Classification of label cardinality for the builder's pickers. Grouping by a
// high-cardinality label produces a series per value, which is slow and
// unreadable — the builder surfaces a warning before that happens.

import type { LabelStat } from "../../api/prom";

/**
 * Distinct-value count above which grouping is flagged. Chosen well below the
 * analyzer's cap (10k) so genuinely explosive labels (pods, trace ids, user
 * ids) warn, while normal dimensions (service, route, status) do not.
 */
export const HIGH_CARDINALITY_THRESHOLD = 1000;

/** Index label stats by name for O(1) lookup from a list of labels. */
export function indexLabelStats(stats: LabelStat[]): Map<string, LabelStat> {
  return new Map(stats.map((s) => [s.name, s]));
}

/** A capped estimate, or one over the threshold, is "high cardinality". */
export function isHighCardinality(stat: LabelStat | undefined): boolean {
  if (!stat) return false;
  return stat.capped || stat.distinct_estimate >= HIGH_CARDINALITY_THRESHOLD;
}

/**
 * Short human count for a picker option, e.g. "≈240 values" or "≥10000 values"
 * when capped. Returns null when cardinality is unknown (no stat).
 */
export function cardinalityLabel(stat: LabelStat | undefined): string | null {
  if (!stat) return null;
  const prefix = stat.capped ? "≥" : "≈";
  return `${prefix}${stat.distinct_estimate} values`;
}

/** A `<datalist>` option label: the count plus a warning marker when risky. */
export function optionLabel(stat: LabelStat | undefined): string | undefined {
  const count = cardinalityLabel(stat);
  if (count === null) return undefined;
  return isHighCardinality(stat) ? `${count} ⚠` : count;
}
