// Classification of label cardinality for the builder's pickers. Grouping by a
// high-cardinality label produces a series per value, which is slow and
// unreadable — the builder surfaces a warning before that happens.

import type { DiscoveredField } from "../../api/gen";

/**
 * Distinct-value count above which grouping is flagged. Chosen well below the
 * analyzer's cap (10k) so genuinely explosive labels (pods, trace ids, user
 * ids) warn, while normal dimensions (service, route, status) do not.
 */
export const HIGH_CARDINALITY_THRESHOLD = 1000;

/** Index `discovery.fields` results by name for O(1) lookup from a label. */
export function indexFields(
  fields: DiscoveredField[],
): Map<string, DiscoveredField> {
  return new Map(fields.map((f) => [f.name, f]));
}

/** A capped estimate, or one over the threshold, is "high cardinality". */
export function isHighCardinality(field: DiscoveredField | undefined): boolean {
  const estimate = field?.cardinality;
  if (!estimate) return false;
  return estimate.at_least || estimate.estimate >= HIGH_CARDINALITY_THRESHOLD;
}

/**
 * Short human count for a picker option, e.g. "≈240 values" or "≥10000 values"
 * when the collector hit its cap. Returns null when cardinality is unknown.
 */
export function cardinalityLabel(
  field: DiscoveredField | undefined,
): string | null {
  const estimate = field?.cardinality;
  if (!estimate) return null;
  const prefix = estimate.at_least ? "≥" : "≈";
  return `${prefix}${estimate.estimate} values`;
}

/** A `<datalist>` option label: the count plus a warning marker when risky. */
export function optionLabel(
  field: DiscoveredField | undefined,
): string | undefined {
  const count = cardinalityLabel(field);
  if (count === null) return undefined;
  return isHighCardinality(field) ? `${count} ⚠` : count;
}
