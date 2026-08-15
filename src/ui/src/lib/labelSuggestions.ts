/**
 * Filter-key autocomplete: registry prefix hits (with their briefs) merged
 * with the label names observed in the tenant's data, so an observed key the
 * registry does not know remains suggestible — just without a description.
 */
import type { AttributeHit } from "../api/gen";

export interface LabelSuggestion {
  key: string;
  /** Registry brief; `null` for an observed-only key. */
  brief: string | null;
  /** Defining namespace; `null` for an observed-only key. */
  namespace: string | null;
  /** True when the key was observed in the current data. */
  seen: boolean;
}

/** Longest suggestion list shown under the key input. */
export const LABEL_SUGGESTION_LIMIT = 12;

/** Loki-path labels arrive underscore-flattened (`k8s_pod_uid`); compare
 * both spellings so a dotted prefix still finds them. */
const flat = (s: string) => s.toLowerCase().replace(/\./g, "_");

export function mergeLabelSuggestions(
  prefix: string,
  hits: AttributeHit[],
  observed: string[],
): LabelSuggestion[] {
  const p = prefix.trim();
  if (!p) return [];
  const fp = flat(p);
  const observedSet = new Set(observed);
  const out: LabelSuggestion[] = [];
  const taken = new Set<string>();
  for (const hit of hits) {
    if (taken.has(hit.key)) continue;
    taken.add(hit.key);
    out.push({
      key: hit.key,
      brief: hit.brief,
      namespace: hit.namespace,
      seen: observedSet.has(hit.key),
    });
  }
  for (const label of observed) {
    if (taken.has(label) || !flat(label).startsWith(fp)) continue;
    taken.add(label);
    out.push({ key: label, brief: null, namespace: null, seen: true });
  }
  return out.slice(0, LABEL_SUGGESTION_LIMIT);
}
