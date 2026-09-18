/**
 * Filter-key autocomplete: registry prefix hits (with their briefs) merged
 * with the label names observed in the tenant's data, so an observed key the
 * registry does not know remains suggestible — just without a description.
 */
import type { AttributeHit } from "../api/gen";
import { plainBrief } from "./semantics";

export interface LabelSuggestion {
  key: string;
  /** Registry brief, already plain text (see `plainBrief`); `null` for an
   * observed-only key. */
  brief: string | null;
  /** Defining namespace; `null` for an observed-only key. */
  namespace: string | null;
  /** Registry source (`"bundled"`, `"custom"`, ...); `null` for an
   * observed-only key. */
  source: string | null;
  /** Replacement key when the registry deprecates this one in favor of
   * another; `null` when there is none (or the key isn't deprecated). */
  deprecatedTo: string | null;
  /** True when the registry marks this key deprecated. */
  deprecated: boolean;
  /** True when the key was observed in the current data. */
  seen: boolean;
}

/** Longest suggestion list shown under the key input. */
export const LABEL_SUGGESTION_LIMIT = 12;

/** Loki-path labels arrive underscore-flattened (`k8s_pod_uid`); compare
 * both spellings so a dotted prefix still finds them. */
const flat = (s: string) => s.toLowerCase().replace(/\./g, "_");

/**
 * A dotted registry key (`service.name`) as the Loki label spelling
 * (`service_name`) a filter chip actually needs — labels are bare
 * identifiers (see `lib/filters.ts`'s `LABEL_RE`), so a suggestion picked
 * from the registry must be flattened before it lands in the chip form.
 */
export function toLokiLabel(key: string): string {
  return key.replace(/\./g, "_");
}

export function mergeLabelSuggestions(
  prefix: string,
  hits: AttributeHit[],
  observed: string[],
): LabelSuggestion[] {
  const p = prefix.trim();
  if (!p) return [];
  const fp = flat(p);
  const observedSet = new Set(observed);
  const taken = new Set<string>();
  // Non-deprecated hits sort before deprecated ones; each block otherwise
  // keeps the server's precedence order.
  const current: LabelSuggestion[] = [];
  const deprecated: LabelSuggestion[] = [];
  for (const hit of hits) {
    if (taken.has(hit.key)) continue;
    taken.add(hit.key);
    const suggestion: LabelSuggestion = {
      key: hit.key,
      brief: plainBrief(hit.brief),
      namespace: hit.namespace,
      source: hit.source,
      deprecatedTo: hit.deprecated?.renamed_to ?? null,
      deprecated: hit.deprecated != null,
      seen: observedSet.has(hit.key),
    };
    (suggestion.deprecated ? deprecated : current).push(suggestion);
  }
  const out = [...current, ...deprecated];
  for (const label of observed) {
    if (taken.has(label) || !flat(label).startsWith(fp)) continue;
    taken.add(label);
    out.push({
      key: label,
      brief: null,
      namespace: null,
      source: null,
      deprecatedTo: null,
      deprecated: false,
      seen: true,
    });
  }
  return out.slice(0, LABEL_SUGGESTION_LIMIT);
}
