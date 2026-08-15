/**
 * Pure helpers over schema-registry attribute resolutions: what the UI shows
 * next to a raw attribute key once the registry has answered. Rendering sites
 * never depend on the wire shape directly — they consume `AttributeSemantics`.
 */
import type {
  AttributeHit,
  AttributeResolution,
  DeprecatedInfo,
} from "../api/gen";

/** What the UI knows about one attribute key, derived from a resolution. */
export interface AttributeSemantics {
  key: string;
  /** Winning definition (tenant custom → signaldb → otel). */
  primary: AttributeHit;
  /** Further definitions of the same key, in precedence order. */
  alternatives: AttributeHit[];
  /** Semantic title: the group's display name or a humanized prefix. */
  title: string;
  /** Set when any hit deprecates the key; hits are never dropped, so a tenant
   * re-describing a deprecated key does not hide the upstream deprecation. */
  deprecated: DeprecatedInfo | null;
}

/** Map of key → semantics for the keys the registry knows. */
export type SemanticsMap = ReadonlyMap<string, AttributeSemantics>;

const wordCase = (word: string): string =>
  word.length === 0 ? word : word[0]!.toUpperCase() + word.slice(1);

/**
 * `k8s.pod.uid` → "K8s Pod": the namespace prefix as words. A key with no
 * prefix humanizes wholesale (`name` → "Name").
 */
export function humanizeNamespace(key: string): string {
  const parts = key.split(".");
  const prefix = parts.length > 1 ? parts.slice(0, -1) : parts;
  return prefix
    .flatMap((p) => p.split("_"))
    .filter((w) => w.length > 0)
    .map(wordCase)
    .join(" ");
}

export function semanticTitle(hit: AttributeHit): string {
  return hit.group_display_name || humanizeNamespace(hit.key);
}

export function semanticsFromResolution(
  res: AttributeResolution,
): AttributeSemantics | undefined {
  const primary = res.primary ?? res.hits[0];
  if (!primary) return undefined;
  const alternatives = res.hits.filter((h) => h !== primary);
  const deprecated =
    primary.deprecated ??
    alternatives.find((h) => h.deprecated)?.deprecated ??
    null;
  return {
    key: res.key,
    primary,
    alternatives,
    title: semanticTitle(primary),
    deprecated,
  };
}

export interface TitledGroup<V> {
  /** `null` when nothing in the list resolved: render exactly as before. */
  title: string | null;
  entries: [string, V][];
}

/** Label of the trailing group holding keys the registry does not know. */
export const OTHER_TITLE = "Other";

/**
 * Partition attribute rows by semantic title, keeping the input order inside
 * each group. Titles appear in first-seen order; unknown keys close the list
 * under "Other". When no key resolved the rows come back as one untitled
 * group so callers render them unchanged.
 */
export function groupBySemanticTitle<V>(
  entries: [string, V][],
  semantics: SemanticsMap,
): TitledGroup<V>[] {
  const titled = new Map<string, [string, V][]>();
  const other: [string, V][] = [];
  for (const entry of entries) {
    const sem = semantics.get(entry[0]);
    if (!sem) {
      other.push(entry);
      continue;
    }
    const bucket = titled.get(sem.title);
    if (bucket) bucket.push(entry);
    else titled.set(sem.title, [entry]);
  }
  if (titled.size === 0) return [{ title: null, entries }];
  const groups: TitledGroup<V>[] = [...titled].map(([title, list]) => ({
    title,
    entries: list,
  }));
  if (other.length > 0) groups.push({ title: OTHER_TITLE, entries: other });
  return groups;
}
