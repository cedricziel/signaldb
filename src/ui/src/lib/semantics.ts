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
  /** Plain-text form of `primary.brief` (see `plainBrief`) — rendering sites
   * show this rather than stripping the markdown themselves. */
  brief: string;
  /** Set when any hit deprecates the key; hits are never dropped, so a tenant
   * re-describing a deprecated key does not hide the upstream deprecation.
   * `note`, when present, is already plain text. */
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

/** Trailing suffix trimmed from a group's display name — "Kubernetes
 * Attributes" reads as "Kubernetes" everywhere the title is shown (an
 * attribute table heading, a sidebar group). */
const ATTRIBUTES_SUFFIX = " Attributes";

export function semanticTitle(hit: AttributeHit): string {
  const title = hit.group_display_name || humanizeNamespace(hit.key);
  return title.endsWith(ATTRIBUTES_SUFFIX)
    ? title.slice(0, -ATTRIBUTES_SUFFIX.length)
    : title;
}

/**
 * Registry briefs and deprecation notes carry markdown (`[label](url)`
 * links, `` `code` `` spans); rendering sites here show plain text, so this
 * strips the markup down to readable words rather than showing it verbatim.
 */
export function plainBrief(text: string | null | undefined): string {
  if (!text) return "";
  return text
    .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1")
    .replace(/`+/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function semanticsFromResolution(
  res: AttributeResolution,
): AttributeSemantics | undefined {
  const primary = res.primary ?? res.hits[0];
  if (!primary) return undefined;
  // The server clones the winning hit into `primary` (`hits.first().cloned()`
  // in the schema registry), so after JSON round-tripping `hits[0]` and
  // `primary` are distinct objects with equal fields — compare by identity
  // (namespace + version), not by reference.
  const alternatives = res.hits.filter(
    (h) => h.namespace !== primary.namespace || h.version !== primary.version,
  );
  const deprecatedHit =
    primary.deprecated ??
    alternatives.find((h) => h.deprecated)?.deprecated ??
    null;
  const deprecated = deprecatedHit
    ? { ...deprecatedHit, note: plainBrief(deprecatedHit.note) }
    : null;
  return {
    key: res.key,
    primary,
    alternatives,
    title: semanticTitle(primary),
    brief: plainBrief(primary.brief),
    deprecated,
  };
}

/**
 * `deprecated`'s badge text: `→ <renamed_to>` when the registry named a
 * replacement, else the bare word "deprecated"; `null` when the key isn't
 * deprecated at all, so callers can render nothing rather than an empty
 * badge.
 */
export function deprecationLabel(
  deprecated: DeprecatedInfo | null | undefined,
): string | null {
  if (!deprecated) return null;
  return deprecated.renamed_to ? `→ ${deprecated.renamed_to}` : "deprecated";
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

/**
 * Folds a titled group with exactly one entry into the trailing "Other"
 * group (created if it doesn't already exist) — a heading over a single row
 * reads as clutter, not structure. The "Other" group's entries are then
 * sorted by key (`localeCompare`) so folded rows interleave alphabetically
 * with the genuinely-unknown ones already there, rather than trailing after
 * them in fold order.
 *
 * When folding leaves no titled group besides "Other" standing, there is
 * nothing left worth a heading at all: this returns the single untitled
 * group `{ title: null, entries }` holding the merged, key-sorted "Other"
 * entries — the same shape `groupBySemanticTitle` returns when nothing
 * resolved, so the caller renders one flat, unheaded list.
 */
export function foldSingletonGroups<V>(
  groups: TitledGroup<V>[],
): TitledGroup<V>[] {
  if (groups.length === 0) return [];
  const kept: TitledGroup<V>[] = [];
  let other: [string, V][] | undefined;
  for (const group of groups) {
    if (group.title === OTHER_TITLE) {
      other = [...(other ?? []), ...group.entries];
    } else if (group.title !== null && group.entries.length === 1) {
      other = [...(other ?? []), group.entries[0]!];
    } else {
      kept.push(group);
    }
  }
  if (other) {
    other.sort(([a], [b]) => a.localeCompare(b));
    kept.push({ title: OTHER_TITLE, entries: other });
  }
  return kept.every((g) => g.title === OTHER_TITLE)
    ? [{ title: null, entries: kept.flatMap((g) => g.entries) }]
    : kept;
}
