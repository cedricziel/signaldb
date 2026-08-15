// Client-side index over a Weaver-format registry document: the flat lists
// of attribute keys, entity names, and metric names the browser searches,
// plus a per-name fingerprint the editor diffs against the stored document.
// The server owns resolution (refs, extends, precedence); this only reads
// what the document literally declares.
import type { RegistryDocument } from "./api";

type Group = {
  id?: string;
  type?: string;
  brief?: string;
  display_name?: string;
  name?: string;
  metric_name?: string;
  attributes?: Array<Record<string, unknown> & { id?: string; ref?: string }>;
  [key: string]: unknown;
};

export interface IndexedAttribute {
  key: string;
  brief: string;
  groupId: string;
  groupTitle: string;
}

export interface IndexedNamed {
  name: string;
  brief: string;
  groupId: string;
}

export interface RegistryIndex {
  attributes: IndexedAttribute[];
  entities: IndexedNamed[];
  metrics: IndexedNamed[];
  /** Stable JSON fingerprint per `kind/name`, for diffing two documents. */
  fingerprints: Map<string, string>;
}

function groups(document: RegistryDocument): Group[] {
  const raw = document.groups;
  return Array.isArray(raw) ? (raw as Group[]) : [];
}

/** `k8s.pod` → "K8s Pod": fallback title when a group has no display name. */
export function humanizeGroupId(id: string): string {
  const tail = id.replace(/^registry\./, "");
  return tail
    .split(".")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function stable(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).sort(
      ([a], [b]) => a.localeCompare(b),
    );
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stable(v)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function indexRegistry(document: RegistryDocument): RegistryIndex {
  const attributes: IndexedAttribute[] = [];
  const entities: IndexedNamed[] = [];
  const metrics: IndexedNamed[] = [];
  const fingerprints = new Map<string, string>();

  for (const group of groups(document)) {
    const groupId = group.id ?? "";
    const groupTitle = group.display_name || humanizeGroupId(groupId);
    const kind = group.type;
    if (kind === "entity" && group.name) {
      entities.push({ name: group.name, brief: group.brief ?? "", groupId });
      fingerprints.set(`entities/${group.name}`, stable(group));
    } else if (kind === "metric" && group.metric_name) {
      metrics.push({
        name: group.metric_name,
        brief: group.brief ?? "",
        groupId,
      });
      fingerprints.set(`metrics/${group.metric_name}`, stable(group));
    }
    if (kind === "entity") continue;
    for (const attr of group.attributes ?? []) {
      if (typeof attr.id !== "string" || attr.id === "") continue;
      attributes.push({
        key: attr.id,
        brief: typeof attr.brief === "string" ? attr.brief : "",
        groupId,
        groupTitle,
      });
      fingerprints.set(`attributes/${attr.id}`, stable(attr));
    }
  }

  attributes.sort((a, b) => a.key.localeCompare(b.key));
  entities.sort((a, b) => a.name.localeCompare(b.name));
  metrics.sort((a, b) => a.name.localeCompare(b.name));
  return { attributes, entities, metrics, fingerprints };
}

/** Case-insensitive substring match on the name (prefix matches first). */
export function filterByName<T>(
  items: T[],
  name: (item: T) => string,
  query: string,
): T[] {
  const q = query.trim().toLowerCase();
  if (!q) return items;
  const prefix: T[] = [];
  const rest: T[] = [];
  for (const item of items) {
    const n = name(item).toLowerCase();
    if (n.startsWith(q)) prefix.push(item);
    else if (n.includes(q)) rest.push(item);
  }
  return [...prefix, ...rest];
}

export interface DiffSummary {
  added: string[];
  changed: string[];
  removed: string[];
}

/** Compare two indexed documents by `kind/name` fingerprints. */
export function diffRegistries(
  before: RegistryIndex,
  after: RegistryIndex,
): DiffSummary {
  const added: string[] = [];
  const changed: string[] = [];
  const removed: string[] = [];
  for (const [key, fp] of after.fingerprints) {
    const prev = before.fingerprints.get(key);
    if (prev === undefined) added.push(key);
    else if (prev !== fp) changed.push(key);
  }
  for (const key of before.fingerprints.keys()) {
    if (!after.fingerprints.has(key)) removed.push(key);
  }
  return {
    added: added.sort(),
    changed: changed.sort(),
    removed: removed.sort(),
  };
}
