/**
 * Pure grouping logic for the Logs field sidebar: turns a flat label list
 * into the sections `FieldSidebar` renders — a pinned "Line" group for the
 * fields people filter on first, one group per semantic title, a
 * "Deprecated" group, and an "Other" group for labels the registry doesn't
 * know. Falls back to one flat, untitled group when nothing resolved (the
 * registry hasn't answered yet, or is unavailable), so the sidebar renders
 * exactly as it did before semantics existed.
 */
import type { SemanticsMap } from "../../lib/semantics";

export interface FieldGroup {
  id: string;
  title: string;
  labels: string[];
}

/** Order of the pinned "Line" group, when each label is present. */
const LINE_LABELS = [
  "level",
  "detected_level",
  "service_name",
  "service.name",
  "event.name",
];

/** "Kubernetes" → "kubernetes"; non-alphanumerics collapse to a single "-". */
function titleToId(title: string): string {
  return title.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

/** A namespace-derived group id, `t-`-prefixed so it can never collide with
 * the sentinel ids ("line", "deprecated", "other", "all") or with each
 * other — a namespace titled e.g. "Other" would otherwise land on the same
 * id as the trailing unresolved-keys group. */
function namespaceGroupId(title: string): string {
  return `t-${titleToId(title)}`;
}

const byLocale = (a: string, b: string) => a.localeCompare(b);

/**
 * Groups `labels` for the sidebar. See `FieldGroup` for shape; group and
 * ordering rules live in the module doc comment above and the task this
 * implements.
 */
export function groupFields(
  labels: string[],
  semantics: SemanticsMap,
): FieldGroup[] {
  if (semantics.size === 0) return [{ id: "all", title: "", labels }];

  const lineLabels = LINE_LABELS.filter((l) => labels.includes(l));
  const lineSet = new Set(lineLabels);

  const byTitle = new Map<string, string[]>();
  const deprecated: string[] = [];
  const other: string[] = [];

  for (const label of labels) {
    if (lineSet.has(label)) continue;
    const sem = semantics.get(label);
    if (!sem) {
      other.push(label);
      continue;
    }
    if (sem.deprecated) {
      deprecated.push(label);
      continue;
    }
    const bucket = byTitle.get(sem.title);
    if (bucket) bucket.push(label);
    else byTitle.set(sem.title, [label]);
  }

  const groups: FieldGroup[] = [];
  if (lineLabels.length > 0) {
    groups.push({ id: "line", title: "Line", labels: lineLabels });
  }
  groups.push(
    ...[...byTitle.entries()]
      .map(([title, group]) => ({
        id: namespaceGroupId(title),
        title,
        labels: [...group].sort(byLocale),
      }))
      .sort((a, b) => byLocale(a.title, b.title)),
  );
  if (deprecated.length > 0) {
    groups.push({
      id: "deprecated",
      title: "Deprecated",
      labels: deprecated.sort(byLocale),
    });
  }
  if (other.length > 0) {
    groups.push({ id: "other", title: "Other", labels: other.sort(byLocale) });
  }
  return groups;
}
