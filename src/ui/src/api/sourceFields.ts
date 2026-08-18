/**
 * Which logical fields each signal source carries, from maintained metadata.
 *
 * This is the catalog's detection step (`features/catalog/deriveEntityTypes`):
 * an entity type exists in a source when that source carries its primary
 * identity attribute. Answering it from metadata rather than by grouping the
 * data matters for cost — one call per source, whatever the entity-type
 * count, instead of one aggregate per entity type per source. The response's
 * `cost` reports `mode: "metadata"`, confirming no signal data was read.
 *
 * `describe` requires irVersion 4 and the `metadata` result envelope; the
 * rest of the catalog's queries are irVersion 1, which is why the version is
 * written here rather than shared.
 */
import type { QueryIrRequest } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

/** Every source the catalog can discover an entity from. Kept here rather
 * than derived from a query so a source that is registered but empty still
 * gets asked — an empty answer is information, an unasked source is not. */
export const CATALOG_SOURCES = [
  "traces",
  "logs",
  "metrics",
  "metrics_histogram",
  "profiles",
];

/** How stale the metadata behind a detection answer is. `undefined` when the
 * source reported none — see `SourceFields.analyzed`. */
export interface SourceFields {
  fields: Set<string>;
  /** The `as_of` stamp the source reported, if any. */
  asOf?: string;
  /**
   * Whether any maintained metadata covered this source at all.
   *
   * A source that has never been analyzed reports no fields, which is not
   * the same as a source known to carry none: the first renders "not
   * analyzed yet", the second renders as genuinely absent. Collapsing them
   * would report a freshly-ingesting deployment as having no entities.
   */
  analyzed: boolean;
}

export function buildSourceFieldsDoc(
  source: string,
  range: ResolvedRange,
  limit = 500,
): QueryIrRequest {
  return {
    irVersion: 4,
    from: source,
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "metadata",
    pipeline: [{ describe: { target: "fields", limit } }],
  };
}

interface FieldsMetadata {
  metadata?: {
    fields?: { name?: string }[];
    cost?: { as_of?: string };
  };
}

/** One source's field set. A source that fails to answer is reported as
 * unanalyzed rather than as empty: a 500 from one signal must not silently
 * delete every entity type that only that signal carries. */
export async function fetchSourceFields(
  source: string,
  range: ResolvedRange,
): Promise<SourceFields> {
  try {
    const res = (await runIrQuery(
      buildSourceFieldsDoc(source, range),
    )) as FieldsMetadata;
    const fields = res.metadata?.fields ?? [];
    return {
      fields: new Set(
        fields.map((f) => f.name).filter((n): n is string => n !== undefined),
      ),
      asOf: res.metadata?.cost?.as_of,
      analyzed: fields.length > 0,
    };
  } catch {
    return { fields: new Set(), analyzed: false };
  }
}

/** Every catalog source's field set, fetched concurrently. */
export async function fetchAllSourceFields(
  range: ResolvedRange,
  sources: string[] = CATALOG_SOURCES,
): Promise<Map<string, SourceFields>> {
  const results = await Promise.all(
    sources.map(async (source) => [
      source,
      await fetchSourceFields(source, range),
    ]),
  );
  return new Map(results as [string, SourceFields][]);
}
