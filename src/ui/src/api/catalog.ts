/**
 * RED metrics per discovered entity for the Catalog tab.
 *
 * An entity type's identity may be discoverable from more than one signal
 * (`entityType.sources` - see `entityTypes.ts`): `service.name` is a
 * resource attribute on logs and metrics as much as on spans. Each source
 * is queried independently, with the same Query IR aggregate shape
 * `buildGroupDoc` in `./traceGroups` uses (count/last-seen, plus
 * errors/p50/p95 when the source is `traces` - the only source with span
 * status and duration), and the per-source results are merged by identity.
 * The scope clause is per source too: an entity type with `spanKindScope`
 * restricts *its trace query* to that span kind (see `entityTypes.ts` for
 * why only "service" needs this) - logs/metrics/profiles have no span kind
 * to scope by, so they go unscoped, naturally limited to whichever records
 * carry the identity attribute at all.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { sortRows, type SortValue } from "../lib/sortTable";
import { compositeKey } from "../lib/traceGroups";
import {
  ERROR_PATTERN,
  GROUP_BUDGET,
  type GroupSort,
  type TraceGroup,
  type TraceGroupResult,
} from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

/** A raw field/value equality pin - an already-known identity value (from a
 * previously-fetched row), not user-typed input, so it bypasses
 * `FACET_FIELDS`/`TraceFilter` entirely; that mechanism exists for
 * compiling user-facing filters (TraceQL search, URL round-tripping), a
 * different concern from pinning a query to one specific entity. */
export interface EntityPin {
  field: string;
  value: string;
}

const NANOS_PER_MS = 1_000_000;

/** The time column a source's rows carry - `traces` rows key off the span's
 * start, every other source off its own event timestamp. */
function timeField(source: string): string {
  return source === "traces" ? "start_time_unix_nano" : "timestamp";
}

/**
 * Builds the aggregate for one entity type against one of its sources.
 * Ordered by count, descending, regardless of the table's displayed sort:
 * the display sort is resolved after merging every source's rows (a
 * per-source order can't predict the merged rank), so this order exists
 * only to bias which rows survive the per-source budget below toward the
 * ones most likely to matter.
 */
export function buildEntitySourceDoc(
  entityType: EntityTypeDef,
  source: string,
  range: ResolvedRange,
  pinned: EntityPin[] = [],
): QueryIrRequest {
  const isTraces = source === "traces";
  const scope: Record<string, unknown>[] = [
    ...(isTraces && entityType.spanKindScope
      ? [
          {
            where: {
              field: "span_kind",
              op: "eq",
              value: entityType.spanKindScope,
            },
          },
        ]
      : []),
    ...pinned.map((p) => ({
      where: { field: p.field, op: "eq", value: p.value },
    })),
  ];

  return {
    irVersion: 1,
    from: source,
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "table",
    pipeline: [
      ...scope,
      {
        aggregate: {
          by: entityType.identity,
          aggs: [
            { fn: "count", as: "n" },
            ...(isTraces
              ? [
                  {
                    fn: "count",
                    as: "errors",
                    where: {
                      field: "status.code",
                      op: "regex",
                      value: ERROR_PATTERN,
                    },
                  },
                  { fn: "quantile", of: "duration", arg: 0.5, as: "p50" },
                  { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
                ]
              : []),
            { fn: "max", of: timeField(source), as: "last" },
          ],
        },
      },
      { order: [{ of: "n", dir: "desc" }] },
      { limit: GROUP_BUDGET + 1 },
    ],
  };
}

/** One source's decoded rows, positional per `buildEntitySourceDoc`: a
 * traces row carries [dims..., n, errors, p50, p95, last]; every other
 * source carries [dims..., n, last] - it asked for no errors/p50/p95 aggs. */
function decodeSourceGroups(
  res: QueryIrResponse,
  dimensionCount: number,
  isTraces: boolean,
): { groups: TraceGroup[]; truncated: boolean } {
  const rows = res.rows ?? [];
  const d = dimensionCount;
  const groups = rows.slice(0, GROUP_BUDGET).map((row): TraceGroup => {
    const cells = row as unknown[];
    const values = cells.slice(0, d).map((v) => (v == null ? null : String(v)));
    const num = (i: number) => {
      const v = cells[d + i];
      return typeof v === "number" ? v : 0;
    };
    const lastCell = cells[d + (isTraces ? 4 : 1)];
    const count = num(0);
    return {
      values,
      count,
      errors: isTraces ? num(1) : 0,
      p50Ms: isTraces ? num(2) / NANOS_PER_MS : 0,
      p95Ms: isTraces ? num(3) / NANOS_PER_MS : 0,
      lastNs: lastCell == null ? "0" : String(lastCell),
      traceCount: isTraces ? count : 0,
    };
  });
  return { groups, truncated: rows.length > GROUP_BUDGET };
}

/** `sortRows`'s per-cell accessor for a merged catalog group: `"last"` sorts
 * as a bigint so an epoch-nanosecond timestamp never loses precision to
 * `Number`'s 2^53 ceiling, matching every other bigint-timestamp compare in
 * this codebase. */
function groupSortValue(g: TraceGroup, key: string): SortValue {
  if (key === "last") return BigInt(g.lastNs);
  if (key === "errors") return g.errors;
  if (key === "p50") return g.p50Ms;
  if (key === "p95") return g.p95Ms;
  return g.count;
}

export async function fetchCatalogEntities(
  entityType: EntityTypeDef,
  range: ResolvedRange,
  sort: GroupSort = { key: "n", dir: "desc" },
  pinned: EntityPin[] = [],
): Promise<TraceGroupResult> {
  const sources = entityType.sources ?? ["traces"];
  const dimensionCount = entityType.identity.length;

  const perSource = await Promise.all(
    sources.map(async (source) => {
      const isTraces = source === "traces";
      const res = await runIrQuery(
        buildEntitySourceDoc(entityType, source, range, pinned),
      );
      return {
        isTraces,
        ...decodeSourceGroups(res, dimensionCount, isTraces),
      };
    }),
  );

  // Merge by identity: count sums across every source (it's total observed
  // volume), but errors/p50/p95 only ever come from the traces source - a
  // non-trace source's rows contribute 0 to traceCount and must not clobber
  // a real trace measurement for the same identity. traceCount itself sums
  // like count (each source contributes its own truthful share, 0 for a
  // non-trace source) - it's the denominator an error rate must use instead
  // of count, not a last-write-wins flag.
  const merged = new Map<string, TraceGroup>();
  for (const { isTraces, groups } of perSource) {
    for (const g of groups) {
      const key = compositeKey(g.values);
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, { ...g });
        continue;
      }
      existing.count += g.count;
      existing.traceCount = (existing.traceCount ?? 0) + (g.traceCount ?? 0);
      if (isTraces) {
        existing.errors = g.errors;
        existing.p50Ms = g.p50Ms;
        existing.p95Ms = g.p95Ms;
      }
      if (BigInt(g.lastNs) > BigInt(existing.lastNs)) {
        existing.lastNs = g.lastNs;
      }
    }
  }

  const allGroups = sortRows([...merged.values()], sort, groupSortValue)
    // A span/log/metric/profile with no value for the primary identity
    // attribute still groups into its own "(not set)" row - meaningful in
    // the traces tab's grouping, where it means "here's the slice with no
    // value for this field", but not here: it isn't a discovered entity,
    // it's the absence of one, so showing it as a catalog row would
    // misrepresent a data gap as a real host/db/etc.
    .filter((g) => g.values[0] !== null);

  return {
    groups: allGroups.slice(0, GROUP_BUDGET),
    truncated:
      perSource.some((p) => p.truncated) || allGroups.length > GROUP_BUDGET,
  };
}
