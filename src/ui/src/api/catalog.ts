/**
 * RED metrics per discovered entity for the Catalog tab.
 *
 * Structurally identical to `buildGroupDoc` in `./traceGroups` — same Query
 * IR aggregate shape (count/errors/p50/p95/last-seen), same server-side
 * `table` result, same decoding. The only difference is the scope clause:
 * an entity type with `spanKindScope` restricts to that span kind (see
 * `entityTypes.ts` for why only "service" needs this); every other entity
 * type is naturally scoped by its identity attribute's own presence.
 */
import type { QueryIrRequest } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import {
  ERROR_PATTERN,
  GROUP_BUDGET,
  groupsFromIrResponse,
  type GroupSort,
  type TraceGroupResult,
} from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

/** A raw field/value equality pin — an already-known identity value (from a
 * previously-fetched row), not user-typed input, so it bypasses
 * `FACET_FIELDS`/`TraceFilter` entirely; that mechanism exists for
 * compiling user-facing filters (TraceQL search, URL round-tripping), a
 * different concern from pinning a query to one specific entity. */
export interface EntityPin {
  field: string;
  value: string;
}

export function buildEntityDoc(
  entityType: EntityTypeDef,
  range: ResolvedRange,
  sort: GroupSort = { key: "n", dir: "desc" },
  pinned: EntityPin[] = [],
): QueryIrRequest {
  const scope: Record<string, unknown>[] = [
    ...(entityType.spanKindScope
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
    from: "traces",
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
            { fn: "max", of: "start_time_unix_nano", as: "last" },
          ],
        },
      },
      { order: [{ of: sort.key, dir: sort.dir }] },
      { limit: GROUP_BUDGET + 1 },
    ],
  };
}

export async function fetchCatalogEntities(
  entityType: EntityTypeDef,
  range: ResolvedRange,
  sort?: GroupSort,
  pinned?: EntityPin[],
): Promise<TraceGroupResult> {
  const result = groupsFromIrResponse(
    await runIrQuery(buildEntityDoc(entityType, range, sort, pinned)),
    entityType.identity.length,
  );
  // A span with no value for the primary identity attribute still groups
  // into its own "(not set)" row — meaningful in the traces tab's grouping,
  // where it means "here's the slice with no value for this field", but not
  // here: it isn't a discovered entity, it's the absence of one, so showing
  // it as a catalog row would misrepresent a data gap as a real host/db/etc.
  return {
    ...result,
    groups: result.groups.filter((g) => g.values[0] !== null),
  };
}
