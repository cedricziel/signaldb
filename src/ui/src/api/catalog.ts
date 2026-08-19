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
import { ERROR_PATTERN, GROUP_BUDGET, type GroupSort } from "./traceGroups";
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

/**
 * That one signal source saw this entity, and how much of it that source
 * carried.
 *
 * The count never reaches the screen — see `Observed` in `CatalogView` for
 * why a sample count is a fact about our storage rather than about the
 * entity. It is kept because ranking needs *some* measure of how active an
 * entity is (see {@link rankOf}), and because per-source counts are the only
 * honest way to hold that: 5 spans and 3 log lines are 5 spans and 3 log
 * lines, and "8" would describe neither.
 */
export interface EntityObservation {
  source: string;
  count: number;
}

/**
 * Trace-derived measurements for one entity.
 *
 * Absent — not zeroed — when the entity was never observed in traces. Span
 * status and span duration have no counterpart on a log line or a metric
 * point, so there is no honest value to report; `undefined` says "not
 * measurable here" where a zero would read as "measured, and it was zero".
 */
export interface EntityRed {
  /** Records the measurements below are a rate *of*. */
  traces: number;
  errors: number;
  p50Ms: number;
  p95Ms: number;
}

/** One discovered entity: its identity, what each signal saw, and — where
 * traces saw it — how it performed. */
export interface CatalogEntity {
  /** One value per identity dimension; `null` where the record has none. */
  values: (string | null)[];
  /** Per-source observations, ordered as the entity type declares its
   * sources. A source that observed nothing contributes no entry. */
  observations: EntityObservation[];
  /** Most recent observation across every source, epoch nanoseconds. */
  lastNs: string;
  red?: EntityRed;
}

export interface CatalogEntityResult {
  entities: CatalogEntity[];
  /** More entities exist than the budget displays. */
  truncated: boolean;
}

/** Total observations across sources — a ranking key only, never rendered.
 * See {@link EntityObservation}. */
export function rankOf(entity: CatalogEntity): number {
  return entity.observations.reduce((sum, o) => sum + o.count, 0);
}

/** One decoded row, before it is attributed to the source it came from. */
interface SourceRow {
  values: (string | null)[];
  count: number;
  lastNs: string;
  red?: EntityRed;
}

/** One source's decoded rows, positional per `buildEntitySourceDoc`: a traces
 * row carries [dims..., n, errors, p50, p95, last]; every other source carries
 * [dims..., n, last] - it asked for no errors/p50/p95 aggs. */
function decodeSourceRows(
  res: QueryIrResponse,
  dimensionCount: number,
  isTraces: boolean,
): { rows: SourceRow[]; truncated: boolean } {
  const rows = res.rows ?? [];
  const d = dimensionCount;
  const decoded = rows.slice(0, GROUP_BUDGET).map((row): SourceRow => {
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
      lastNs: lastCell == null ? "0" : String(lastCell),
      red: isTraces
        ? {
            traces: count,
            errors: num(1),
            p50Ms: num(2) / NANOS_PER_MS,
            p95Ms: num(3) / NANOS_PER_MS,
          }
        : undefined,
    };
  });
  return { rows: decoded, truncated: rows.length > GROUP_BUDGET };
}

/** `sortRows`'s per-cell accessor. `"last"` sorts as a bigint so an epoch-
 * nanosecond timestamp never loses precision to `Number`'s 2^53 ceiling,
 * matching every other bigint-timestamp compare in this codebase. An entity
 * with no trace measurement sorts as 0 on the RED columns - it has no value
 * to rank by, and 0 keeps it out of the way of entities that do. */
function entitySortValue(e: CatalogEntity, key: string): SortValue {
  if (key === "last") return BigInt(e.lastNs);
  if (key === "errors") return e.red?.errors ?? 0;
  if (key === "p50") return e.red?.p50Ms ?? 0;
  if (key === "p95") return e.red?.p95Ms ?? 0;
  return rankOf(e);
}

export async function fetchCatalogEntities(
  entityType: EntityTypeDef,
  range: ResolvedRange,
  sort: GroupSort = { key: "n", dir: "desc" },
  pinned: EntityPin[] = [],
): Promise<CatalogEntityResult> {
  const sources = entityType.sources ?? ["traces"];
  const dimensionCount = entityType.identity.length;

  const perSource = await Promise.all(
    sources.map(async (source) => {
      const res = await runIrQuery(
        buildEntitySourceDoc(entityType, source, range, pinned),
      );
      const decoded = decodeSourceRows(
        res,
        dimensionCount,
        source === "traces",
      );
      return { ...decoded, source };
    }),
  );

  // Merge by identity. Each source contributes its own observation entry;
  // `red` only ever comes from the traces source, so a non-trace source can
  // never clobber a real trace measurement for the same identity, nor
  // manufacture one for an identity traces never saw.
  const merged = new Map<string, CatalogEntity>();
  for (const { source, rows } of perSource) {
    for (const row of rows) {
      const key = compositeKey(row.values);
      const observation = { source, count: row.count };
      const existing = merged.get(key);
      if (!existing) {
        merged.set(key, {
          values: row.values,
          observations: [observation],
          lastNs: row.lastNs,
          red: row.red,
        });
        continue;
      }
      existing.observations.push(observation);
      if (row.red) existing.red = row.red;
      if (BigInt(row.lastNs) > BigInt(existing.lastNs)) {
        existing.lastNs = row.lastNs;
      }
    }
  }

  const all = sortRows([...merged.values()], sort, entitySortValue)
    // A record with no value for the primary identity attribute still groups
    // into its own "(not set)" row - meaningful in the traces tab's grouping,
    // where it means "here's the slice with no value for this field", but not
    // here: it isn't a discovered entity, it's the absence of one, so showing
    // it as a catalog row would misrepresent a data gap as a real host/db/etc.
    .filter((e) => e.values[0] !== null);

  return {
    entities: all.slice(0, GROUP_BUDGET),
    truncated: perSource.some((p) => p.truncated) || all.length > GROUP_BUDGET,
  };
}
