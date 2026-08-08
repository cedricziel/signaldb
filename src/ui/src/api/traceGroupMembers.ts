/**
 * A group's member traces for the traces tab's drill-in view.
 *
 * Deliberately a Query IR `rows` query rather than a client-side filter of
 * the row-limited Tempo search response: the group table's dimension pins
 * are enforced server-side here too, so a drill-in returns exactly the
 * group's members instead of whatever fell inside a separately-fetched
 * sample. Bounded by a limit, newest first — consistent with the group
 * table's own budget.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { ROOT_SPAN_SENTINEL, type GroupGrain } from "./traceGroups";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { facetField, type TraceFilter } from "../lib/traceFilters";

export interface TraceGroupMember {
  traceId: string;
  spanId: string;
  /** Null for a root span (or if the source omits it). */
  parentSpanId: string | null;
  spanName: string;
  serviceName: string;
  startNs: string;
  durationNanos: string;
  statusCode: string;
}

/**
 * Build the IR document for a group's members. Pins each grouping dimension
 * to the drilled-in group's value — a `null` value compiles to a negated
 * `exists`, not `eq null`, since the group's "(not set)" row means the field
 * is absent, not that it equals the literal value null.
 */
export function buildMembersDoc(
  dims: string[],
  values: (string | null)[],
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
  limit: number,
): QueryIrRequest {
  const scope: Record<string, unknown>[] =
    grain === "traces"
      ? [
          {
            where: {
              field: "parent_span_id",
              op: "eq",
              value: ROOT_SPAN_SENTINEL,
            },
          },
        ]
      : [];

  const active = filters.flatMap((f) => {
    const facet = facetField(f.field);
    if (!facet) return [];
    return [
      { where: { field: facet.irField, op: "eq", value: f.value } },
    ] as Record<string, unknown>[];
  });

  const pinned = dims.map((dim, i) => ({
    where:
      values[i] == null
        ? { not: { field: dim, op: "exists" } }
        : { field: dim, op: "eq", value: values[i] },
  }));

  return {
    irVersion: 1,
    from: "traces",
    range: {
      from: String(msToNanos(range.fromMs)),
      to: String(msToNanos(range.toMs)),
    },
    result: "rows",
    pipeline: [
      ...scope,
      ...active,
      ...pinned,
      { order: [{ of: "start_time_unix_nano", dir: "desc" }] },
      { limit },
    ],
  };
}

const str = (v: unknown): string => (v == null ? "" : String(v));

/**
 * Decode a `rows` envelope into members.
 *
 * Values are read **positionally**: the traces source's `rows` projection is
 * a fixed eight columns (trace_id, span_id, parent_span_id, span_name,
 * service_name, start_time_unix_nano, duration_nanos, status_code) in that
 * order, under physical names — looking them up by name would silently miss.
 */
export function membersFromIrResponse(
  res: QueryIrResponse,
): TraceGroupMember[] {
  const rows = res.rows ?? [];
  return rows.map((row): TraceGroupMember => {
    const cells = row as unknown[];
    const parentSpanId = cells[2];
    return {
      traceId: str(cells[0]),
      spanId: str(cells[1]),
      parentSpanId: parentSpanId == null ? null : String(parentSpanId),
      spanName: str(cells[3]),
      serviceName: str(cells[4]),
      startNs: str(cells[5]),
      durationNanos: str(cells[6]),
      statusCode: str(cells[7]),
    };
  });
}

export async function fetchTraceGroupMembers(
  dims: string[],
  values: (string | null)[],
  range: ResolvedRange,
  filters: TraceFilter[],
  grain: GroupGrain,
  limit: number,
): Promise<TraceGroupMember[]> {
  return membersFromIrResponse(
    await runIrQuery(
      buildMembersDoc(dims, values, range, filters, grain, limit),
    ),
  );
}
