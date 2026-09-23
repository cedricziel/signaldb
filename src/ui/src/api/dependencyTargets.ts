/**
 * Per-dependency detail behind the "Time by dependency" bar (see
 * `./dependencyBreakdown.ts` for the bar itself): one row per distinct
 * downstream *target* the service calls, plus a synthetic "(self)" row for
 * in-process time.
 *
 * A target is the first semconv attribute that identifies *which* downstream
 * instance a CLIENT span called: `db.namespace` for a database call,
 * `server.address` for HTTP, `rpc.service` for RPC, and
 * `messaging.destination.name` for messaging — scoped by the same
 * kind-defining attribute `dependencyBreakdown.ts` already filters on
 * (`db.system.name`, `http.request.method`, `rpc.system`,
 * `messaging.system`), so a span counts toward at most one kind.
 *
 * The Query IR has no coalesce/CASE expression (same limitation
 * `dependencyBreakdown.ts` documents), so this is one grouped query per kind
 * rather than one query grouped by "whichever target attribute is present".
 * `aggregate.by` groups a missing attribute's `null` values together (see
 * docs/users/querying-ir.md's "usable in aggregate.by ... null on rows"), so
 * spans of a kind with no target attribute collapse into a single row per
 * kind automatically — the "fall back to one row per kind" case is just
 * "target came back null", not special-cased here.
 *
 * Self (in-process) time is the service's total request time — summed
 * duration of its own SERVER spans — minus its total CLIENT time across
 * every kind, not just the ones broken out above: a span whose kind we don't
 * recognize is still downstream time, it just isn't its own row.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";

export type DependencyKind = "database" | "http" | "rpc" | "messaging";

export interface DependencyTargetRow {
  key: string;
  kind: DependencyKind;
  /** The downstream target, or the kind's own label when no target
   * attribute was present on these spans (the fallback case). */
  target: string;
  /** e.g. "postgresql · SELECT"; empty when no operation attribute exists. */
  operation: string;
  durationNs: number;
  count: number;
  p95Ns: number;
}

export interface DependencyTargetsResult {
  rows: DependencyTargetRow[];
  /** Total request (SERVER span) time — the share denominator for every row,
   * including self. */
  requestDurationNs: number;
  /** Total request (SERVER span) count — the calls/req denominator. */
  requestCount: number;
  /** Total time minus all downstream CLIENT time. */
  selfDurationNs: number;
}

interface KindConfig {
  kind: DependencyKind;
  label: string;
  filterAttr: string;
  targetAttr: string;
  opAttrs: [string, string];
}

const KIND_CONFIGS: KindConfig[] = [
  {
    kind: "database",
    label: "Database",
    filterAttr: "db.system.name",
    targetAttr: "db.namespace",
    opAttrs: ["db.system.name", "db.operation.name"],
  },
  {
    kind: "http",
    label: "HTTP",
    filterAttr: "http.request.method",
    targetAttr: "server.address",
    opAttrs: ["http.request.method", "url.template"],
  },
  {
    kind: "rpc",
    label: "RPC",
    filterAttr: "rpc.system",
    targetAttr: "rpc.service",
    opAttrs: ["rpc.system", "rpc.method"],
  },
  {
    kind: "messaging",
    label: "Messaging",
    filterAttr: "messaging.system",
    targetAttr: "messaging.destination.name",
    opAttrs: ["messaging.system", "messaging.operation"],
  },
];

function rangeDoc(range: ResolvedRange) {
  return {
    from: String(msToNanos(range.fromMs)),
    to: String(msToNanos(range.toMs)),
  };
}

function baseWhere(
  serviceName: string,
  spanKind: "Client" | "Server",
): Record<string, unknown>[] {
  return [
    { where: { field: "service.name", op: "eq", value: serviceName } },
    { where: { field: "span_kind", op: "eq", value: spanKind } },
  ];
}

/** Total time+count across a span kind, with no per-target grouping — used
 * both for the SERVER-side request total and the CLIENT-side downstream
 * total that self time is computed from. */
function totalsDoc(
  serviceName: string,
  range: ResolvedRange,
  spanKind: "Client" | "Server",
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      ...baseWhere(serviceName, spanKind),
      {
        aggregate: {
          by: ["service.name"],
          aggs: [
            { fn: "sum", of: "duration", as: "total" },
            { fn: "count", as: "n" },
          ],
        },
      },
    ],
  };
}

function decodeTotals(res: QueryIrResponse): {
  durationNs: number;
  count: number;
} {
  const row = res.rows?.[0] as unknown[] | undefined;
  if (!row) return { durationNs: 0, count: 0 };
  const total = row[1];
  const n = row[2];
  return {
    durationNs: typeof total === "number" ? total : 0,
    count: typeof n === "number" ? n : 0,
  };
}

function kindDoc(
  cfg: KindConfig,
  serviceName: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      ...baseWhere(serviceName, "Client"),
      { where: { field: cfg.filterAttr, op: "exists" } },
      {
        aggregate: {
          by: ["service.name", cfg.targetAttr, ...cfg.opAttrs],
          aggs: [
            { fn: "sum", of: "duration", as: "total" },
            { fn: "count", as: "n" },
            { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
          ],
        },
      },
      { limit: 50 },
    ],
  };
}

function decodeKindRows(
  cfg: KindConfig,
  res: QueryIrResponse,
): DependencyTargetRow[] {
  const rows = (res.rows ?? []) as unknown[][];
  return rows
    .map((row, i): DependencyTargetRow => {
      const target = row[1];
      const op1 = row[2];
      const op2 = row[3];
      const total = row[4];
      const n = row[5];
      const p95 = row[6];
      const operation = [op1, op2]
        .filter((v): v is string => typeof v === "string" && v.length > 0)
        .join(" · ");
      const targetLabel =
        typeof target === "string" && target.length > 0 ? target : cfg.label;
      return {
        key: `${cfg.kind}:${targetLabel}:${operation}:${i}`,
        kind: cfg.kind,
        target: targetLabel,
        operation,
        durationNs: typeof total === "number" ? total : 0,
        count: typeof n === "number" ? n : 0,
        p95Ns: typeof p95 === "number" ? p95 : 0,
      };
    })
    .filter((r) => r.durationNs > 0 || r.count > 0);
}

export async function fetchDependencyTargets(
  serviceName: string,
  range: ResolvedRange,
): Promise<DependencyTargetsResult> {
  const [server, clientBaseline, ...kindResults] = await Promise.all([
    runIrQuery(totalsDoc(serviceName, range, "Server")).then(decodeTotals),
    runIrQuery(totalsDoc(serviceName, range, "Client")).then(decodeTotals),
    ...KIND_CONFIGS.map((cfg) =>
      runIrQuery(kindDoc(cfg, serviceName, range)).then((res) =>
        decodeKindRows(cfg, res),
      ),
    ),
  ]);

  return {
    rows: kindResults.flat(),
    requestDurationNs: server.durationNs,
    requestCount: server.count,
    selfDurationNs: Math.max(0, server.durationNs - clientBaseline.durationNs),
  };
}
