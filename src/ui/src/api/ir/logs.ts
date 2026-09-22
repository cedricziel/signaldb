/**
 * Logs tab over the native Query IR: rows (the log list) and per-severity
 * volume (the histogram), replacing the LogQL-compiled `api/loki.ts` calls.
 * Field/value discovery (`FieldSidebar`) stays on Loki labels until the
 * `describe`-based discovery client lands.
 */
import type { QueryIrRequest, QueryIrResponse } from "../gen";
import type { LabelFilter } from "../../lib/filters";
import { msToNanos, type ResolvedRange } from "../../lib/time";
import { runIrQuery } from "../queryIr";

export interface LogRow {
  tsNs: string;
  tsMs: number;
  body: string;
  serviceName: string;
  severityText: string;
  traceId: string | null;
  spanId: string | null;
  scopeName: string;
  logAttributes: Record<string, string>;
  scopeAttributes: Record<string, string>;
  resourceAttributes: Record<string, string>;
}

export interface HistogramSeries {
  level: string;
  /** [timestampMs, count] pairs, ascending. */
  points: [number, number][];
}

/** Chip labels spelled the Loki way (the picker isn't on `describe` yet, see
 * module doc) canonicalized to the IR's logical field names — everything
 * else is passed through as an attribute name the resolver searches every
 * scope for. */
function irFieldForLabel(label: string): string {
  if (label === "level") return "severity_text";
  if (label === "service_name") return "service.name";
  return label;
}

function filterWhere(f: LabelFilter): Record<string, unknown> {
  const field = irFieldForLabel(f.label);
  switch (f.op) {
    case "=":
      return { field, op: "eq", value: f.value };
    case "!=":
      return { field, op: "ne", value: f.value };
    case "=~":
      return { field, op: "regex", value: f.value };
    case "!~":
      return { not: { field, op: "regex", value: f.value } };
  }
}

function irRange(range: ResolvedRange) {
  return {
    from: String(msToNanos(range.fromMs)),
    to: String(msToNanos(range.toMs)),
  };
}

/** Columns `buildLogRowsDoc` projects, in the order the physical names come
 * back (the response echoes them by name, so order is only for readers). */
const ROW_FIELDS = [
  "timestamp",
  "body",
  "service.name",
  "severity_text",
  "trace_id",
  "span_id",
  "scope.name",
  "log.attributes",
  "scope.attributes",
  "resource.attributes",
] as const;

export function buildLogRowsDoc(
  filters: LabelFilter[],
  search: string,
  range: ResolvedRange,
  limit: number,
): QueryIrRequest {
  const where = filters.map((f) => ({ where: filterWhere(f) }));
  if (search.trim() !== "") {
    where.push({
      where: { field: "body", op: "contains", value: search.trim() },
    });
  }
  return {
    irVersion: 1,
    from: "logs",
    range: irRange(range),
    result: "rows",
    fields: [...ROW_FIELDS],
    pipeline: [
      ...where,
      { order: [{ of: "timestamp", dir: "desc" }] },
      { limit },
    ],
  };
}

export function buildLogVolumeDoc(
  filters: LabelFilter[],
  search: string,
  range: ResolvedRange,
  step: string,
): QueryIrRequest {
  const where = filters.map((f) => ({ where: filterWhere(f) }));
  if (search.trim() !== "") {
    where.push({
      where: { field: "body", op: "contains", value: search.trim() },
    });
  }
  return {
    irVersion: 1,
    from: "logs",
    range: irRange(range),
    result: "series",
    pipeline: [
      ...where,
      {
        aggregate: {
          by: ["severity_text"],
          aggs: [{ fn: "count", as: "count" }],
          step,
        },
      },
    ],
  };
}

type Row = Record<string, unknown>;

/** Rows as objects keyed by the response's column names. */
function namedRows(res: QueryIrResponse): Row[] {
  const names = (res.columns ?? []).map((c) => c.name);
  return (res.rows ?? []).map((row) => {
    const cells = row as unknown[];
    const out: Row = {};
    names.forEach((n, i) => {
      out[n] = cells[i];
    });
    return out;
  });
}

const str = (v: unknown): string => (v == null ? "" : String(v));

function nullableStr(v: unknown): string | null {
  return v == null || v === "" ? null : String(v);
}

/** An attribute container cell: a JSON object from a Map column, or — from a
 * legacy table whose container is still a JSON string — that string. */
function container(v: unknown): Record<string, string> {
  let obj: unknown = v;
  if (typeof v === "string") {
    try {
      obj = JSON.parse(v);
    } catch {
      return {};
    }
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return {};
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(obj as Record<string, unknown>)) {
    if (val != null) out[k] = String(val);
  }
  return out;
}

function toLogRow(row: Row): LogRow {
  const tsNs = str(row.timestamp);
  return {
    tsNs,
    tsMs: Number(BigInt(tsNs || "0") / 1_000_000n),
    body: str(row.body),
    serviceName: str(row.service_name),
    severityText: str(row.severity_text),
    traceId: nullableStr(row.trace_id),
    spanId: nullableStr(row.span_id),
    scopeName: str(row.scope_name),
    logAttributes: container(row.log_attributes),
    scopeAttributes: container(row.scope_attributes),
    resourceAttributes: container(row.resource_attributes),
  };
}

export async function runLogRows(
  filters: LabelFilter[],
  search: string,
  range: ResolvedRange,
  limit: number,
): Promise<LogRow[]> {
  const res = await runIrQuery(buildLogRowsDoc(filters, search, range, limit));
  return namedRows(res).map(toLogRow);
}

function toHistogramSeries(
  series: NonNullable<QueryIrResponse["series"]>,
): HistogramSeries[] {
  return series.map((s) => ({
    level: s.labels["severity_text"] || "unknown",
    points: s.points.map((p): [number, number] => [
      Number(p[0]) / 1_000_000,
      Number(p[1]),
    ]),
  }));
}

export async function runLogVolume(
  filters: LabelFilter[],
  search: string,
  range: ResolvedRange,
  step: string,
): Promise<HistogramSeries[]> {
  const res = await runIrQuery(buildLogVolumeDoc(filters, search, range, step));
  return toHistogramSeries(res.series ?? []);
}
