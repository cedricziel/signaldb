// Trace detail over the native Query IR: every span of one trace with its
// kind, status, attribute containers, and events, plus the profiles captured
// during it — the data the waterfall and span panel render. Replaces the
// Tempo-compat `tempoGetTrace` (which lacks span kind and flattens scopes)
// and the separate span-kinds enrichment.
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import type {
  AttrValue,
  ProfileSummaryView,
  SpanEventView,
  TempoSpan,
  TempoTrace,
} from "./tempo";
import { msToNanos, type ResolvedRange } from "../lib/time";

/** Lookback for the retry when the trace is outside the viewer's range: a
 * trace opened by pasting its ID may be much older than the explore window. */
const WIDE_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000;

/** The trace-id filter. The traces source names the join key `trace_id`;
 * the profiles source calls it `trace.id`. */
function whereTrace(
  traceId: string,
  field: "trace_id" | "trace.id" = "trace_id",
): Record<string, unknown> {
  return { where: { field, op: "eq", value: traceId } };
}

function irRange(range: ResolvedRange) {
  return {
    from: String(msToNanos(range.fromMs)),
    to: String(msToNanos(range.toMs)),
  };
}

/** The columns `buildTraceSpansDoc` projects, in order. Rows are decoded by
 * column name (the response echoes them), so order is only for readers. */
const SPAN_FIELDS = [
  "trace_id",
  "span_id",
  "parent_span_id",
  "span.name",
  "service.name",
  "status.code",
  "status_message",
  "start_time_unix_nano",
  "duration",
  "span_kind",
  "span.attributes",
  "scope.attributes",
  "resource.attributes",
  "span_events",
] as const;

export function buildTraceSpansDoc(
  traceId: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: irRange(range),
    result: "rows",
    fields: [...SPAN_FIELDS],
    pipeline: [whereTrace(traceId)],
  };
}

/** Profile summaries for the trace: the profiles source's row defaults are
 * exactly the summary metadata (id, time, duration, sample/period, service,
 * trace/span ids), so no projection is named. */
export function buildTraceProfilesDoc(
  traceId: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "profiles",
    range: irRange(range),
    result: "rows",
    pipeline: [whereTrace(traceId, "trace.id")],
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

/** An attribute container cell: a JSON object from a Map column, or — from a
 * legacy table whose container is still a JSON string — that string. */
function container(v: unknown): Record<string, AttrValue> {
  let obj: unknown = v;
  if (typeof v === "string") {
    try {
      obj = JSON.parse(v);
    } catch {
      return {};
    }
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) return {};
  const out: Record<string, AttrValue> = {};
  for (const [k, val] of Object.entries(obj as Record<string, unknown>)) {
    if (val == null) continue;
    out[k] =
      typeof val === "string" ||
      typeof val === "number" ||
      typeof val === "boolean"
        ? val
        : JSON.stringify(val);
  }
  return out;
}

/** The `span_events` cell: `[{name, timestamp_unix_nano, attributes}]`. */
function events(v: unknown): SpanEventView[] {
  if (typeof v !== "string" || v === "") return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(v);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  return parsed.map((e) => {
    const ev = (e ?? {}) as Record<string, unknown>;
    return {
      name: str(ev.name),
      timeUnixNano: str(ev.timestamp_unix_nano),
      attributes: container(ev.attributes),
    };
  });
}

/** Flatten the three containers into the span panel's one bag: span
 * attributes as-is, scope and resource attributes under their prefix. */
function flattenAttributes(row: Row): Record<string, AttrValue> {
  const out: Record<string, AttrValue> = { ...container(row.span_attributes) };
  for (const [k, v] of Object.entries(container(row.scope_attributes))) {
    out[`scope.${k}`] = v;
  }
  for (const [k, v] of Object.entries(container(row.resource_attributes))) {
    out[`resource.${k}`] = v;
  }
  return out;
}

function toSpan(row: Row): TempoSpan {
  const parent = row.parent_span_id;
  const kind = row.span_kind;
  const statusMessage = row.status_message;
  return {
    spanId: str(row.span_id),
    parentSpanId: parent == null || parent === "" ? null : String(parent),
    name: str(row.span_name),
    serviceName: str(row.service_name),
    // The Tempo path used lower-case status words; keep the contract.
    status: str(row.status_code || "unset").toLowerCase(),
    ...(statusMessage != null && statusMessage !== ""
      ? { statusMessage: String(statusMessage) }
      : {}),
    ...(kind != null && kind !== "" ? { kind: String(kind) } : {}),
    startNs: str(row.start_time_unix_nano),
    durNs: str(row.duration),
    attributes: flattenAttributes(row),
    events: events(row.span_events),
  };
}

function toProfile(row: Row): ProfileSummaryView {
  const spanId = row.span_id;
  return {
    profileId: str(row.profile_id),
    timeUnixNano: str(row.timestamp),
    durationNano: str(row.duration_nano),
    sampleType: str(row.sample_type),
    sampleUnit: str(row.sample_unit),
    serviceName: str(row.service_name),
    spanId: spanId == null || spanId === "" ? null : String(spanId),
  };
}

/** The root span: no parent wins, else the earliest span. */
function rootSpan(spans: TempoSpan[]): TempoSpan | undefined {
  return (
    spans.find((s) => s.parentSpanId === null) ??
    [...spans].sort((a, b) =>
      BigInt(a.startNs) < BigInt(b.startNs) ? -1 : 1,
    )[0]
  );
}

/**
 * Assemble the trace view from the two IR responses. `undefined` when the
 * spans response is empty (trace not in the queried window).
 */
export function traceFromIrResponses(
  traceId: string,
  spansRes: QueryIrResponse,
  profilesRes: QueryIrResponse | undefined,
): TempoTrace | undefined {
  const spans = namedRows(spansRes).map(toSpan);
  if (spans.length === 0) return undefined;
  const root = rootSpan(spans)!;
  let startNs = BigInt(spans[0]!.startNs || "0");
  let endNs = startNs;
  for (const s of spans) {
    const start = BigInt(s.startNs || "0");
    const end = start + BigInt(s.durNs || "0");
    if (start < startNs) startNs = start;
    if (end > endNs) endNs = end;
  }
  return {
    traceId,
    rootServiceName: root.serviceName,
    rootTraceName: root.name,
    startNs: startNs.toString(),
    durationMs: Number((endNs - startNs) / 1_000_000n),
    rootAttributes: root.attributes,
    rootError: root.status === "error",
    profiles: profilesRes ? namedRows(profilesRes).map(toProfile) : [],
    spans,
  };
}

/**
 * Fetch one trace over the Query IR. Looks in the viewer's range first (the
 * cheap, partition-pruned scan); a trace opened by ID that isn't there is
 * retried over the last 30 days. `null` when neither finds it.
 */
export async function fetchTraceDetail(
  traceId: string,
  range: ResolvedRange,
  nowMs = Date.now(),
): Promise<TempoTrace | null> {
  let window = range;
  let spansRes = await runIrQuery(buildTraceSpansDoc(traceId, window));
  if ((spansRes.rows ?? []).length === 0) {
    window = { fromMs: nowMs - WIDE_LOOKBACK_MS, toMs: nowMs };
    spansRes = await runIrQuery(buildTraceSpansDoc(traceId, window));
    if ((spansRes.rows ?? []).length === 0) return null;
  }
  const profilesRes = await runIrQuery(buildTraceProfilesDoc(traceId, window));
  // `null`, not `undefined`: react-query rejects undefined query data.
  return traceFromIrResponses(traceId, spansRes, profilesRes) ?? null;
}
