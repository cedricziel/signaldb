// Trace detail over the native Query IR: every span of one trace with its
// kind, status, attribute containers, and events, plus the profiles captured
// during it — the data the waterfall and span panel render. Replaces the
// Tempo-compat `tempoGetTrace` (which lacks span kind and flattens scopes)
// and the separate span-kinds enrichment.
import type { IrStage, QueryIrRequest, QueryIrResponse } from "./gen";
import { namedRows, runIrQuery, type IrRow } from "./queryIr";
import { ROOT_SPAN_SENTINEL } from "./traceGroups";
import type {
  AttrValue,
  ProfileSummaryView,
  LinkedFromView,
  SpanEventView,
  SpanLinkView,
  TempoSpan,
  TempoTrace,
} from "./traceTypes";
import { msToNanos, type ResolvedRange } from "../lib/time";

/** Lookback for the retry when the trace is outside the viewer's range: a
 * trace opened by pasting its ID may be much older than the explore window. */
export const WIDE_LOOKBACK_MS = 30 * 24 * 60 * 60 * 1000;

/** The trace-id filter. The traces source names the join key `trace_id`;
 * the profiles source calls it `trace.id`. */
function whereTrace(
  traceId: string,
  field: "trace_id" | "trace.id" = "trace_id",
): IrStage {
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
  "span_links",
] as const;

/** How far past the trace's start a linking span may begin: a consumer runs
 * after the producer that enqueued it, usually soon but possibly delayed. */
export const LINKED_FROM_WINDOW_MS = 24 * 60 * 60 * 1000;

/** Cap on linking spans fetched for one trace. */
const LINKED_FROM_LIMIT = 200;

/** Every span of one trace, or of several (`in`) at once. */
export function buildTraceSpansDoc(
  traceId: string | string[],
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: irRange(range),
    result: "rows",
    fields: [...SPAN_FIELDS],
    pipeline: Array.isArray(traceId)
      ? [
          { where: { field: "trace_id", op: "in", value: traceId } },
          { limit: 100_000 },
        ]
      : [whereTrace(traceId)],
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

/** Spans in other traces that link to `traceId` (`links.trace_id eq`), from
 * the trace's start to +24h. `span_links` is projected so each entry can be
 * matched to the targeted span by the link's `span_id`. */
export function buildLinkedFromDoc(
  traceId: string,
  traceStartMs: number,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: irRange({
      fromMs: traceStartMs,
      toMs: traceStartMs + LINKED_FROM_WINDOW_MS,
    }),
    result: "rows",
    fields: [
      "trace_id",
      "span_id",
      "span.name",
      "service.name",
      "start_time_unix_nano",
      "span_links",
    ],
    pipeline: [
      { where: { field: "links.trace_id", op: "eq", value: traceId } },
      { limit: LINKED_FROM_LIMIT },
    ],
  };
}

type Row = IrRow;

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

/** The `span_links` cell: `[{trace_id, span_id, attributes}]`, as a JSON
 * string or an already-parsed array; null or anything else is no links. */
function links(v: unknown): SpanLinkView[] {
  let parsed: unknown = v;
  if (typeof v === "string") {
    if (v === "") return [];
    try {
      parsed = JSON.parse(v);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(parsed)) return [];
  return parsed.map((l) => {
    const link = (l ?? {}) as Record<string, unknown>;
    return {
      traceId: str(link.trace_id),
      spanId: str(link.span_id),
      attributes: container(link.attributes),
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
    parentSpanId:
      !parent || parent === ROOT_SPAN_SENTINEL ? null : String(parent),
    name: str(row.span_name),
    serviceName: str(row.service_name),
    // The Tempo path used lower-case status words; keep the contract.
    status: str(row.status_code || "unset").toLowerCase(),
    ...(statusMessage != null && statusMessage !== ""
      ? { statusMessage: String(statusMessage) }
      : {}),
    ...(kind != null && kind !== "" ? { kind: String(kind) } : {}),
    startNs: str(row.start_time_unix_nano),
    // The IR projects a logical field under its physical column name, so
    // `duration` arrives as `duration_nanos` (the others' names coincide).
    durNs: str(row.duration_nanos ?? row.duration),
    attributes: flattenAttributes(row),
    events: events(row.span_events),
    links: links(row.span_links),
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

/** Decode the linked-from response: one entry per link that points into
 * `traceId`, so a span linking to several spans of it yields several. */
export function linkedFromFromResponse(
  traceId: string,
  res: QueryIrResponse,
): LinkedFromView[] {
  const out: LinkedFromView[] = [];
  for (const row of namedRows(res)) {
    const from = str(row.trace_id);
    // A span linking within its own trace already shows under "Links".
    if (from === traceId) continue;
    for (const link of links(row.span_links)) {
      if (link.traceId !== traceId) continue;
      out.push({
        traceId: from,
        spanId: str(row.span_id),
        name: str(row.span_name),
        serviceName: str(row.service_name),
        startNs: str(row.start_time_unix_nano),
        targetSpanId: link.spanId,
      });
    }
  }
  return out;
}

/** Spans of other traces that link into this one. */
export async function fetchLinkedFrom(
  traceId: string,
  traceStartNs: string,
): Promise<LinkedFromView[]> {
  const startMs = Number(BigInt(traceStartNs || "0") / 1_000_000n);
  const res = await runIrQuery(buildLinkedFromDoc(traceId, startMs));
  return linkedFromFromResponse(traceId, res);
}
