// Client for the router's Tempo-compatible API (/tempo/api).
//
// Everything below `tempoSearchTags` predates the generated OpenAPI client
// and talks to `/tempo/api` directly via `tempoFetch`; new endpoints go
// through the generated client instead (see `./schema.ts` for the same
// `unwrap`-a-`SdkResult` shape) — `tempoSearchTags` is the first of those.

import "./client";
import { searchTags } from "./gen";
import type { ResolvedRange } from "../lib/time";
import {
  ApiError,
  retryAfterMsFrom,
  retryingFetch,
  tenantHeaders,
} from "./http";

export type AttrValue = string | number | boolean;

/** A span event (annotation or exception) attached to a span. */
export interface SpanEventView {
  name: string;
  timeUnixNano: string;
  attributes: Record<string, AttrValue>;
}

export interface TempoSpan {
  spanId: string;
  parentSpanId: string | null;
  name: string;
  serviceName: string;
  /** "ok" | "error" | "unset" */
  status: string;
  /** Status description, when the span set one (IR path only). */
  statusMessage?: string;
  /** OTel span kind, when known (IR path only; the Tempo wire lacks it). */
  kind?: string;
  startNs: string;
  durNs: string;
  attributes: Record<string, AttrValue>;
  /** Span events; exceptions are the event named "exception". */
  events: SpanEventView[];
}

export interface TraceSummary {
  traceId: string;
  rootServiceName: string;
  rootTraceName: string;
  startNs: string;
  durationMs: number;
  /**
   * Root span attributes (resource attributes prefixed "resource."), the
   * grouping dimensions for the traces landing screen.
   */
  rootAttributes: Record<string, AttrValue>;
  /** True when the root span's status is "error". */
  rootError: boolean;
}

/** Summary of a profile linked to a trace (or one of its spans), without the
 * bulky stack/sample payload — enough to offer a "view this profile" link. */
export interface ProfileSummaryView {
  profileId: string;
  timeUnixNano: string;
  durationNano: string;
  sampleType: string;
  sampleUnit: string;
  serviceName: string;
  spanId: string | null;
}

export interface TempoTrace extends TraceSummary {
  spans: TempoSpan[];
  /** Profiles captured during this trace, requested via include_profiles. */
  profiles: ProfileSummaryView[];
}

interface WireValue {
  stringValue?: string;
  intValue?: number;
  boolValue?: boolean;
  doubleValue?: number;
}

interface WireSpanEvent {
  name: string;
  timeUnixNano: string;
  attributes?: Record<string, { key: string; value: WireValue }>;
}

interface WireSpan {
  spanID: string;
  startTimeUnixNano: string;
  durationNanos: string;
  name?: string;
  parentSpanID?: string;
  serviceName?: string;
  status?: string;
  attributes?: Record<string, { key: string; value: WireValue }>;
  events?: WireSpanEvent[];
}

interface WireTrace {
  traceID: string;
  rootServiceName: string;
  rootTraceName: string;
  startTimeUnixNano: string;
  durationMs: number;
  spanSets?: { spans: WireSpan[]; matched: number }[];
}

export function flattenAttrValue(v: WireValue): AttrValue {
  if (v.stringValue !== undefined) return v.stringValue;
  if (v.intValue !== undefined) return v.intValue;
  if (v.doubleValue !== undefined) return v.doubleValue;
  if (v.boolValue !== undefined) return v.boolValue;
  return "";
}

function flattenAttrs(
  wire: Record<string, { key: string; value: WireValue }> | undefined,
): Record<string, AttrValue> {
  const attributes: Record<string, AttrValue> = {};
  for (const [key, attr] of Object.entries(wire ?? {})) {
    attributes[key] = flattenAttrValue(attr.value);
  }
  return attributes;
}

function toSpan(w: WireSpan): TempoSpan {
  // OTLP encodes "no parent" as an all-zero span id.
  const parent = w.parentSpanID?.replace(/0/g, "") ? w.parentSpanID : null;
  return {
    spanId: w.spanID,
    parentSpanId: parent,
    name: w.name ?? "",
    serviceName: w.serviceName ?? "",
    status: w.status ?? "unset",
    startNs: w.startTimeUnixNano,
    durNs: w.durationNanos,
    attributes: flattenAttrs(w.attributes),
    events: (w.events ?? []).map((e) => ({
      name: e.name,
      timeUnixNano: e.timeUnixNano,
      attributes: flattenAttrs(e.attributes),
    })),
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

async function tempoFetch<T>(
  path: string,
  params: URLSearchParams,
): Promise<T> {
  const query = params.size > 0 ? `?${params}` : "";
  const res = await retryingFetch(`/tempo/api/${path}${query}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Tempo API ${path} failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
  return (await res.json()) as T;
}

export async function tempoSearch(
  range: ResolvedRange,
  limit: number,
  /** TraceQL selector; omitted from the request when empty. */
  query = "",
): Promise<TraceSummary[]> {
  const params = new URLSearchParams({
    start: String(Math.floor(range.fromMs / 1000)),
    end: String(Math.ceil(range.toMs / 1000)),
    limit: String(limit),
  });
  if (query !== "") params.set("q", query);
  const wire = await tempoFetch<{ traces?: WireTrace[] }>("search", params);
  return (wire.traces ?? []).map((t) => {
    const root = rootSpan(
      (t.spanSets ?? []).flatMap((s) => s.spans.map(toSpan)),
    );
    return {
      traceId: t.traceID,
      rootServiceName: t.rootServiceName,
      rootTraceName: t.rootTraceName,
      startNs: t.startTimeUnixNano,
      durationMs: t.durationMs,
      rootAttributes: root?.attributes ?? {},
      rootError: root?.status === "error",
    };
  });
}

/** Trace attribute tag names observed in the window (#1073), for filter/
 * group-by key autocomplete — the traces-tab analog of `lokiLabels`. */
export async function tempoSearchTags(range: ResolvedRange): Promise<string[]> {
  const result = await searchTags({
    query: {
      start: Math.floor(range.fromMs / 1000),
      end: Math.ceil(range.toMs / 1000),
    },
  });
  const { data, error, response } = result;
  if (error !== undefined || !response?.ok) {
    const status = response?.status ?? 0;
    const message =
      (error as { error?: string } | undefined)?.error ??
      `Tempo tags request failed (${status})`;
    throw new ApiError(message, status, retryAfterMsFrom(response));
  }
  return data!.tagNames;
}
