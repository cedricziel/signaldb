// Client for the router's Tempo-compatible API (/tempo/api).

import type { ResolvedRange } from "../lib/time";

export type AttrValue = string | number | boolean;

export interface TempoSpan {
  spanId: string;
  parentSpanId: string | null;
  name: string;
  serviceName: string;
  /** "ok" | "error" | "unset" */
  status: string;
  startNs: string;
  durNs: string;
  attributes: Record<string, AttrValue>;
}

export interface TraceSummary {
  traceId: string;
  rootServiceName: string;
  rootTraceName: string;
  startNs: string;
  durationMs: number;
}

export interface TempoTrace extends TraceSummary {
  spans: TempoSpan[];
}

interface WireValue {
  stringValue?: string;
  intValue?: number;
  boolValue?: boolean;
  doubleValue?: number;
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

function toSpan(w: WireSpan): TempoSpan {
  const attributes: Record<string, AttrValue> = {};
  for (const [key, attr] of Object.entries(w.attributes ?? {})) {
    attributes[key] = flattenAttrValue(attr.value);
  }
  return {
    spanId: w.spanID,
    parentSpanId: w.parentSpanID || null,
    name: w.name ?? "",
    serviceName: w.serviceName ?? "",
    status: w.status ?? "unset",
    startNs: w.startTimeUnixNano,
    durNs: w.durationNanos,
    attributes,
  };
}

async function tempoFetch<T>(
  path: string,
  params: URLSearchParams,
): Promise<T> {
  const query = params.size > 0 ? `?${params}` : "";
  const res = await fetch(`/tempo/api/${path}${query}`, {
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(
      `Tempo API ${path} failed (${res.status}): ${body.slice(0, 300)}`,
    );
  }
  return (await res.json()) as T;
}

export async function tempoGetTrace(
  traceId: string,
  range?: ResolvedRange,
): Promise<TempoTrace> {
  const params = new URLSearchParams();
  if (range) {
    params.set("start", String(Math.floor(range.fromMs / 1000)));
    params.set("end", String(Math.ceil(range.toMs / 1000)));
  }
  const wire = await tempoFetch<WireTrace>(
    `traces/${encodeURIComponent(traceId)}`,
    params,
  );
  return {
    traceId: wire.traceID,
    rootServiceName: wire.rootServiceName,
    rootTraceName: wire.rootTraceName,
    startNs: wire.startTimeUnixNano,
    durationMs: wire.durationMs,
    spans: (wire.spanSets ?? []).flatMap((s) => s.spans.map(toSpan)),
  };
}

export async function tempoSearch(
  range: ResolvedRange,
  limit: number,
): Promise<TraceSummary[]> {
  const params = new URLSearchParams({
    start: String(Math.floor(range.fromMs / 1000)),
    end: String(Math.ceil(range.toMs / 1000)),
    limit: String(limit),
  });
  const wire = await tempoFetch<{ traces?: WireTrace[] }>("search", params);
  return (wire.traces ?? []).map((t) => ({
    traceId: t.traceID,
    rootServiceName: t.rootServiceName,
    rootTraceName: t.rootTraceName,
    startNs: t.startTimeUnixNano,
    durationMs: t.durationMs,
  }));
}
