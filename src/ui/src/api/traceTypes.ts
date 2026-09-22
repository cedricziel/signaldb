// Tempo-shaped types shared by the trace detail and group-search paths,
// both of which are Query IR reads now (api/traceDetail.ts,
// api/traceGroupMembers.ts, api/traceGroups.ts) — this module holds no
// HTTP client of its own.

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
