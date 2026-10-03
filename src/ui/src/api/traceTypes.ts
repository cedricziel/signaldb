// Trace/span view types shared by the trace detail and group-search paths,
// both of which are Query IR reads (api/traceDetail.ts,
// api/traceGroupMembers.ts, api/traceGroups.ts) — this module holds no
// HTTP client of its own. Named for the shape (a rendered trace/span), not
// the now-removed Tempo compat client that originally produced it.

export type AttrValue = string | number | boolean;

/** A span event (annotation or exception) attached to a span. */
export interface SpanEventView {
  name: string;
  timeUnixNano: string;
  attributes: Record<string, AttrValue>;
}

/** An outgoing span link: the (possibly other-trace) span this one points at. */
export interface SpanLinkView {
  traceId: string;
  spanId: string;
  attributes: Record<string, AttrValue>;
}

/** A span in another trace whose link points into the viewed trace. */
export interface LinkedFromView {
  /** The linking span's trace and span. */
  traceId: string;
  spanId: string;
  name: string;
  serviceName: string;
  startNs: string;
  /** The span of the viewed trace the link targets (`links.span_id`). */
  targetSpanId: string;
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
  /** Outgoing links (`span_links`); absent or empty when the span has none. */
  links?: SpanLinkView[];
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
