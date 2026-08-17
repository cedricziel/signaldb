/**
 * Facet definitions and TraceQL compilation for the traces tab.
 *
 * Facet value counts (see api/traceFacets.ts) are a Query IR aggregate, so
 * the backend can enumerate any attribute exactly — `/api/search/tags`'s old
 * hardcoded three-name list (#1073) is no longer the constraint. This list
 * is a curated set with a defined TraceQL selector and quoting rule per
 * field, not an enumeration limit; add an entry when a field gets a UI
 * treatment (a facet sidebar row, a catalog drill-down), not speculatively.
 */

import { escapeQuotedString, upsertBy } from "./collections";

export interface TraceFilter {
  field: string;
  value: string;
}

export interface FacetField {
  /** The filter's stored field, and the URL param's key. */
  field: string;
  /** Column header in the sidebar. */
  label: string;
  /** Logical field the Query IR aggregates by to count values. */
  irField: string;
  /** TraceQL left-hand side, scoped where the tag is not intrinsic. */
  selector: string;
  /** Intrinsic enums are bare in TraceQL; everything else is quoted. */
  quoted: boolean;
  /**
   * Several values may be selected at once (compiled to one `in`); the
   * sidebar lists `values` as a fixed set rather than only what the data
   * currently holds, so every option is always offered.
   */
  multi?: true;
  values?: readonly string[];
}

/** OTel span kinds, in the writer's stored spelling. */
export const KIND_VALUES = [
  "Server",
  "Client",
  "Internal",
  "Producer",
  "Consumer",
] as const;

/**
 * The kinds selected when the state names none: every kind that marks a
 * remote-boundary span. Internal spans are noise for the traces landing
 * page and are opted into.
 */
export const DEFAULT_KIND_FILTERS: TraceFilter[] = [
  "Server",
  "Client",
  "Producer",
  "Consumer",
].map((value) => ({ field: "kind", value }));

export const FACET_FIELDS: FacetField[] = [
  {
    field: "service.name",
    label: "service.name",
    irField: "service.name",
    selector: "resource.service.name",
    quoted: true,
  },
  {
    field: "name",
    label: "span.name",
    irField: "span.name",
    selector: "name",
    quoted: true,
  },
  {
    field: "status",
    label: "status",
    irField: "status.code",
    selector: "status",
    quoted: false,
  },
  {
    field: "kind",
    label: "span.kind",
    irField: "span_kind",
    selector: "kind",
    quoted: false,
    multi: true,
    values: KIND_VALUES,
  },
  {
    // A span attribute (db client spans set it directly, not on the
    // resource), so the selector is `span.`-scoped, not `resource.`-scoped
    // like service.name above.
    field: "db.namespace",
    label: "db.namespace",
    irField: "db.namespace",
    selector: "span.db.namespace",
    quoted: true,
  },
  {
    // Same reasoning as db.namespace: messaging client instrumentation sets
    // this on the producer/consumer span, not the resource.
    field: "messaging.destination.name",
    label: "messaging.destination.name",
    irField: "messaging.destination.name",
    selector: "span.messaging.destination.name",
    quoted: true,
  },
  {
    // Unlike db.namespace/messaging.*, these describe the process emitting
    // telemetry rather than an individual operation — OTel resource
    // detectors (or a collector's resourcedetection processor) set them on
    // the resource, mirroring service.name above.
    field: "host.name",
    label: "host.name",
    irField: "host.name",
    selector: "resource.host.name",
    quoted: true,
  },
  {
    field: "k8s.pod.name",
    label: "k8s.pod.name",
    irField: "k8s.pod.name",
    selector: "resource.k8s.pod.name",
    quoted: true,
  },
  {
    field: "k8s.namespace.name",
    label: "k8s.namespace.name",
    irField: "k8s.namespace.name",
    selector: "resource.k8s.namespace.name",
    quoted: true,
  },
  {
    field: "k8s.node.name",
    label: "k8s.node.name",
    irField: "k8s.node.name",
    selector: "resource.k8s.node.name",
    quoted: true,
  },
  {
    field: "container.name",
    label: "container.name",
    irField: "container.name",
    selector: "resource.container.name",
    quoted: true,
  },
  {
    field: "process.pid",
    label: "process.pid",
    irField: "process.pid",
    selector: "resource.process.pid",
    quoted: true,
  },
];

export function facetField(field: string): FacetField | undefined {
  return FACET_FIELDS.find((f) => f.field === field);
}

/**
 * Compile filters into a TraceQL selector for `/api/search?q=`. Filters on
 * fields that are not facetable are dropped rather than emitted as invalid
 * TraceQL. An empty set compiles to "" — the caller omits `q` entirely.
 */
export function compileTraceQL(filters: TraceFilter[]): string {
  const terms = groupByFacet(filters).map(({ facet, values }) => {
    const eq = (v: string) =>
      `${facet.selector} = ${facet.quoted ? `"${escapeQuotedString(v)}"` : v}`;
    return values.length === 1
      ? eq(values[0]!)
      : `(${values.map(eq).join(" || ")})`;
  });
  return terms.length === 0 ? "" : `{ ${terms.join(" && ")} }`;
}

/** Filters grouped by facet, in first-seen order; unknown fields dropped. */
function groupByFacet(
  filters: TraceFilter[],
): { facet: FacetField; values: string[] }[] {
  const groups: { facet: FacetField; values: string[] }[] = [];
  for (const f of filters) {
    const facet = facetField(f.field);
    if (!facet) continue;
    const group = groups.find((g) => g.facet.field === facet.field);
    if (group) group.values.push(f.value);
    else groups.push({ facet, values: [f.value] });
  }
  return groups;
}

/**
 * The Query IR `where` stages for a filter set: one `eq` per single-valued
 * field, one `in` for a field with several values. `excludeIrField` leaves
 * that facet's own filters out — a facet's value counts must not be
 * narrowed by the facet itself.
 */
export function filterStages(
  filters: TraceFilter[],
  excludeIrField?: string,
): Record<string, unknown>[] {
  return groupByFacet(filters)
    .filter(({ facet }) => facet.irField !== excludeIrField)
    .map(({ facet, values }) => ({
      where:
        values.length === 1
          ? { field: facet.irField, op: "eq", value: values[0] }
          : { field: facet.irField, op: "in", value: values },
    }));
}

/**
 * The filters as the traces tab reads them: when the state names no kind,
 * the default kinds apply — so a fresh URL and a link from elsewhere both
 * start on remote-boundary spans.
 */
export function withDefaultTraceFilters(filters: TraceFilter[]): TraceFilter[] {
  return filters.some((f) => f.field === "kind")
    ? filters
    : [...filters, ...DEFAULT_KIND_FILTERS];
}

/** The filters as the URL carries them: the default kind set is implied. */
export function traceFiltersForUrl(filters: TraceFilter[]): TraceFilter[] {
  const kinds = filters.filter((f) => f.field === "kind").map((f) => f.value);
  const defaults = DEFAULT_KIND_FILTERS.map((f) => f.value);
  const isDefault =
    kinds.length === defaults.length &&
    defaults.every((k) => kinds.includes(k));
  return isDefault ? filters.filter((f) => f.field !== "kind") : filters;
}

/** URL serialization: one `tf` param per filter, "field|value". */
export function traceFilterToParam(f: TraceFilter): string {
  return `${f.field}|${f.value}`;
}

export function traceFilterFromParam(param: string): TraceFilter | null {
  const sep = param.indexOf("|");
  if (sep === -1) return null;
  const field = param.slice(0, sep);
  if (facetField(field) === undefined) return null;
  // Values may contain the separator; only the first one delimits.
  return { field, value: param.slice(sep + 1) };
}

/**
 * One value per field — selecting another replaces the previous choice —
 * except for a multi facet, where a further value is added alongside.
 */
export function upsertTraceFilter(
  filters: TraceFilter[],
  next: TraceFilter,
): TraceFilter[] {
  if (facetField(next.field)?.multi) {
    return filters.some((f) => f.field === next.field && f.value === next.value)
      ? filters
      : [...filters, next];
  }
  return upsertBy(filters, next, (f) => f.field === next.field);
}

/**
 * Drop one filter. Removing the last value of a multi facet with a fixed
 * value set selects every value instead: an empty kind selection would
 * mean "no kinds", which is never what unchecking the last box intends.
 */
export function removeTraceFilter(
  filters: TraceFilter[],
  gone: TraceFilter,
): TraceFilter[] {
  const rest = filters.filter(
    (f) => !(f.field === gone.field && f.value === gone.value),
  );
  const facet = facetField(gone.field);
  if (
    facet?.multi &&
    facet.values &&
    !rest.some((f) => f.field === gone.field)
  ) {
    return [
      ...rest,
      ...facet.values.map((value) => ({ field: gone.field, value })),
    ];
  }
  return rest;
}
