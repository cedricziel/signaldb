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
}

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

function escapeTraceQLString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

/**
 * Compile filters into a TraceQL selector for `/api/search?q=`. Filters on
 * fields that are not facetable are dropped rather than emitted as invalid
 * TraceQL. An empty set compiles to "" — the caller omits `q` entirely.
 */
export function compileTraceQL(filters: TraceFilter[]): string {
  const terms = filters.flatMap((f) => {
    const facet = facetField(f.field);
    if (!facet) return [];
    const rhs = facet.quoted ? `"${escapeTraceQLString(f.value)}"` : f.value;
    return [`${facet.selector} = ${rhs}`];
  });
  return terms.length === 0 ? "" : `{ ${terms.join(" && ")} }`;
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

/** One value per field: selecting another replaces the previous choice. */
export function upsertTraceFilter(
  filters: TraceFilter[],
  next: TraceFilter,
): TraceFilter[] {
  const idx = filters.findIndex((f) => f.field === next.field);
  if (idx === -1) return [...filters, next];
  const copy = [...filters];
  copy[idx] = next;
  return copy;
}
