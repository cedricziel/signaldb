/**
 * Facet definitions and TraceQL compilation for the traces tab.
 *
 * The facet list is deliberately limited to the fields the backend can
 * enumerate exactly. `/api/search/tags` is a hardcoded three-name list and
 * tag-value lookup returns 501 for every attribute (#1073), so offering more
 * would mean guessing a field list from the row-limited search response — the
 * sampling bias this UI has just finished removing from its charts.
 */

export interface TraceFilter {
  field: string;
  value: string;
}

export interface FacetField {
  /** Tempo tag name, and the filter's stored field. */
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
