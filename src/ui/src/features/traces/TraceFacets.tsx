import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { fetchFacet, type FacetValue } from "../../api/traceFacets";
import { SemanticInfo } from "../../components/SemanticKey";
import { SidebarResizer } from "../../components/SidebarResizer";
import { useSemantics } from "../../hooks/useSemantics";
import type { ResolvedRange } from "../../lib/time";
import {
  FACET_FIELDS,
  type FacetField,
  type TraceFilter,
} from "../../lib/traceFilters";

const NUM = new Intl.NumberFormat();

/** Registry keys behind the facet headers (`FacetField.field` is the wire
 * attribute for attribute-backed facets; built-ins like `name` resolve to
 * nothing and simply carry no info glyph). */
const FACET_KEYS = FACET_FIELDS.map((f) => f.field);

/** The filter the errors-only toggle adds: the root span's OTel status. */
const ERRORS_ONLY: TraceFilter = { field: "status", value: "Error" };

interface Props {
  range: ResolvedRange;
  /** Cache scope: window plus tenant context. */
  rangeKey: string;
  filters: TraceFilter[];
  onAddFilter: (filter: TraceFilter) => void;
  onRemoveFilter: (filter: TraceFilter) => void;
}

/**
 * Facet sidebar for the traces tab.
 *
 * Only the fields the backend can enumerate exactly are offered — see #1073;
 * a guessed field list would reintroduce the row-limit sampling bias this UI
 * has removed elsewhere.
 */
export function TraceFacets({
  range,
  rangeKey,
  filters,
  onAddFilter,
  onRemoveFilter,
}: Props) {
  const [open, setOpen] = useState<string | null>(null);
  const semantics = useSemantics(FACET_KEYS);
  // "Errors only" is the status facet's Error value as a one-click toggle;
  // going through the same filter keeps groups, list, volume, and facet
  // counts in step.
  const errorsOnly = filters.some(
    (f) => f.field === ERRORS_ONLY.field && f.value === ERRORS_ONLY.value,
  );

  return (
    <aside className="sidebar" aria-label="Facets">
      <SidebarResizer />
      <div className="sidebar-head">Facets</div>
      <label className="sidebar-toggle">
        <input
          type="checkbox"
          checked={errorsOnly}
          onChange={() =>
            errorsOnly ? onRemoveFilter(ERRORS_ONLY) : onAddFilter(ERRORS_ONLY)
          }
        />
        Errors only
      </label>
      <div className="fieldlist">
        {FACET_FIELDS.map((facet) => {
          const active = filters.filter((f) => f.field === facet.field);
          return (
            <div key={facet.field}>
              <div className="field-row">
                <button
                  className={`field ${open === facet.field ? "open" : ""}`}
                  aria-expanded={open === facet.field}
                  onClick={() =>
                    setOpen(open === facet.field ? null : facet.field)
                  }
                >
                  <span>{facet.label}</span>
                  {active.length > 0 && (
                    <span className="facet-active">{active.length}</span>
                  )}
                </button>
                <SemanticInfo
                  name={facet.label}
                  semantics={semantics.get(facet.field)}
                />
              </div>
              {open === facet.field && (
                <FacetValues
                  facet={facet}
                  range={range}
                  rangeKey={rangeKey}
                  filters={filters}
                  onAddFilter={onAddFilter}
                  onRemoveFilter={onRemoveFilter}
                />
              )}
            </div>
          );
        })}
      </div>
    </aside>
  );
}

function FacetValues({
  facet,
  range,
  rangeKey,
  filters,
  onAddFilter,
  onRemoveFilter,
}: {
  facet: FacetField;
  range: ResolvedRange;
  rangeKey: string;
  filters: TraceFilter[];
  onAddFilter: (filter: TraceFilter) => void;
  onRemoveFilter: (filter: TraceFilter) => void;
}) {
  // Other facets' filters narrow the counts; this facet's own do not, so its
  // alternatives stay visible and switchable.
  const narrowing = filters.filter((f) => f.field !== facet.field);
  const result = useQuery({
    queryKey: [
      "trace-facet",
      facet.irField,
      rangeKey,
      narrowing.map((f) => `${f.field}|${f.value}`).join(","),
    ],
    queryFn: () => fetchFacet(facet.irField, range, narrowing),
    staleTime: 30_000,
  });

  if (result.isPending) return <div className="fieldvals-note">Loading…</div>;
  if (result.isError) {
    return <div className="fieldvals-note">Could not load values</div>;
  }
  if (!result.data.resolved) {
    return <div className="fieldvals-note">Values not available yet</div>;
  }
  if (result.data.values.length === 0) {
    return <div className="fieldvals-note">No values in range</div>;
  }

  const isActive = (v: FacetValue) =>
    v.value !== null &&
    filters.some((f) => f.field === facet.field && f.value === v.value);

  return (
    <div className="fieldvals">
      {result.data.values.map((v) => {
        const active = isActive(v);
        const label = v.value ?? "(none)";
        return (
          <button
            className="fieldval facet-val"
            data-testid="facet-value"
            key={label}
            // An absent value has no TraceQL equality to select on.
            disabled={v.value === null}
            aria-pressed={active}
            onClick={() => {
              if (v.value === null) return;
              const filter = { field: facet.field, value: v.value };
              (active ? onRemoveFilter : onAddFilter)(filter);
            }}
          >
            <span className="facet-val-name">{label}</span>
            <span className="facet-val-count">{NUM.format(v.count)}</span>
          </button>
        );
      })}
      {result.data.truncated && (
        <div className="fieldvals-note">+ more values</div>
      )}
    </div>
  );
}
