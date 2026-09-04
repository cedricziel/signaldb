// Facet sidebar for the Errors tab — client-side over the already-fetched
// group list (see lib/errorFacets.ts for why this needs no extra query,
// unlike the traces facet sidebar). Reuses the traces tab's facet styling
// (.fieldlist/.field/.fieldvals/.facet-val*) for visual consistency.
import { useState } from "react";
import type { ErrorGroup } from "../../api/errors";
import {
  ERROR_FACET_FIELDS,
  errorFacetLabel,
  errorFacetValueLabel,
  errorFacetValues,
  type ErrorFacetField,
  type ErrorFilter,
} from "../../lib/errorFacets";

const NUM = new Intl.NumberFormat();

interface Props {
  groups: ErrorGroup[];
  filters: ErrorFilter[];
  onAddFilter: (filter: ErrorFilter) => void;
  onRemoveFilter: (filter: ErrorFilter) => void;
}

export function ErrorFacets({
  groups,
  filters,
  onAddFilter,
  onRemoveFilter,
}: Props) {
  const [open, setOpen] = useState<ErrorFacetField | null>(null);

  return (
    <aside className="sidebar" aria-label="Facets">
      <div className="sidebar-head">Facets</div>
      <div className="fieldlist">
        {ERROR_FACET_FIELDS.map((field) => {
          const active = filters.filter((f) => f.field === field);
          return (
            <div key={field}>
              <button
                className={`field ${open === field ? "open" : ""}`}
                aria-expanded={open === field}
                onClick={() => setOpen(open === field ? null : field)}
              >
                <span>{errorFacetLabel(field)}</span>
                {active.length > 0 && (
                  <span className="facet-active">{active.length}</span>
                )}
              </button>
              {open === field && (
                <FacetValues
                  field={field}
                  groups={groups}
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
  field,
  groups,
  filters,
  onAddFilter,
  onRemoveFilter,
}: {
  field: ErrorFacetField;
  groups: ErrorGroup[];
  filters: ErrorFilter[];
  onAddFilter: (filter: ErrorFilter) => void;
  onRemoveFilter: (filter: ErrorFilter) => void;
}) {
  const values = errorFacetValues(groups, field, filters);
  if (values.length === 0) {
    return <div className="fieldvals-note">No values in this window</div>;
  }

  const isActive = (value: string) =>
    filters.some((f) => f.field === field && f.value === value);

  return (
    <div className="fieldvals">
      {values.map((v) => {
        const active = isActive(v.value);
        return (
          <button
            className="fieldval facet-val"
            key={v.value}
            aria-pressed={active}
            onClick={() => {
              const filter: ErrorFilter = { field, value: v.value };
              (active ? onRemoveFilter : onAddFilter)(filter);
            }}
          >
            <span className="facet-val-name">
              {errorFacetValueLabel(field, v.value)}
            </span>
            <span className="facet-val-count">{NUM.format(v.count)}</span>
          </button>
        );
      })}
    </div>
  );
}
