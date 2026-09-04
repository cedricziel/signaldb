import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { lokiLabelValues } from "../../api/loki";
import { SemanticInfo } from "../../components/SemanticKey";
import { SidebarResizer } from "../../components/SidebarResizer";
import { sidebarWidth } from "../../lib/sidebarWidth";
import { useSemantics } from "../../hooks/useSemantics";
import type { LabelFilter } from "../../lib/filters";
import type { ResolvedRange } from "../../lib/time";

interface Props {
  labels: string[];
  range: ResolvedRange;
  rangeKey: string;
  onAddFilter: (filter: LabelFilter) => void;
}

/**
 * Fields panel v1: label names from the Loki labels endpoint, values on
 * expand. Presence/cardinality stats arrive with the native fields API.
 */
export function FieldSidebar({ labels, range, rangeKey, onAddFilter }: Props) {
  const [open, setOpen] = useState<string | null>(null);
  const [filterText, setFilterText] = useState("");

  const visible = labels.filter((l) =>
    l.toLowerCase().includes(filterText.toLowerCase()),
  );
  const semantics = useSemantics(labels);

  return (
    <aside className="sidebar" aria-label="Fields">
      <SidebarResizer panel={sidebarWidth} />
      <div className="sidebar-head">Fields</div>
      <input
        type="search"
        className="sidebar-search"
        placeholder="Filter fields…"
        aria-label="Filter fields"
        value={filterText}
        onChange={(e) => setFilterText(e.target.value)}
      />
      <div className="fieldlist">
        {visible.length === 0 && (
          <div className="fieldlist-empty">No fields</div>
        )}
        {visible.map((label) => (
          <div key={label}>
            <div className="field-row">
              <button
                className={`field ${open === label ? "open" : ""}`}
                aria-expanded={open === label}
                onClick={() => setOpen(open === label ? null : label)}
              >
                {label}
              </button>
              <SemanticInfo name={label} semantics={semantics.get(label)} />
            </div>
            {open === label && (
              <FieldValues
                label={label}
                range={range}
                rangeKey={rangeKey}
                onAddFilter={onAddFilter}
              />
            )}
          </div>
        ))}
      </div>
    </aside>
  );
}

function FieldValues({
  label,
  range,
  rangeKey,
  onAddFilter,
}: {
  label: string;
  range: ResolvedRange;
  rangeKey: string;
  onAddFilter: (filter: LabelFilter) => void;
}) {
  const { data, isPending, isError } = useQuery({
    queryKey: ["loki-label-values", label, rangeKey],
    queryFn: () => lokiLabelValues(label, range),
    staleTime: 30_000,
  });

  if (isPending) return <div className="fieldvals-note">Loading…</div>;
  if (isError)
    return <div className="fieldvals-note">Could not load values</div>;
  if (!data || data.length === 0)
    return <div className="fieldvals-note">No values in this window</div>;

  return (
    <div className="fieldvals">
      {data.map((value) => (
        <button
          key={value}
          className="fieldval"
          title={`Add filter ${label} = ${value}`}
          onClick={() => onAddFilter({ label, op: "=", value })}
        >
          {value}
        </button>
      ))}
    </div>
  );
}
