import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { lokiLabelValues } from "../../api/loki";
import { SemanticInfo } from "../../components/SemanticKey";
import { SidebarResizer } from "../../components/SidebarResizer";
import { sidebarWidth } from "../../lib/sidebarWidth";
import { useSemantics } from "../../hooks/useSemantics";
import type { AttributeSemantics, SemanticsMap } from "../../lib/semantics";
import type { LabelFilter } from "../../lib/filters";
import type { ResolvedRange } from "../../lib/time";
import { groupFields, type FieldGroup } from "./fieldGroups";

interface Props {
  labels: string[];
  range: ResolvedRange;
  rangeKey: string;
  onAddFilter: (filter: LabelFilter) => void;
}

/**
 * Fields panel: label names from the Loki labels endpoint, grouped by
 * semantic title once the schema registry resolves them (flat and
 * alphabetical until then), values on expand. Presence/cardinality stats
 * arrive with the native fields API.
 */
export function FieldSidebar({ labels, range, rangeKey, onAddFilter }: Props) {
  const [open, setOpen] = useState<string | null>(null);
  const [filterText, setFilterText] = useState("");
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(
    () => new Set(),
  );

  const semantics = useSemantics(labels);
  const groups = useMemo(
    () => groupFields(labels, semantics),
    [labels, semantics],
  );

  const needle = filterText.trim().toLowerCase();
  const filtering = needle.length > 0;
  const visibleGroups = filtering
    ? groups
        .map((g) => ({
          ...g,
          labels: g.title.toLowerCase().includes(needle)
            ? g.labels
            : g.labels.filter((l) => l.toLowerCase().includes(needle)),
        }))
        .filter((g) => g.labels.length > 0)
    : groups;
  // `groupFields` only ever emits the sentinel "all" group on its own.
  const flat = groups[0]?.id === "all";

  const toggleGroup = (id: string) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

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
        {visibleGroups.length === 0 && (
          <div className="fieldlist-empty">No fields</div>
        )}
        {visibleGroups.map((group) => (
          <FieldGroupSection
            key={group.id}
            group={group}
            flat={flat}
            collapsed={!filtering && collapsed.has(group.id)}
            onToggle={() => toggleGroup(group.id)}
            semantics={semantics}
            open={open}
            setOpen={setOpen}
            range={range}
            rangeKey={rangeKey}
            onAddFilter={onAddFilter}
          />
        ))}
      </div>
    </aside>
  );
}

function FieldGroupSection({
  group,
  flat,
  collapsed,
  onToggle,
  semantics,
  open,
  setOpen,
  range,
  rangeKey,
  onAddFilter,
}: {
  group: FieldGroup;
  flat: boolean;
  collapsed: boolean;
  onToggle: () => void;
  semantics: SemanticsMap;
  open: string | null;
  setOpen: (label: string | null) => void;
  range: ResolvedRange;
  rangeKey: string;
  onAddFilter: (filter: LabelFilter) => void;
}) {
  return (
    <div className={flat ? undefined : "fieldgroup"}>
      {!flat && (
        <button
          className="fieldgroup-head"
          aria-expanded={!collapsed}
          onClick={onToggle}
        >
          <span aria-hidden="true">{collapsed ? "▸" : "▾"}</span>
          {group.title}
          <span className="fieldgroup-count">{group.labels.length}</span>
        </button>
      )}
      {!collapsed &&
        group.labels.map((label) => (
          <FieldRow
            key={label}
            label={label}
            semantics={semantics.get(label)}
            open={open === label}
            onToggle={() => setOpen(open === label ? null : label)}
            range={range}
            rangeKey={rangeKey}
            onAddFilter={onAddFilter}
          />
        ))}
    </div>
  );
}

function FieldRow({
  label,
  semantics,
  open,
  onToggle,
  range,
  rangeKey,
  onAddFilter,
}: {
  label: string;
  semantics: AttributeSemantics | undefined;
  open: boolean;
  onToggle: () => void;
  range: ResolvedRange;
  rangeKey: string;
  onAddFilter: (filter: LabelFilter) => void;
}) {
  const renamedTo = semantics?.deprecated?.renamed_to;
  return (
    <div>
      <div className="field-row">
        <button
          className={`field ${open ? "open" : ""}`}
          data-known={semantics ? "" : undefined}
          aria-expanded={open}
          onClick={onToggle}
        >
          {semantics?.deprecated ? (
            <>
              <s>{label}</s>
              {renamedTo && <span className="field-dep">→ {renamedTo}</span>}
            </>
          ) : (
            label
          )}
        </button>
        <SemanticInfo name={label} semantics={semantics} />
      </div>
      {open && (
        <FieldValues
          label={label}
          range={range}
          rangeKey={rangeKey}
          onAddFilter={onAddFilter}
        />
      )}
    </div>
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
