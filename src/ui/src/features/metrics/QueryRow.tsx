// The visual metric-query builder row: metric · from · agg by · function.
// Each control is populated through the Query IR's discovery stage
// (api/ir/discovery.ts) so filters and group-by are pick-from-what-exists
// rather than typed blind. State is a MetricQuery (see buildPromQL); the
// row is fully controlled via onChange.

import { useId, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fields,
  metricNames as discoverMetricNames,
  values as discoverValues,
} from "../../api/ir/discovery";
import { FILTER_OPS, type LabelFilter } from "../../lib/filters";
import type { ResolvedRange } from "../../lib/time";
import {
  cardinalityLabel,
  indexFields,
  isHighCardinality,
  optionLabel,
} from "./cardinality";
import {
  RANGE_FNS,
  SPACE_AGGS,
  type MetricQuery,
  type RangeFn,
  type RangeFnSpec,
  type SpaceAgg,
} from "./metricQuery";

interface Props {
  query: MetricQuery;
  range: ResolvedRange;
  onChange: (next: MetricQuery) => void;
}

const rangeKey = (range: ResolvedRange) => `${range.fromMs}-${range.toMs}`;

/** Bounded, case-insensitive substring match against the discovered metric
 * names — shown as-is (unfiltered, capped) on focus with an empty input, so
 * a new user sees what's available before typing anything. */
const MAX_METRIC_SUGGESTIONS = 20;

function matchingMetrics(names: string[], typed: string): string[] {
  const needle = typed.trim().toLowerCase();
  const matches =
    needle === ""
      ? names
      : names.filter((n) => n.toLowerCase().includes(needle));
  return matches.slice(0, MAX_METRIC_SUGGESTIONS);
}

export function QueryRow({ query, range, onChange }: Props) {
  const labelList = useId();

  const metricNames = useQuery({
    queryKey: ["ir-metric-names", rangeKey(range)],
    queryFn: () => discoverMetricNames(range),
    staleTime: 60_000,
  });
  // `fields` doubles as the label-cardinality source: `DiscoveredField`
  // already carries the coverage/cardinality estimate a separate
  // `label_stats` call used to fetch.
  const labelFields = useQuery({
    queryKey: ["ir-metric-fields", rangeKey(range)],
    queryFn: () => fields("metrics", range),
    staleTime: 60_000,
  });
  const statByName = useMemo(
    () => indexFields(labelFields.data ?? []),
    [labelFields.data],
  );

  const patch = (p: Partial<MetricQuery>) => onChange({ ...query, ...p });

  // Selected group-by labels whose cardinality makes grouping risky.
  const riskyGroupBy = (query.agg?.by ?? []).filter((l) =>
    isHighCardinality(statByName.get(l)),
  );

  const setFilter = (i: number, f: LabelFilter) =>
    patch({ filters: query.filters.map((old, j) => (j === i ? f : old)) });
  const addFilter = () =>
    patch({ filters: [...query.filters, { label: "", op: "=", value: "" }] });
  const removeFilter = (i: number) =>
    patch({ filters: query.filters.filter((_, j) => j !== i) });

  return (
    <div className="qrow">
      <datalist id={labelList}>
        {(labelFields.data ?? []).map((f) => (
          <option
            key={f.name}
            value={f.name}
            label={optionLabel(statByName.get(f.name))}
          />
        ))}
      </datalist>

      <span className="qrow-ref" aria-hidden>
        {query.ref}
      </span>

      <MetricNameCombobox
        value={query.metric}
        names={(metricNames.data ?? []).map((m) => m.value)}
        onChange={(metric) => patch({ metric })}
      />

      <span className="qrow-kw">from</span>
      <div className="qrow-filters">
        {query.filters.map((f, i) => (
          <FilterEditor
            key={i}
            filter={f}
            labelListId={labelList}
            range={range}
            onChange={(next) => setFilter(i, next)}
            onRemove={() => removeFilter(i)}
          />
        ))}
        <button
          type="button"
          className="qrow-add"
          onClick={addFilter}
          aria-label="Add filter"
        >
          + filter
        </button>
      </div>

      <select
        className="qrow-agg"
        aria-label="Aggregation"
        value={query.agg?.op ?? ""}
        onChange={(e) =>
          patch({
            agg: e.target.value
              ? { op: e.target.value as SpaceAgg, by: query.agg?.by ?? [] }
              : undefined,
          })
        }
      >
        <option value="">no aggregation</option>
        {SPACE_AGGS.map((op) => (
          <option key={op} value={op}>
            {op} by
          </option>
        ))}
      </select>
      {query.agg && (
        <>
          <input
            className="qrow-groupby"
            aria-label="Group by"
            list={labelList}
            placeholder="group by (comma-separated)"
            value={query.agg.by.join(", ")}
            title={query.agg.by.join(", ")}
            onChange={(e) =>
              patch({
                agg: {
                  op: query.agg?.op ?? "sum",
                  by: e.target.value
                    .split(",")
                    .map((s) => s.trim())
                    .filter((s) => s !== ""),
                },
              })
            }
          />
          {riskyGroupBy.length > 0 && (
            <span
              className="qrow-warn"
              role="status"
              aria-label="Cardinality warning"
            >
              ⚠ high cardinality:{" "}
              {riskyGroupBy
                .map((l) => `${l} (${cardinalityLabel(statByName.get(l))})`)
                .join(", ")}
            </span>
          )}
        </>
      )}

      <select
        className="qrow-fn"
        aria-label="Function"
        value={query.range?.fn ?? ""}
        onChange={(e) =>
          patch({
            range: e.target.value
              ? { ...query.range, fn: e.target.value as RangeFn }
              : undefined,
          })
        }
      >
        <option value="">no function</option>
        {RANGE_FNS.map((fn) => (
          <option key={fn} value={fn}>
            {fn}
          </option>
        ))}
      </select>
      {query.range && (
        <RangeOptions
          range={query.range}
          onChange={(range) => patch({ range })}
        />
      )}
    </div>
  );
}

/** The metric-name box: a combobox over the discovered metric names, in the
 * same open-on-focus/filter-as-you-type/arrow-key-navigable shape as
 * `AttributeKeyInput`'s label suggestions, but plain strings — there's no
 * registry metadata to show alongside a metric name. */
function MetricNameCombobox({
  value,
  names,
  onChange,
}: {
  value: string;
  names: string[];
  onChange: (value: string) => void;
}) {
  const listId = useId();
  const [focused, setFocused] = useState(false);
  const [active, setActive] = useState(-1);
  const suggestions = useMemo(
    () => matchingMetrics(names, value),
    [names, value],
  );
  const open = focused && suggestions.length > 0;
  const activeIndex = open && active < suggestions.length ? active : -1;
  const optionId = (index: number) => `${listId}-option-${index}`;

  const pick = (name: string) => {
    onChange(name);
    setActive(-1);
    setFocused(false);
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!open) return;
    const count = suggestions.length;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActive((activeIndex + 1) % count);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((activeIndex - 1 + count) % count);
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      pick(suggestions[activeIndex]!);
    } else if (e.key === "Escape") {
      e.preventDefault();
      setActive(-1);
      setFocused(false);
    }
  };

  return (
    <span className="qrow-metric-combobox">
      <input
        className="qrow-metric"
        role="combobox"
        aria-label="Metric"
        aria-autocomplete="list"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        aria-activedescendant={
          activeIndex >= 0 ? optionId(activeIndex) : undefined
        }
        placeholder="metric"
        value={value}
        title={value}
        onChange={(e) => {
          setActive(-1);
          onChange(e.target.value);
        }}
        onFocus={() => setFocused(true)}
        onKeyDown={onKeyDown}
        // Clicking a suggestion's `onMouseDown` below prevents this blur
        // from racing the click.
        onBlur={() => {
          setFocused(false);
          setActive(-1);
        }}
      />
      {open && (
        <ul
          id={listId}
          role="listbox"
          aria-label="Metric name suggestions"
          className="chip-suggest"
        >
          {suggestions.map((name, index) => (
            <li
              key={name}
              id={optionId(index)}
              role="option"
              aria-selected={index === activeIndex}
              className="chip-suggest-item"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => pick(name)}
            >
              <span className="chip-suggest-key">{name}</span>
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}

interface FilterProps {
  filter: LabelFilter;
  labelListId: string;
  range: ResolvedRange;
  onChange: (f: LabelFilter) => void;
  onRemove: () => void;
}

function FilterEditor({
  filter,
  labelListId,
  range,
  onChange,
  onRemove,
}: FilterProps) {
  const valueList = useId();
  const values = useQuery({
    queryKey: ["ir-metric-label-values", filter.label, rangeKey(range)],
    queryFn: () => discoverValues("metrics", filter.label, range),
    enabled: filter.label !== "",
    staleTime: 60_000,
  });

  return (
    <span className="qrow-filter">
      <input
        className="qrow-filter-label"
        aria-label="Filter label"
        list={labelListId}
        placeholder="label"
        value={filter.label}
        onChange={(e) => onChange({ ...filter, label: e.target.value })}
      />
      <select
        className="qrow-filter-op"
        aria-label="Filter operator"
        value={filter.op}
        onChange={(e) =>
          onChange({ ...filter, op: e.target.value as LabelFilter["op"] })
        }
      >
        {FILTER_OPS.map((op) => (
          <option key={op} value={op}>
            {op}
          </option>
        ))}
      </select>
      <datalist id={valueList}>
        {(values.data ?? []).map((v) => (
          <option key={v.value} value={v.value} />
        ))}
      </datalist>
      <input
        className="qrow-filter-value"
        aria-label="Filter value"
        list={valueList}
        placeholder="value"
        value={filter.value}
        onChange={(e) => onChange({ ...filter, value: e.target.value })}
      />
      <button
        type="button"
        className="qrow-filter-remove"
        onClick={onRemove}
        aria-label="Remove filter"
      >
        ✕
      </button>
    </span>
  );
}

/** The window/across controls shown once a range function is selected — a
 * separate component so its handlers narrow `range` once instead of
 * asserting `query.range!` at every `onChange`. */
function RangeOptions({
  range,
  onChange,
}: {
  range: RangeFnSpec;
  onChange: (range: RangeFnSpec) => void;
}) {
  return (
    <>
      <input
        className="qrow-window"
        aria-label="Window"
        placeholder="window (default: step)"
        value={range.window ?? ""}
        title={range.window ?? ""}
        onChange={(e) =>
          onChange({
            ...range,
            window: e.target.value === "" ? undefined : e.target.value,
          })
        }
      />
      <select
        className="qrow-across"
        aria-label="Across"
        value={range.across ?? ""}
        onChange={(e) =>
          onChange({
            ...range,
            across: e.target.value ? (e.target.value as SpaceAgg) : undefined,
          })
        }
      >
        <option value="">across: sum</option>
        {SPACE_AGGS.map((op) => (
          <option key={op} value={op}>
            across: {op}
          </option>
        ))}
      </select>
    </>
  );
}
