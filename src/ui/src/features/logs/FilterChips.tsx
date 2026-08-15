import { useId, useMemo, useState } from "react";
import { useAttributeSearch } from "../../hooks/useSemantics";
import {
  FILTER_OPS,
  isValidLabelName,
  type FilterOp,
  type LabelFilter,
} from "../../lib/filters";
import { mergeLabelSuggestions } from "../../lib/labelSuggestions";

interface Props {
  filters: LabelFilter[];
  /** Known label names for the add-filter form; free text is also allowed. */
  labels: string[];
  onChange: (filters: LabelFilter[]) => void;
}

export function FilterChips({ filters, labels, onChange }: Props) {
  const [adding, setAdding] = useState(false);
  const [label, setLabel] = useState("");
  const [op, setOp] = useState<FilterOp>("=");
  const [value, setValue] = useState("");

  const submit = () => {
    if (!isValidLabelName(label)) return;
    onChange([...filters, { label, op, value }]);
    setLabel("");
    setOp("=");
    setValue("");
    setAdding(false);
  };

  return (
    <div className="chips" role="group" aria-label="Filters">
      {filters.map((f, i) => (
        <span className="chip" key={`${f.label}-${f.op}-${f.value}-${i}`}>
          <span className="chip-k">{f.label}</span>
          <span className="chip-op">{f.op}</span>
          <span className="chip-v">{f.value}</span>
          <button
            className="chip-x"
            aria-label={`Remove filter ${f.label} ${f.op} ${f.value}`}
            onClick={() => onChange(filters.filter((_, idx) => idx !== i))}
          >
            ×
          </button>
        </span>
      ))}
      {adding ? (
        <form
          className="chip-form"
          onSubmit={(e) => {
            e.preventDefault();
            submit();
          }}
        >
          <LabelInput value={label} labels={labels} onChange={setLabel} />
          <select
            aria-label="Filter operator"
            value={op}
            onChange={(e) => setOp(e.target.value as FilterOp)}
          >
            {FILTER_OPS.map((o) => (
              <option key={o} value={o}>
                {o}
              </option>
            ))}
          </select>
          <input
            aria-label="Filter value"
            placeholder="value"
            value={value}
            onChange={(e) => setValue(e.target.value)}
          />
          <button type="submit">Add</button>
          <button type="button" onClick={() => setAdding(false)}>
            Cancel
          </button>
        </form>
      ) : (
        <button className="add-filter" onClick={() => setAdding(true)}>
          + filter
        </button>
      )}
    </div>
  );
}

/**
 * Key input with suggestions: registry prefix hits (key, brief, namespace)
 * merged with the observed label list; observed-but-unregistered keys stay
 * suggestible without a brief. Free text is always accepted.
 */
function LabelInput({
  value,
  labels,
  onChange,
}: {
  value: string;
  labels: string[];
  onChange: (label: string) => void;
}) {
  const listId = useId();
  const [picked, setPicked] = useState<string | null>(null);
  const hits = useAttributeSearch(value);
  const suggestions = useMemo(
    () => mergeLabelSuggestions(value, hits, labels),
    [value, hits, labels],
  );
  const open =
    value.trim() !== "" && picked !== value && suggestions.length > 0;

  return (
    <span className="chip-label">
      <input
        role="combobox"
        aria-label="Filter label"
        aria-autocomplete="list"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        placeholder="label"
        value={value}
        autoFocus
        onChange={(e) => {
          setPicked(null);
          onChange(e.target.value);
        }}
      />
      {open && (
        <ul
          id={listId}
          role="listbox"
          aria-label="Label suggestions"
          className="chip-suggest"
        >
          {suggestions.map((s) => (
            <li
              key={s.key}
              role="option"
              aria-selected={false}
              data-key={s.key}
              className="chip-suggest-item"
              // Mouse down would blur the input before click lands.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => {
                setPicked(s.key);
                onChange(s.key);
              }}
            >
              <span className="chip-suggest-head">
                <span className="chip-suggest-key">{s.key}</span>
                {s.namespace && (
                  <span className="chip-suggest-ns">{s.namespace}</span>
                )}
                {s.seen && <span className="chip-suggest-seen">● seen</span>}
              </span>
              {s.brief && <span className="chip-suggest-brief">{s.brief}</span>}
            </li>
          ))}
        </ul>
      )}
    </span>
  );
}
