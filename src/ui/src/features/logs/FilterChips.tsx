import { useState } from "react";
import { AttributeKeyInput } from "../../components/AttributeKeyInput";
import {
  FILTER_OPS,
  isValidLabelName,
  type FilterOp,
  type LabelFilter,
} from "../../lib/filters";
import { toLokiLabel } from "../../lib/labelSuggestions";

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

  const labelValid = isValidLabelName(label);

  const submit = () => {
    if (!labelValid) return;
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
          <AttributeKeyInput
            value={label}
            onChange={setLabel}
            onPick={(key) => setLabel(toLokiLabel(key))}
            observed={labels}
            ariaLabel="Filter label"
            placeholder="label"
            autoFocus
          />
          {label !== "" && !labelValid && (
            <span className="chip-form-hint" role="status">
              use letters, digits and _
            </span>
          )}
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
          <button
            type="submit"
            className="btn btn-primary"
            disabled={!labelValid}
          >
            Add
          </button>
          <button type="button" className="btn" onClick={() => setAdding(false)}>
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
