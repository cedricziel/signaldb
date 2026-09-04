import { useId, useMemo, useState } from "react";
import { useAttributeSearch } from "../hooks/useSemantics";
import { mergeLabelSuggestions } from "../lib/labelSuggestions";

/**
 * Attribute/label key combobox: registry prefix hits (key, brief, namespace)
 * merged with the caller's observed keys, so an observed-but-unregistered
 * key remains suggestible without a description. Free text is always
 * accepted. Shared by the logs filter-chip label input (`FilterChips`) and
 * the traces "group by attribute" input (`TracesView`).
 */
export function AttributeKeyInput({
  value,
  onChange,
  onPick,
  observed,
  ariaLabel,
  placeholder,
  autoFocus,
}: {
  value: string;
  onChange: (value: string) => void;
  onPick: (key: string) => void;
  observed: string[];
  ariaLabel: string;
  placeholder?: string;
  autoFocus?: boolean;
}) {
  const listId = useId();
  const [picked, setPicked] = useState<string | null>(null);
  // Index of the keyboard-highlighted suggestion; -1 when none. Reset on
  // every edit so it never points at a suggestion from a previous list.
  const [active, setActive] = useState(-1);
  const hits = useAttributeSearch(value);
  const suggestions = useMemo(
    () => mergeLabelSuggestions(value, hits, observed),
    [value, hits, observed],
  );
  const open =
    value.trim() !== "" && picked !== value && suggestions.length > 0;
  const activeIndex = open && active < suggestions.length ? active : -1;
  const optionId = (index: number) => `${listId}-option-${index}`;

  const pick = (key: string) => {
    setPicked(key);
    setActive(-1);
    onPick(key);
  };

  // Arrow keys move the highlight, Enter picks it, Escape closes the list.
  // Enter with nothing highlighted is left to the surrounding form, so a
  // typed key still submits the way it did before suggestions existed.
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
      pick(suggestions[activeIndex]!.key);
    } else if (e.key === "Escape") {
      e.preventDefault();
      setPicked(value);
      setActive(-1);
    }
  };

  return (
    <span className="chip-label">
      <input
        role="combobox"
        aria-label={ariaLabel}
        aria-autocomplete="list"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        aria-activedescendant={
          activeIndex >= 0 ? optionId(activeIndex) : undefined
        }
        placeholder={placeholder}
        value={value}
        autoFocus={autoFocus}
        onChange={(e) => {
          setPicked(null);
          setActive(-1);
          onChange(e.target.value);
        }}
        onKeyDown={onKeyDown}
      />
      {open && (
        <ul
          id={listId}
          role="listbox"
          aria-label="Attribute key suggestions"
          className="chip-suggest"
        >
          {suggestions.map((s, index) => (
            <li
              key={s.key}
              id={optionId(index)}
              role="option"
              aria-selected={index === activeIndex}
              data-key={s.key}
              className="chip-suggest-item"
              // Mouse down would blur the input before click lands.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => pick(s.key)}
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
