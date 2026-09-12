// Shared allowed-origins UI for the API-key create/update forms
// (ApiKeys.tsx). Origins are free-form strings rather than a fixed
// enumerable set like datasets, so this is a controlled add/remove list
// (see `DatasetPicker` for the sibling, checkbox-based restriction picker).

import { useState } from "react";

/** The subset of a key/response shape needed to read its allowed-origins
 * restriction: the `allowed_origins` set, or absent/null for unrestricted. */
interface RestrictedByOrigin {
  allowed_origins?: string[] | null;
}

/** Normalize a key's allowed-origins restriction to a plain array: empty
 * means unrestricted. */
export function allowedOriginsSet(key: RestrictedByOrigin): string[] {
  return key.allowed_origins ?? [];
}

/** Display label for a key's allowed-origins restriction: the joined origin
 * list, or "Any origin" when there is none. */
export function allowedOriginsLabel(key: RestrictedByOrigin): string {
  const origins = allowedOriginsSet(key);
  return origins.length > 0 ? origins.join(", ") : "Any origin";
}

/** Free-text add/remove list of allowed origins: type an origin and press
 * Enter or click Add; remove one via its own button. An empty list means
 * "unrestricted" on create, and "leave the current restriction unchanged" on
 * update (mirrors `DatasetPicker`/D1a) — clearing an existing restriction is
 * its own explicit control (`clear_allowed_origins`), never a side effect of
 * this list being empty. `idPrefix` keeps input ids unique per form
 * instance. */
export function OriginPicker({
  idPrefix,
  origins,
  onChange,
  disabled,
}: {
  idPrefix: string;
  origins: string[];
  onChange: (origins: string[]) => void;
  disabled?: boolean;
}) {
  const [pending, setPending] = useState("");

  const add = () => {
    const value = pending.trim();
    if (!value || origins.includes(value)) {
      setPending("");
      return;
    }
    onChange([...origins, value]);
    setPending("");
  };

  return (
    <fieldset className="origin-picker">
      <legend>Allowed origins</legend>
      <p className="origin-picker-help">
        Restrict browser (CORS) ingest requests to these origins. Leave empty to
        allow any origin.
      </p>
      {origins.length > 0 && (
        <div className="origin-chips">
          {origins.map((origin) => (
            <span key={origin} className="origin-chip">
              {origin}
              <button
                type="button"
                aria-label={`Remove ${origin}`}
                onClick={() => onChange(origins.filter((o) => o !== origin))}
                disabled={disabled}
              >
                ×
              </button>
            </span>
          ))}
        </div>
      )}
      <div className="origin-add">
        <input
          id={`${idPrefix}-origin-input`}
          aria-label="Add allowed origin"
          placeholder="https://app.example.com"
          value={pending}
          disabled={disabled}
          onChange={(event) => setPending(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
        />
        <button type="button" onClick={add} disabled={disabled}>
          Add
        </button>
      </div>
    </fieldset>
  );
}
