// Shared dataset-restriction UI for the API-key create/update forms
// (ApiKeys.tsx, ManagementPanel.tsx). Mirrors the checkbox-per-item
// convention `ScopePicker` already uses in ApiKeys.tsx: every checkbox
// shares `name="dataset"`, so a submitting form collects the checked set via
// `FormData.getAll("dataset")`.
import type { WhoamiDataset } from "../../api/session";

/** The subset of a key/response shape needed to read its dataset
 * restriction, however it arrived on the wire: the current `dataset_ids`
 * set when present, else the legacy single `dataset_id` projected into a
 * one-element (or empty) array — the same dual-read rule the catalog
 * applies server-side (D2). */
interface RestrictedByDataset {
  dataset_ids?: string[] | null;
  dataset_id?: string | null;
}

/** Normalize a key's dataset restriction to a plain array: empty means
 * unrestricted. */
export function restrictionSet(key: RestrictedByDataset): string[] {
  return key.dataset_ids ?? (key.dataset_id ? [key.dataset_id] : []);
}

/** Display label for a key's dataset restriction: the joined dataset names,
 * or the literal word "unrestricted" when there is none. */
export function datasetRestrictionLabel(key: RestrictedByDataset): string {
  const ids = restrictionSet(key);
  return ids.length > 0 ? ids.join(", ") : "unrestricted";
}

/** Datasets checked in a submitted form, via the shared `dataset` checkbox
 * name every `DatasetPicker` instance uses. */
export function selectedDatasetIds(data: FormData): string[] {
  return data.getAll("dataset").map(String);
}

/** Multi-select dataset picker: zero or more checkboxes, one per tenant
 * dataset. Selecting none means "unrestricted" on create, and "leave the
 * current restriction unchanged" on update (D1a) — clearing an existing
 * restriction is its own explicit control, never a side effect of this
 * picker being empty. `idPrefix` keeps input ids unique per form instance. */
export function DatasetPicker({
  idPrefix,
  datasets,
  checked,
  disabled,
}: {
  idPrefix: string;
  datasets: ReadonlyArray<WhoamiDataset>;
  checked: (datasetId: string) => boolean;
  disabled?: boolean;
}) {
  return (
    <fieldset className="dataset-picker">
      <legend>Datasets</legend>
      {datasets.map((dataset) => {
        const id = `${idPrefix}-dataset-${dataset.id}`;
        return (
          <div key={dataset.id} className="dataset-option">
            <input
              type="checkbox"
              id={id}
              name="dataset"
              value={dataset.id}
              defaultChecked={checked(dataset.id)}
              disabled={disabled}
            />
            <label htmlFor={id}>{dataset.id}</label>
          </div>
        );
      })}
    </fieldset>
  );
}
