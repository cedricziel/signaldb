// Generic client-side sortable-table helpers, shared by any view that
// renders a sortable `<table>` (trace groups/members, the catalog entity
// table, ...). Sorting itself may be server-side (re-fetch on sort change)
// or client-side (`sortRows` over an already-fetched page) — this module
// only owns the `SortSpec`/toggle state and the header control, not how a
// caller reacts to a sort change.
import { useState } from "react";

export type SortDir = "asc" | "desc";

export interface SortSpec {
  key: string;
  dir: SortDir;
}

export function useSort(defaultKey: string, defaultDir: SortDir) {
  const [sort, setSort] = useState<SortSpec>({
    key: defaultKey,
    dir: defaultDir,
  });
  const toggle = (key: string, firstDir: SortDir) =>
    setSort((s) =>
      s.key === key
        ? { key, dir: s.dir === "asc" ? "desc" : "asc" }
        : { key, dir: firstDir },
    );
  return [sort, toggle] as const;
}

export type SortValue = string | number | bigint;

export function compareValues(a: SortValue, b: SortValue): number {
  if (typeof a === "string" || typeof b === "string") {
    return String(a).localeCompare(String(b));
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

export function sortRows<T>(
  rows: T[],
  sort: SortSpec,
  value: (row: T, key: string) => SortValue,
): T[] {
  const sign = sort.dir === "asc" ? 1 : -1;
  return [...rows].sort(
    (a, b) => sign * compareValues(value(a, sort.key), value(b, sort.key)),
  );
}

export function SortTh({
  label,
  sortKey,
  sort,
  toggle,
  numeric = false,
  firstDir,
}: {
  label: string;
  sortKey: string;
  sort: SortSpec;
  toggle: (key: string, firstDir: SortDir) => void;
  /** Right-aligned metric column; sorts descending on first click. */
  numeric?: boolean;
  /** Overrides the first-click direction (e.g. timestamps: newest first). */
  firstDir?: SortDir;
}) {
  const active = sort.key === sortKey;
  return (
    <th
      className={numeric ? "num" : undefined}
      aria-sort={
        active ? (sort.dir === "asc" ? "ascending" : "descending") : undefined
      }
    >
      <button
        className="th-sort"
        onClick={() => toggle(sortKey, firstDir ?? (numeric ? "desc" : "asc"))}
      >
        {label}
      </button>
    </th>
  );
}
