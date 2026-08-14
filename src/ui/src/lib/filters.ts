// Structured filter model and its LogQL compilation. Chips are the primary
// input; "edit as text" switches the query to a raw LogQL string that takes
// precedence until cleared.

import { escapeQuotedString, upsertBy } from "./collections";

export type FilterOp = "=" | "!=" | "=~" | "!~";

export interface LabelFilter {
  label: string;
  op: FilterOp;
  value: string;
}

export interface LogQueryModel {
  filters: LabelFilter[];
  /** Case-sensitive line-contains match, compiled to `|= "…"`. */
  search: string;
  /** Raw LogQL override; when set it wins over filters + search. */
  raw?: string;
}

export const FILTER_OPS: FilterOp[] = ["=", "!=", "=~", "!~"];

const LABEL_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function isValidLabelName(name: string): boolean {
  return LABEL_RE.test(name);
}

/**
 * LogQL requires at least one matcher in a selector; match everything via a
 * non-empty regex on service_name (always present in SignalDB log streams).
 */
export const MATCH_ALL_SELECTOR = '{service_name=~".+"}';

export function compileSelector(filters: LabelFilter[]): string {
  const valid = filters.filter((f) => isValidLabelName(f.label));
  if (valid.length === 0) return MATCH_ALL_SELECTOR;
  const matchers = valid.map(
    (f) => `${f.label}${f.op}"${escapeQuotedString(f.value)}"`,
  );
  return `{${matchers.join(", ")}}`;
}

export function compileLogQL(model: LogQueryModel): string {
  if (model.raw && model.raw.trim() !== "") return model.raw.trim();
  let q = compileSelector(model.filters);
  if (model.search.trim() !== "") {
    q += ` |= "${escapeQuotedString(model.search.trim())}"`;
  }
  return q;
}

/**
 * Histogram companion query: log volume grouped by level. Only derivable for
 * structured queries — a raw LogQL override may already be a metric query, so
 * callers skip the histogram when `raw` is set.
 */
export function compileHistogramQL(
  model: LogQueryModel,
  step: string,
): string | null {
  if (model.raw && model.raw.trim() !== "") return null;
  return `sum by (level) (count_over_time(${compileLogQL(model)} [${step}]))`;
}

/** URL serialization: one `f` param per filter, "label|op|value". */
export function filterToParam(f: LabelFilter): string {
  return `${f.label}|${f.op}|${f.value}`;
}

export function filterFromParam(param: string): LabelFilter | null {
  const m = /^([^|]+)\|(=|!=|=~|!~)\|(.*)$/.exec(param);
  if (!m) return null;
  const label = m[1] ?? "";
  if (!isValidLabelName(label)) return null;
  return { label, op: m[2] as FilterOp, value: m[3] ?? "" };
}

/** Add or replace: an `=` filter on a label replaces an existing `=` filter. */
export function upsertFilter(
  filters: LabelFilter[],
  next: LabelFilter,
): LabelFilter[] {
  return upsertBy(
    filters,
    next,
    (f) => f.label === next.label && f.op === next.op,
  );
}
