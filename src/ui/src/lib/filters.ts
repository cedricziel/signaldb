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

/** One identifier segment, shared by the PromQL and LogQL label-name shapes
 * below so they can't drift apart. */
const LABEL_SEGMENT = "[a-zA-Z_][a-zA-Z0-9_]*";

const LABEL_RE = new RegExp(`^${LABEL_SEGMENT}$`);

/** Strict Prometheus-style label name: PromQL matchers require this shape
 * (see `features/metrics/buildPromQL.ts`). */
export function isValidLabelName(name: string): boolean {
  return LABEL_RE.test(name);
}

const LOG_LABEL_RE = new RegExp(`^${LABEL_SEGMENT}(\\.[a-zA-Z0-9_]+)*$`);

/**
 * LogQL label name, spelled either as a plain identifier or dotted (an
 * attribute's real key, e.g. `k8s.pod.name`) — the querier resolves a
 * dotted label directly against the attribute maps. No leading, trailing,
 * or doubled dots.
 */
export function isValidLogLabelName(name: string): boolean {
  return LOG_LABEL_RE.test(name);
}

/**
 * LogQL requires at least one matcher in a selector; match everything via a
 * non-empty regex on service_name (always present in SignalDB log streams).
 */
export const MATCH_ALL_SELECTOR = '{service_name=~".+"}';

export function compileSelector(filters: LabelFilter[]): string {
  const valid = filters.filter((f) => isValidLogLabelName(f.label));
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
  if (!isValidLogLabelName(label)) return null;
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
