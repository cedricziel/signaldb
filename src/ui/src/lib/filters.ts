// Structured filter model for the logs chip UI, compiled to the Query IR
// (see api/ir/logs.ts) rather than a dialect string.

import { upsertBy } from "./collections";

export type FilterOp = "=" | "!=" | "=~" | "!~";

export interface LabelFilter {
  label: string;
  op: FilterOp;
  value: string;
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
