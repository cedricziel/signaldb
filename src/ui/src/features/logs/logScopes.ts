/**
 * Groups a log row's fields for the detail view into the scopes the IR keeps
 * apart (see docs/users/querying-ir.md's "Addressing an attribute scope"):
 * "This line" (the record itself — trace/span id plus log attributes),
 * "Scope" (instrumentation-scope attributes), "Resource" (resource
 * attributes, plus the promoted `service.name` field, which describes the
 * emitter same as any other resource attribute even though it isn't stored
 * in the resource attribute map). No semantic-registry lookup decides the
 * split — a key carried in the resource container is a resource field,
 * always, even if the same key also appears on the line.
 */
import type { LogRow } from "../../api/ir/logs";

export interface LogScope {
  title: string;
  entries: [string, string][];
}

function sortedEntries(bag: Record<string, string>): [string, string][] {
  return Object.entries(bag).sort(([a], [b]) => a.localeCompare(b));
}

function lineAttributes(row: LogRow): Record<string, string> {
  const bag: Record<string, string> = { ...row.logAttributes };
  if (row.traceId) bag.trace_id = row.traceId;
  if (row.spanId) bag.span_id = row.spanId;
  return bag;
}

function resourceAttributes(row: LogRow): Record<string, string> {
  const bag: Record<string, string> = { ...row.resourceAttributes };
  if (row.serviceName) bag["service.name"] = row.serviceName;
  return bag;
}

/** "This line" always renders (even empty); "Scope" only when the row
 * carried scope attributes. */
export function logScopes(row: LogRow): LogScope[] {
  return [
    { title: "This line", entries: sortedEntries(lineAttributes(row)) },
    { title: "Scope", entries: sortedEntries(row.scopeAttributes) },
    { title: "Resource", entries: sortedEntries(resourceAttributes(row)) },
  ].filter((g) => g.title === "This line" || g.entries.length > 0);
}
