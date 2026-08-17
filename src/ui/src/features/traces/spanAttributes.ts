// Groups a span's flattened attribute bag for display.
//
// The trace detail (api/traceDetail.ts) flattens the three OTel attribute
// scopes into one object: span attributes as-is, instrumentation-scope
// attributes prefixed `scope.`, resource attributes prefixed `resource.`.
import type { AttrValue } from "../../api/tempo";

const RESOURCE_PREFIX = "resource.";
const SCOPE_PREFIX = "scope.";

export interface AttributeGroup {
  label: string;
  entries: [string, AttrValue][];
}

function sortedEntries(
  attrs: Record<string, AttrValue>,
): [string, AttrValue][] {
  return Object.entries(attrs).sort(([a], [b]) => a.localeCompare(b));
}

/** Span attributes first (the immediate context), then scope, then
 * resource; a scope with no entries is omitted rather than shown empty. */
export function groupSpanAttributes(
  attrs: Record<string, AttrValue>,
): AttributeGroup[] {
  const span: Record<string, AttrValue> = {};
  const scope: Record<string, AttrValue> = {};
  const resource: Record<string, AttrValue> = {};
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith(RESOURCE_PREFIX)) {
      resource[key.slice(RESOURCE_PREFIX.length)] = value;
    } else if (key.startsWith(SCOPE_PREFIX)) {
      scope[key.slice(SCOPE_PREFIX.length)] = value;
    } else {
      span[key] = value;
    }
  }

  const groups: AttributeGroup[] = [];
  for (const [label, bag] of [
    ["Span", span],
    ["Scope", scope],
    ["Resource", resource],
  ] as const) {
    const entries = sortedEntries(bag);
    if (entries.length > 0) groups.push({ label, entries });
  }
  return groups;
}

/**
 * The span-detail header line: service name plus whichever of namespace,
 * deployment environment, and version the resource attributes carry. Any
 * piece can be absent (not every producer sets all four), so each is
 * appended only when present rather than left as a blank placeholder.
 */
export function describeService(
  serviceName: string,
  attrs: Record<string, AttrValue>,
): string {
  const resource = (key: string): AttrValue | undefined =>
    attrs[`${RESOURCE_PREFIX}${key}`];

  const namespace = resource("service.namespace");
  const environment = resource("deployment.environment.name");
  const version = resource("service.version");

  let identity = serviceName;
  if (namespace != null && namespace !== "") identity += ` (${namespace})`;

  const parts = [identity];
  if (environment != null && environment !== "")
    parts.push(String(environment));
  if (version != null && version !== "") parts.push(`v${version}`);
  return parts.join(" · ");
}
