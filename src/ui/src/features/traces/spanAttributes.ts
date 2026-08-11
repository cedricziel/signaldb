// Groups a span's flattened attribute bag for display.
//
// The Tempo-API wire format flattens two OTel attribute levels into one
// object: span attributes as-is, resource attributes prefixed `resource.`
// (see router's endpoints/tempo.rs). There is no third, scope-level group in
// today's data — the internal span model carries no scope attributes at all,
// so a "Scope" bucket would always be empty; omitted rather than shown empty.
import type { AttrValue } from "../../api/tempo";

const RESOURCE_PREFIX = "resource.";

export interface AttributeGroup {
  label: string;
  entries: [string, AttrValue][];
}

function sortedEntries(
  attrs: Record<string, AttrValue>,
): [string, AttrValue][] {
  return Object.entries(attrs).sort(([a], [b]) => a.localeCompare(b));
}

/** Span attributes first (the immediate context), then resource attributes. */
export function groupSpanAttributes(
  attrs: Record<string, AttrValue>,
): AttributeGroup[] {
  const span: Record<string, AttrValue> = {};
  const resource: Record<string, AttrValue> = {};
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith(RESOURCE_PREFIX)) {
      resource[key.slice(RESOURCE_PREFIX.length)] = value;
    } else {
      span[key] = value;
    }
  }

  const groups: AttributeGroup[] = [];
  const spanEntries = sortedEntries(span);
  if (spanEntries.length > 0)
    groups.push({ label: "Span", entries: spanEntries });
  const resourceEntries = sortedEntries(resource);
  if (resourceEntries.length > 0) {
    groups.push({ label: "Resource", entries: resourceEntries });
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
