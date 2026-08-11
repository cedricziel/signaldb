// The catalog's entity registry.
//
// Every entity type is discovered and measured the same way: group `traces`
// by its `identity` dimensions and compute RED (count, error count, p50,
// p95, last-seen) — the same aggregate the traces tab's own group table
// uses. Nothing here is hardcoded sample data; an entity type with no
// telemetry carrying its identity attribute(s) simply returns zero rows —
// see CatalogView's empty state. Any tenant that starts sending an
// attribute (an SDK resource detector, an OTel Collector with
// `resourcedetection`, ...) gets that entity type populated with no further
// engineering, because discovery is driven by this registry, not by which
// entity types happen to have data today.
export interface EntityTypeDef {
  id: string;
  /** Nav label, plural: "Services". */
  label: string;
  /** Used in empty-state copy: "No services observed...". */
  singular: string;
  /**
   * Query IR grouping dimensions forming this entity's identity. The first
   * is primary — the bold column, and what a drill-down filters on.
   */
  identity: string[];
  /**
   * Scopes the aggregate to spans of this kind. Only "service" needs this:
   * service.name is a resource attribute on every span a service's SDK
   * emits — including its own outbound calls to dependencies — so without
   * scoping to Server-kind spans, a service's request rate/p95 would mix
   * inbound-request metrics with outbound-call metrics. Other entity types
   * are naturally scoped by their identity attribute's own presence (only
   * the relevant spans carry `db.namespace`, `host.name`, etc.), so no kind
   * filter is needed there.
   */
  spanKindScope?: "Server";
}

export const ENTITY_TYPES: EntityTypeDef[] = [
  {
    id: "service",
    label: "Services",
    singular: "service",
    identity: ["service.name", "service.namespace"],
    spanKindScope: "Server",
  },
  {
    id: "database",
    label: "Databases",
    singular: "database",
    identity: ["db.namespace", "db.system.name"],
  },
  {
    id: "messaging_destination",
    label: "Message destinations",
    singular: "message destination",
    identity: ["messaging.destination.name", "messaging.system"],
  },
  {
    id: "host",
    label: "Hosts",
    singular: "host",
    identity: ["host.name"],
  },
  {
    id: "k8s_pod",
    label: "Kubernetes pods",
    singular: "pod",
    identity: ["k8s.pod.name", "k8s.namespace.name"],
  },
  {
    id: "k8s_node",
    label: "Kubernetes nodes",
    singular: "node",
    identity: ["k8s.node.name"],
  },
  {
    id: "container",
    label: "Containers",
    singular: "container",
    identity: ["container.name"],
  },
  {
    id: "process",
    label: "Processes",
    singular: "process",
    identity: ["process.pid", "host.name"],
  },
];

export const DEFAULT_ENTITY_TYPE = "service";

export function entityType(id: string): EntityTypeDef | undefined {
  return ENTITY_TYPES.find((e) => e.id === id);
}
