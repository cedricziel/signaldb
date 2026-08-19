import { CATALOG_SOURCES } from "../../api/sourceFields";

// The catalog's entity registry.
//
// Every entity type is discovered and measured the same way: group its
// `sources` by its `identity` dimensions and compute RED (count, error
// count, p50, p95, last-seen) — the same aggregate the traces tab's own
// group table uses, run once per source and merged by identity (see
// `api/catalog.ts`). Nothing here is hardcoded sample data; an entity type
// with no telemetry carrying its identity attribute(s) simply returns zero
// rows — see CatalogView's empty state. Any tenant that starts sending an
// attribute (an SDK resource detector, an OTel Collector with
// `resourcedetection`, ...) gets that entity type populated with no further
// engineering, because discovery is driven by this registry, not by which
// entity types happen to have data today.
//
// Error rate and duration percentiles are trace-only concepts (span status,
// span duration) — a source with no `traces` in its `sources` list, or a
// row whose count came entirely from non-trace sources, carries no RED
// duration data; `api/catalog.ts` marks that explicitly rather than
// reporting a false "0ms" p50/p95.
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
  /**
   * A second real dimension, grouped within this entity, shown as a
   * breakdown table on its detail page — e.g. a service's inbound
   * operations, or which services were observed alongside a host. The
   * breakdown query inherits this entity type's own `spanKindScope`: an
   * operation's numbers should describe inbound requests to that endpoint,
   * same reasoning as the service level itself. Omitted where no second
   * real dimension is wired yet (nothing here is invented to fill the gap —
   * see the module doc).
   */
  breakdown?: { field: string; label: string };
  /**
   * A third dimension shown as its own read-only table (no drill-down —
   * `catalogSecondary` already belongs to `breakdown`) below it, ranked by
   * count rather than pinned to a specific value. Distinct from `breakdown`
   * because it identifies individual occurrences (e.g. a SQL statement)
   * rather than a small, stable set of categories worth drilling into.
   */
  topValues?: { field: string; label: string };
  /**
   * Query IR sources to discover and measure this entity from, merged by
   * identity (see `api/catalog.ts`). Defaults to `["traces"]` when omitted.
   * Safe for any entity whose identity is an OTel *resource* attribute
   * (`service.*`, `host.*`, `k8s.*`, `container.*`, `process.*`) — resource
   * attributes are attached to every signal an SDK emits, not just spans.
   * Not safe for an identity that is a *span* attribute (`db.namespace`,
   * `messaging.destination.name`, ...): those describe one client call, not
   * the process that made it, and have no counterpart on a log line or
   * metric point.
   */
  sources?: string[];
  /**
   * Identity to fall back to when the registry-declared one is absent from
   * the data, in registry-declared order — the entity's descriptive
   * attributes. Present only on a registry-derived type: where the catalog
   * names an identity it is deliberate (see `resolveIdentity`) and is never
   * substituted.
   */
  identityFallback?: string[];
  /**
   * The schema-registry entity this type is, by its registry name — the join
   * key to everything else the registry says about it, such as which metrics
   * measure it.
   *
   * Set by `deriveEntityTypes` for any type a registry entity matched,
   * curated or derived; absent for a curated type no visible registry
   * declares. It is carried rather than reversed out of `id`, because
   * registry names contain underscores of their own (`gcp.cloud_run`), so the
   * dots-to-underscores mapping has no safe inverse.
   */
  registryEntity?: string;
}

/**
 * The resource-scoped multi-source list (see `EntityTypeDef.sources`): every
 * signal a tenant can query, because a resource attribute rides on everything
 * an SDK emits, not just on spans.
 *
 * This is {@link CATALOG_SOURCES} under the name that reads correctly here.
 * The two were separate literals holding the same five strings, with nothing
 * keeping them equal — and they mean the same thing, so a new signal source
 * must not be able to reach one and miss the other. The API layer owns the
 * list because it is what talks to the backend.
 *
 * `metrics` and `profiles` were excluded until #1205 (no logical `timestamp`
 * field, so the "last seen" aggregate 400d) and #1206 (grouping metrics by an
 * unregistered resource attribute 500d) were fixed. Both are closed. Their
 * absence was not cosmetic: `process.pid` and `container.name` appear on
 * metrics and on no other signal in a typical deployment, so the Processes
 * and Containers pages rendered empty over data that was already stored.
 *
 * `metrics_histogram` is its own Query IR source but the same OTel signal as
 * `metrics`; both are listed because an entity reporting only histograms
 * would otherwise go undiscovered.
 */
export const RESOURCE_SOURCES = CATALOG_SOURCES;

export const ENTITY_TYPES: EntityTypeDef[] = [
  {
    id: "service",
    label: "Services",
    singular: "service",
    identity: ["service.name", "service.namespace"],
    spanKindScope: "Server",
    breakdown: { field: "span.name", label: "Operations" },
    sources: RESOURCE_SOURCES,
  },
  {
    id: "database",
    label: "Databases",
    singular: "database",
    identity: ["db.namespace", "db.system.name"],
    breakdown: { field: "db.operation.name", label: "Operations" },
    topValues: { field: "db.query.text", label: "Top statements" },
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
    breakdown: { field: "service.name", label: "Services" },
    sources: RESOURCE_SOURCES,
  },
  {
    id: "k8s_pod",
    label: "Kubernetes pods",
    singular: "pod",
    identity: ["k8s.pod.name", "k8s.namespace.name"],
    breakdown: { field: "service.name", label: "Services" },
    sources: RESOURCE_SOURCES,
  },
  {
    id: "k8s_node",
    label: "Kubernetes nodes",
    singular: "node",
    identity: ["k8s.node.name"],
    breakdown: { field: "service.name", label: "Services" },
    sources: RESOURCE_SOURCES,
  },
  {
    id: "container",
    label: "Containers",
    singular: "container",
    identity: ["container.name"],
    breakdown: { field: "service.name", label: "Services" },
    sources: RESOURCE_SOURCES,
  },
  {
    id: "process",
    label: "Processes",
    singular: "process",
    identity: ["process.pid", "host.name"],
    breakdown: { field: "service.name", label: "Services" },
    sources: RESOURCE_SOURCES,
  },
];

export const DEFAULT_ENTITY_TYPE = "service";

export function entityType(id: string): EntityTypeDef | undefined {
  return ENTITY_TYPES.find((e) => e.id === id);
}
