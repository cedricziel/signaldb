/**
 * The entity types this tenant's telemetry actually carries.
 *
 * Two metadata fetches, neither of which reads signal data: the registry's
 * entity definitions, and each source's field list. Their intersection is
 * the catalog's nav (see `deriveEntityTypes`). Both are cached well beyond a
 * time-range change — a registry does not move, and field metadata is
 * maintained by compaction rather than by the window under inspection.
 */
import { useQuery } from "@tanstack/react-query";
import {
  fetchAllSourceFields,
  type SourceFields,
} from "../../api/sourceFields";
import { searchEntities } from "../schema/api";
import type { ResolvedRange } from "../../lib/time";
import {
  deriveEntityTypes,
  observedEntityTypes,
  type RegistryEntity,
} from "./deriveEntityTypes";
import { ENTITY_TYPES, type EntityTypeDef } from "./entityTypes";

export interface CatalogEntityTypes {
  types: EntityTypeDef[];
  isPending: boolean;
  /**
   * Whether any source reported maintained metadata. False means the answer
   * is "we have not looked yet", not "there is nothing here" — the two must
   * not render alike (see `SourceFields.analyzed`).
   */
  analyzed: boolean;
  /** Oldest `as_of` across the sources that answered, if any reported one. */
  asOf?: string;
}

/** The registry hit shape, narrowed to what derivation needs. `identifying`
 * is optional on the wire for an entity that declares none. */
interface EntityHitLike {
  name: string;
  identifying?: { key: string }[];
  descriptive?: { key: string }[];
}

export function toRegistryEntities(hits: EntityHitLike[]): RegistryEntity[] {
  return hits.map((h) => ({
    name: h.name,
    identifying: (h.identifying ?? []).map((a) => a.key),
    descriptive: (h.descriptive ?? []).map((a) => a.key),
  }));
}

/** Drops the range from a `rangeScopeKey` (`range|tenant|dataset`), leaving
 * the tenant and dataset a range-independent query should key on. */
export function tenantScope(rangeKey: string): string {
  return rangeKey.split("|").slice(1).join("|");
}

/** The oldest stamp any source reported — the honest age of the whole
 * answer, since one stale source makes the merged view that stale. */
function oldestAsOf(fields: Map<string, SourceFields>): string | undefined {
  const stamps = [...fields.values()]
    .map((f) => f.asOf)
    .filter((s): s is string => s !== undefined);
  return stamps.length > 0 ? stamps.sort()[0] : undefined;
}

export function useCatalogEntityTypes(
  range: ResolvedRange,
  rangeKey: string,
): CatalogEntityTypes {
  const registry = useQuery({
    queryKey: ["catalog-registry-entities"],
    queryFn: () => searchEntities(),
    staleTime: 10 * 60_000,
  });

  // Keyed on tenant and dataset but NOT on the time range: field metadata is
  // maintained by compaction, and the describe call reports itself as not
  // window-scoped, so the answer is the same whichever window is selected.
  // Keying it on the range would refetch all five sources every time someone
  // touches the range picker, for an identical result.
  const fields = useQuery({
    queryKey: ["catalog-source-fields", tenantScope(rangeKey)],
    queryFn: () => fetchAllSourceFields(range),
    staleTime: 60_000,
  });

  const isPending = registry.isPending || fields.isPending;
  const bySource = fields.data ?? new Map<string, SourceFields>();
  const analyzed = [...bySource.values()].some((f) => f.analyzed);
  const asOf = oldestAsOf(bySource);

  // No source reported any metadata — the fetch has not landed, nothing has
  // been compacted yet, or the describe calls failed. Either way we know
  // nothing about what is present, and filtering on that emptiness would
  // delete every entity type from the nav and report a working deployment as
  // having none. Fall back to the curated list and say it is unanalyzed.
  if (!analyzed) {
    return { types: ENTITY_TYPES, isPending, analyzed: false, asOf };
  }

  const derived = deriveEntityTypes(
    toRegistryEntities(registry.data ?? []),
    ENTITY_TYPES,
  );
  const fieldsBySource = new Map(
    [...bySource].map(([source, f]) => [source, f.fields]),
  );

  return {
    types: observedEntityTypes(derived, fieldsBySource),
    isPending,
    analyzed,
    asOf,
  };
}
