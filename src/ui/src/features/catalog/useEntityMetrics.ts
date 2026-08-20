/**
 * The metrics an entity type is measured by, in this window.
 *
 * Two fetches with deliberately different lifetimes. What a window holds is a
 * fact about the window, so the observed-name discovery is keyed by range.
 * Which metrics describe an entity is a fact about the registry, so that half
 * is keyed by tenant and entity — re-reading it on every range change would
 * re-fetch an identical answer.
 */
import { useQuery } from "@tanstack/react-query";
import {
  discoverObservedMetricNames,
  fetchEntityMetricNames,
  fetchMetricDefinitions,
  METRIC_SOURCES,
} from "../../api/entityMetrics";
import type { MetricHit } from "../schema/api";
import type { ResolvedRange } from "../../lib/time";
import type { EntityTypeDef } from "./entityTypes";
import { tenantScope } from "./useEntityTypes";

export interface EntityMetrics {
  /** The entity's associated metric definitions observed in this window. */
  metrics: MetricHit[];
  isPending: boolean;
  /**
   * Whether asking *failed*, as opposed to answering "none".
   *
   * Both are the same empty list, and rendering them alike is what makes a
   * broken lookup look like an entity with nothing to show.
   */
  isError: boolean;
}

export function useEntityMetrics(
  entity: EntityTypeDef,
  range: ResolvedRange,
  rangeKey: string,
): EntityMetrics {
  // An entity type no visible registry declares has no association to look
  // up, so neither fetch has a question to ask.
  const enabled = entity.registryEntity !== undefined;

  const observed = useQuery({
    queryKey: ["entity-metric-names", rangeKey],
    queryFn: async () => {
      const perSource = await Promise.all(
        Object.values(METRIC_SOURCES).map((source) =>
          discoverObservedMetricNames(source, range),
        ),
      );
      // Sorted so an equal set of names is an equal cache key: the querier
      // makes no ordering promise, and an unstable key would re-fetch the
      // definitions this hook exists to keep cached.
      return [...new Set(perSource.flat())].sort();
    },
    enabled,
    staleTime: 60_000,
  });

  const names = observed.data ?? [];
  const definitions = useQuery({
    queryKey: [
      "entity-metric-definitions",
      tenantScope(rangeKey),
      entity.registryEntity,
      names,
    ],
    queryFn: async () => {
      // The registry says which metrics describe this entity; the window says
      // which of them exist. The panel wants the intersection, and asking for
      // definitions of only that intersection keeps the prefix searches down
      // to the families the entity actually uses.
      const associated = await fetchEntityMetricNames(entity.registryEntity!);
      const inWindow = new Set(names);
      return fetchMetricDefinitions(associated.filter((n) => inWindow.has(n)));
    },
    enabled: enabled && observed.isSuccess,
    staleTime: 10 * 60_000,
  });

  return {
    metrics: definitions.data ?? [],
    isPending: enabled && (observed.isPending || definitions.isPending),
    isError: observed.isError || definitions.isError,
  };
}
