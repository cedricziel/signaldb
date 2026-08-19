/**
 * The metrics an entity type is measured by, in this window.
 *
 * Two fetches with deliberately different lifetimes. What a window holds is a
 * fact about the window, so the observed-name discovery is keyed by range.
 * What a metric *measures* is a fact about the registry, so the definition
 * lookup is keyed by tenant and dataset only — re-reading it on every range
 * change would re-fetch an identical answer.
 *
 * See `api/entityMetrics.ts` for why the join runs data-first.
 */
import { useQuery } from "@tanstack/react-query";
import {
  discoverObservedMetricNames,
  fetchMetricDefinitions,
  METRIC_SOURCES,
  metricsForEntity,
} from "../../api/entityMetrics";
import type { MetricHit } from "../schema/api";
import type { ResolvedRange } from "../../lib/time";
import type { EntityTypeDef } from "./entityTypes";
import { tenantScope } from "./useEntityTypes";

export interface EntityMetrics {
  /** The entity's associated metric definitions observed in this window. */
  metrics: MetricHit[];
  isPending: boolean;
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
    queryKey: ["entity-metric-definitions", tenantScope(rangeKey), names],
    queryFn: () => fetchMetricDefinitions(names),
    enabled: enabled && observed.isSuccess,
    staleTime: 10 * 60_000,
  });

  return {
    metrics: metricsForEntity(definitions.data ?? [], entity),
    isPending: enabled && (observed.isPending || definitions.isPending),
  };
}
