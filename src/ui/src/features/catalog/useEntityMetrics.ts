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
  fetchEntityActivity,
  fetchEntitySparklines,
  headlineMetric,
} from "../../api/entitySparkline";
import {
  discoverObservedMetricNames,
  fetchEntityMetricNames,
  fetchMetricDefinitions,
  METRIC_SOURCES,
} from "../../api/entityMetrics";
import type { MetricHit } from "../schema/api";
import type { IrSeries } from "../../api/entityMetrics";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import type { EntityTypeDef } from "./entityTypes";
import { tenantScope } from "./useEntityTypes";

/** Points per sparkline — enough shape to read a trend at cell size. */
const SPARKLINE_POINTS = 40;

export interface SparklineColumn {
  /** What the column charts, for its header. Absent means no column. */
  label?: string;
  byRow: Map<string, IrSeries>;
}

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
  /** Callers that cannot show metrics skip the fetches entirely. */
  wanted = true,
): EntityMetrics {
  // An entity type no visible registry declares has no association to look
  // up, so neither fetch has a question to ask.
  const enabled = wanted && entity.registryEntity !== undefined;

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

/**
 * The entity list's sparkline column: which metric it charts, and each row's
 * series.
 *
 * One hook rather than two data-fetching primitives at the call site, and an
 * explicit `wanted` rather than an inferred guard — `EntityTable` is shared
 * with the breakdown and top-values tables, whose synthetic entity types
 * happen to carry no registry name today. That is a coincidence in another
 * file, not a contract, so the caller says whether it wants the column.
 */
export function useSparklineColumn(
  entity: EntityTypeDef,
  range: ResolvedRange,
  rangeKey: string,
  wanted: boolean,
): SparklineColumn {
  const { metrics, isPending } = useEntityMetrics(
    entity,
    range,
    rangeKey,
    wanted,
  );
  const headline = wanted ? headlineMetric(metrics) : undefined;
  // Fall back only once the lookup has answered: charting activity while the
  // metric is still arriving would swap the column out from under the reader.
  const activity = wanted && !isPending && headline === undefined;

  const step = durationToSeconds(stepForRange(range, SPARKLINE_POINTS)) ?? 60;
  const series = useQuery({
    queryKey: [
      "entity-sparklines",
      entity.id,
      rangeKey,
      headline?.name ?? (activity ? "activity" : undefined),
    ],
    queryFn: () =>
      headline
        ? fetchEntitySparklines(headline, entity.identity, range, step)
        : fetchEntityActivity(entity, range, step),
    enabled: headline !== undefined || activity,
  });

  return {
    // The header names what is drawn, so a count is never mistaken for the
    // metric an entity type with associations would have shown.
    label: headline?.name ?? (activity ? activityLabel(entity) : undefined),
    byRow: series.data ?? new Map(),
  };
}

/** What the activity column is counting, in the reader's terms. */
function activityLabel(entity: EntityTypeDef): string {
  const sources = entity.sources ?? ["traces"];
  const source = sources.includes("traces") ? "traces" : sources[0]!;
  return source === "traces" ? "spans" : `${source} points`;
}
