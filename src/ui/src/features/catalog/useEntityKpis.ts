/**
 * KPI stats + sparkline series for the entity detail page's card row — see
 * `api/entityDetailStats.ts` for the query/decode shape.
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { fetchEntityKpis, type EntityKpis } from "../../api/entityDetailStats";
import { pinsKey, type EntityPin } from "../../api/catalog";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import type { EntityTypeDef } from "./entityTypes";

/** Buckets per sparkline — enough shape for a card-sized chart without an
 * oversized aggregate. */
const KPI_SERIES_BUCKETS = 30;

export function useEntityKpis(
  entity: EntityTypeDef,
  range: ResolvedRange,
  rangeKey: string,
  pinned: EntityPin[],
): UseQueryResult<EntityKpis> {
  const step = durationToSeconds(stepForRange(range, KPI_SERIES_BUCKETS)) ?? 60;
  return useQuery({
    queryKey: [
      "catalog-entity-kpis",
      entity.id,
      rangeKey,
      pinsKey(pinned),
      step,
    ],
    queryFn: () => fetchEntityKpis(entity, range, pinned, step),
  });
}
