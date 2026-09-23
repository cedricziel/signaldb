/**
 * The breakdown table's per-operation rate series (see
 * `api/operationSeries.ts`) — one grouped query for the whole table.
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import {
  fetchOperationSeries,
  type SeriesByOperation,
} from "../../api/operationSeries";
import { pinsKey, type EntityPin } from "../../api/catalog";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import type { EntityTypeDef } from "./entityTypes";

const SERIES_BUCKETS = 30;

export function useOperationSeries(
  entity: EntityTypeDef,
  /** The entity type's breakdown field (`entity.breakdown.field`) — absent
   * for an entity type with no breakdown table, so the query is skipped
   * rather than asked for a group-by of nothing. */
  breakdownField: string | undefined,
  range: ResolvedRange,
  rangeKey: string,
  pinned: EntityPin[],
): UseQueryResult<SeriesByOperation> {
  const step = durationToSeconds(stepForRange(range, SERIES_BUCKETS)) ?? 60;
  return useQuery({
    queryKey: [
      "catalog-operation-series",
      entity.id,
      breakdownField,
      rangeKey,
      pinsKey(pinned),
      step,
    ],
    queryFn: () =>
      fetchOperationSeries(entity, breakdownField!, range, pinned, step),
    enabled: breakdownField !== undefined,
  });
}
