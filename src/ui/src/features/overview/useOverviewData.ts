// React Query wiring for the Overview page: one query per card, keyed by
// the window + tenant scope (`rangeKey`) and the environment, so each card
// loads, fails and refetches on its own.

import { useQuery } from "@tanstack/react-query";
import { fetchCatalogEntities } from "../../api/catalog";
import { fetchErrorGroups } from "../../api/errors";
import { listMemberships } from "../../api/management";
import {
  deploysFromSightings,
  envPins,
  envWhere,
  ENV_FIELD,
  fetchIngestVolume,
  fetchServiceActivity,
  fetchSlowestEndpoints,
  fetchSystemKpis,
  fetchVersionSightings,
  latestVersions,
} from "../../api/overview";
import { fetchServiceGraph } from "../../api/serviceGraph";
import { values as discoverValues } from "../../api/ir/discovery";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import { NAV_SORT } from "../catalog/CatalogView";
import { entityType } from "../catalog/entityTypes";
import { toGraphView } from "../catalog/graphView";

/** Buckets per window for every series on the page. */
export const OVERVIEW_BUCKETS = 30;

export interface OverviewScope {
  range: ResolvedRange;
  /** `rangeScopeKey(state)` — the window plus tenant/dataset. */
  rangeKey: string;
  env: string;
}

export function overviewStep(range: ResolvedRange): {
  step: string;
  stepSeconds: number;
} {
  const step = stepForRange(range, OVERVIEW_BUCKETS);
  return { step, stepSeconds: durationToSeconds(step) ?? 60 };
}

const SERVICE = entityType("service")!;
const STALE = 30_000;

export function useSystemKpis({ range, rangeKey, env }: OverviewScope) {
  const { stepSeconds } = overviewStep(range);
  return useQuery({
    queryKey: ["overview-kpis", rangeKey, env, stepSeconds],
    queryFn: () => fetchSystemKpis(range, env, stepSeconds),
    staleTime: STALE,
  });
}

export function useServices({ range, rangeKey, env }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-services", rangeKey, env],
    queryFn: () => fetchCatalogEntities(SERVICE, range, NAV_SORT, envPins(env)),
    staleTime: STALE,
  });
}

export function useServiceActivity({ range, rangeKey, env }: OverviewScope) {
  const { stepSeconds } = overviewStep(range);
  return useQuery({
    queryKey: ["overview-service-activity", rangeKey, env, stepSeconds],
    queryFn: () => fetchServiceActivity(SERVICE, range, env, stepSeconds),
    staleTime: STALE,
  });
}

export function useDeploys({ range, rangeKey, env }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-deploys", rangeKey, env],
    queryFn: async () => {
      const sightings = await fetchVersionSightings(range, env);
      return {
        deploys: deploysFromSightings(sightings),
        latest: latestVersions(sightings),
      };
    },
    staleTime: STALE,
  });
}

export function useServiceMap({ range, rangeKey, env }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-service-map", rangeKey, env],
    queryFn: async () => {
      const { graph } = await fetchServiceGraph(range, {
        where: envWhere(env),
      });
      return {
        ...toGraphView(graph.nodes, graph.edges),
        external: graph.nodes.filter((n) => n.kind === "external").length,
        dropped: graph.dropped_nodes ?? 0,
        total: graph.nodes.length + (graph.dropped_nodes ?? 0),
      };
    },
    staleTime: STALE,
  });
}

export function useIngestVolume({ range, rangeKey, env }: OverviewScope) {
  const { stepSeconds } = overviewStep(range);
  return useQuery({
    queryKey: ["overview-ingest", rangeKey, env, stepSeconds],
    queryFn: () => fetchIngestVolume(range, env, stepSeconds),
    staleTime: STALE,
  });
}

export function useTopErrorGroups({ range, rangeKey, env }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-error-groups", rangeKey, env],
    queryFn: () => fetchErrorGroups(range, undefined, envWhere(env)),
    staleTime: STALE,
  });
}

export function useSlowestEndpoints({ range, rangeKey, env }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-slowest", rangeKey, env],
    queryFn: () => fetchSlowestEndpoints(range, env, 6),
    staleTime: STALE,
  });
}

/** The environments seen on spans in the window, for the picker. */
export function useEnvironments({ range, rangeKey }: OverviewScope) {
  return useQuery({
    queryKey: ["overview-envs", rangeKey],
    queryFn: async () =>
      (await discoverValues("traces", ENV_FIELD, range, 50)).map(
        (v) => v.value,
      ),
    staleTime: 5 * 60_000,
  });
}

/** Distinct members of the tenant — admin-only, so gated on `canManage`;
 * a user can hold a local and an OIDC-mapped grant at once. */
export function useMemberCount(tenant: string, canManage: boolean) {
  return useQuery({
    queryKey: ["overview-members", tenant],
    queryFn: async () =>
      new Set((await listMemberships(tenant)).map((m) => m.user_id)).size,
    enabled: canManage && tenant !== "",
    staleTime: 5 * 60_000,
    retry: false,
  });
}
