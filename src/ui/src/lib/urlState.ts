// Every explore view is a URL: the signal lives in the path (/logs, /traces,
// ...) so views are separately navigable and bookmarkable; time range,
// filters, and other options live in search params alongside it.

import { useCallback } from "react";
import { useLocation, useNavigate, useParams } from "react-router";
import { DEFAULT_SCALE, isScale, type Scale } from "../features/explore/scale";
import { DEFAULT_ENTITY_TYPE } from "../features/catalog/entityTypes";
import { filterFromParam, filterToParam, type LabelFilter } from "./filters";
import {
  traceFilterFromParam,
  traceFilterToParam,
  traceFiltersForUrl,
  type TraceFilter,
} from "./traceFilters";
import { DEFAULT_GROUP_BY, KEY_SEP } from "./traceGroups";
import type { GroupGrain } from "../api/traceGroups";

/** A group row counts whole traces unless the URL says otherwise. */
export const DEFAULT_GRAIN: GroupGrain = "traces";
import {
  DEFAULT_RANGE,
  durationToSeconds,
  parseRangeParam,
  rangeToParam,
  type TimeRange,
} from "./time";

export type Signal =
  "logs" | "traces" | "metrics" | "profiles" | "query" | "catalog" | "errors";

export interface ExploreState {
  signal: Signal;
  range: TimeRange;
  filters: LabelFilter[];
  search: string;
  raw: string;
  limit: number;
  live: boolean;
  /** Vertical scale for the volume charts. */
  scale: Scale;
  /** Chart bucket width; "" means pick one automatically for the range. */
  step: string;
  /** Facet selections on the traces tab, compiled to a TraceQL selector. */
  traceFilters: TraceFilter[];
  /** Selected trace id — opens the trace view. */
  trace: string;
  /** Selected trace group value — dives into that group's trace list. */
  group: string;
  /** Trace grouping dimension: "span.name", "service.name", or an attribute key. */
  groupBy: string;
  /**
   * What one group row counts: whole traces (root spans only, end-to-end
   * duration) or every matching span. The two answer different questions from
   * the same window, so a shared link must carry it.
   */
  grain: GroupGrain;
  /** PromQL expression for the metrics view. */
  promql: string;
  /** Profile type id (e.g. `cpu:nanoseconds`) — "" auto-picks the first. */
  profileType: string;
  /** Service filter for the profiles view — "" means all services. */
  profileService: string;
  /** Extra attribute matcher for the profiles view: a bare attribute key,
   * "" means unset. Paired with profileMatcherValue. */
  profileMatcherLabel: string;
  profileMatcherValue: string;
  /** Whether the profiles view shows a baseline-vs-comparison diff instead
   * of a single flamegraph; `range` is the comparison window. */
  profileCompare: boolean;
  /** Baseline (left) window for the diff view. */
  profileBaseline: TimeRange;
  /** Exact profile id to render (from a trace's linked-profile action) —
   * "" means the normal service/type-filtered view. */
  profileId: string;
  /**
   * Explicit tenant/dataset context. Empty means "ambient default": the dev
   * proxy (or a future session) supplies it and no header is sent.
   */
  tenant: string;
  dataset: string;
  /** Selected entity type on the catalog tab (an `EntityTypeDef.id`). */
  catalogEntity: string;
  /**
   * Composite key (see `lib/traceGroups.ts`'s `compositeKey`) of the
   * catalog entity drilled into — "" means the list view. Encodes the
   * entity type's `identity` values.
   */
  catalogPrimary: string;
  /**
   * Composite key of the breakdown row drilled into, within `catalogPrimary`
   * — "" means no further drill. Only meaningful when the selected entity
   * type has a `breakdown`; encodes that single dimension's value.
   */
  catalogSecondary: string;
}

export const DEFAULT_STATE: ExploreState = {
  signal: "logs",
  range: DEFAULT_RANGE,
  filters: [],
  search: "",
  raw: "",
  limit: 500,
  live: false,
  scale: DEFAULT_SCALE,
  step: "",
  traceFilters: [],
  trace: "",
  group: "",
  groupBy: DEFAULT_GROUP_BY,
  grain: DEFAULT_GRAIN,
  promql: "",
  profileType: "",
  profileService: "",
  profileMatcherLabel: "",
  profileMatcherValue: "",
  profileCompare: false,
  profileBaseline: DEFAULT_RANGE,
  profileId: "",
  tenant: "",
  dataset: "",
  catalogEntity: DEFAULT_ENTITY_TYPE,
  catalogPrimary: "",
  catalogSecondary: "",
};

export const SIGNALS: Signal[] = [
  "logs",
  "traces",
  "metrics",
  "profiles",
  "query",
  "catalog",
  "errors",
];

/** Maps a `:signal` route param to a known signal, defaulting invalid/missing values to "logs". */
export function signalFromParam(value: string | undefined): Signal {
  return value && SIGNALS.includes(value as Signal)
    ? (value as Signal)
    : "logs";
}

/**
 * Builds the path segment for a signal, and — for traces — an open trace.
 * Single-trace view is a real route (`/traces/:traceId`), not a `?trace=`
 * param: distinct URLs for the list and a trace mean the browser's
 * Back/Forward and the signal nav tabs (which always target the bare
 * `/traces` list path — see `crossSignalSearch`) act like real navigation
 * instead of a no-op within one path.
 */
export function buildPath(
  signal: Signal,
  trace: string,
  catalog?: Pick<
    ExploreState,
    "catalogEntity" | "catalogPrimary" | "catalogSecondary"
  >,
): string {
  if (signal === "traces" && trace !== "") {
    return `/traces/${encodeURIComponent(trace)}`;
  }
  if (signal === "catalog" && catalog) {
    const { catalogEntity, catalogPrimary, catalogSecondary } = catalog;
    // Bare /catalog is the default type's list; once anything more specific
    // is selected the entity type is always spelled out.
    if (catalogPrimary === "" && catalogEntity === DEFAULT_ENTITY_TYPE) {
      return "/catalog";
    }
    let path = `/catalog/${encodeURIComponent(catalogEntity)}`;
    if (catalogPrimary !== "") {
      path += `/${encodeCatalogSegment(catalogPrimary)}`;
      if (catalogSecondary !== "") {
        path += `/${encodeCatalogSegment(catalogSecondary)}`;
      }
    }
    return path;
  }
  return `/${signal}`;
}

/**
 * A catalog identity (a `compositeKey` of one or more dimension values) as one
 * URL path segment: each value percent-encoded, joined by `,`. Because
 * `encodeURIComponent` encodes `,` and `/`, splitting the raw segment on `,`
 * is unambiguous even for values containing either.
 */
export function encodeCatalogSegment(compositeKey: string): string {
  return compositeKey.split(KEY_SEP).map(encodeURIComponent).join(",");
}

/** Reverse of {@link encodeCatalogSegment}; expects the raw (undecoded) segment. */
export function decodeCatalogSegment(segment: string): string {
  return segment
    .split(",")
    .map((part) => {
      try {
        return decodeURIComponent(part);
      } catch {
        return part;
      }
    })
    .join(KEY_SEP);
}

/**
 * The catalog selection encoded in a pathname (`/catalog[/:entity[/:primary
 * [/:secondary]]]`). Reads the raw pathname rather than router params so the
 * comma-joined segments are split before percent-decoding.
 */
export function parseCatalogPath(
  pathname: string,
): Pick<ExploreState, "catalogEntity" | "catalogPrimary" | "catalogSecondary"> {
  const [, first, entity, primary, secondary] = pathname.split("/");
  if (first !== "catalog") {
    return {
      catalogEntity: DEFAULT_ENTITY_TYPE,
      catalogPrimary: "",
      catalogSecondary: "",
    };
  }
  let catalogEntity = DEFAULT_ENTITY_TYPE;
  if (entity) {
    try {
      catalogEntity = decodeURIComponent(entity) || DEFAULT_ENTITY_TYPE;
    } catch {
      catalogEntity = entity;
    }
  }
  return {
    catalogEntity,
    catalogPrimary: primary ? decodeCatalogSegment(primary) : "",
    catalogSecondary: secondary ? decodeCatalogSegment(secondary) : "",
  };
}

/**
 * Parses the search-param half of explore state — everything but the
 * signal and the selected trace id, both of which come from the route path
 * (see {@link signalFromParam} and {@link buildPath}: single-trace view is
 * `/traces/:traceId`, not a `?trace=` param).
 */
export function parseExploreState(search: string): ExploreState {
  const p = new URLSearchParams(search);
  const limit = Number(p.get("limit"));
  return {
    signal: "logs",
    range: parseRangeParam(p.get("range")),
    filters: p
      .getAll("f")
      .map(filterFromParam)
      .filter((f): f is LabelFilter => f !== null),
    search: p.get("q") ?? "",
    raw: p.get("raw") ?? "",
    limit: Number.isFinite(limit) && limit > 0 ? Math.min(limit, 5000) : 500,
    live: p.get("live") === "1",
    scale: scaleFromParam(p.get("scale")),
    step: stepFromParam(p.get("step")),
    traceFilters: p
      .getAll("tf")
      .map(traceFilterFromParam)
      .filter((f): f is TraceFilter => f !== null),
    trace: "",
    group: p.get("group") ?? "",
    groupBy: p.get("groupBy") || DEFAULT_GROUP_BY,
    grain: grainFromParam(p.get("grain")),
    promql: p.get("promql") ?? "",
    profileType: p.get("ptype") ?? "",
    profileService: p.get("psvc") ?? "",
    profileMatcherLabel: p.get("plabel") ?? "",
    profileMatcherValue: p.get("pvalue") ?? "",
    profileCompare: p.get("pcmp") === "1",
    profileBaseline: parseRangeParam(p.get("pbase")),
    profileId: p.get("pid") ?? "",
    tenant: p.get("tenant") ?? "",
    dataset: p.get("dataset") ?? "",
    // Catalog selection lives in the path (/catalog/:entity/:primary/
    // :secondary — see buildPath/parseCatalogPath), never in search params.
    catalogEntity: DEFAULT_ENTITY_TYPE,
    catalogPrimary: "",
    catalogSecondary: "",
  };
}

/** An unknown grain degrades to the default rather than rejecting the URL. */
function grainFromParam(value: string | null): GroupGrain {
  return value === "spans" || value === "traces" ? value : DEFAULT_GRAIN;
}

/** Unknown values fall back to the default rather than rejecting the URL. */
function scaleFromParam(value: string | null): Scale {
  return value !== null && isScale(value) ? value : DEFAULT_SCALE;
}

/** A malformed step degrades to automatic rather than rejecting the URL. */
function stepFromParam(value: string | null): string {
  if (value === null) return "";
  return durationToSeconds(value) === null ? "" : value;
}

export function buildSearch(state: ExploreState): string {
  const p = new URLSearchParams();
  const rangeParam = rangeToParam(state.range);
  if (rangeParam !== rangeToParam(DEFAULT_RANGE)) p.set("range", rangeParam);
  for (const f of state.filters) p.append("f", filterToParam(f));
  if (state.search) p.set("q", state.search);
  if (state.raw) p.set("raw", state.raw);
  if (state.limit !== 500) p.set("limit", String(state.limit));
  if (state.live) p.set("live", "1");
  if (state.scale !== DEFAULT_SCALE) p.set("scale", state.scale);
  if (state.step !== "") p.set("step", state.step);
  for (const f of traceFiltersForUrl(state.traceFilters)) {
    p.append("tf", traceFilterToParam(f));
  }
  if (state.group) p.set("group", state.group);
  if (state.groupBy !== DEFAULT_GROUP_BY) p.set("groupBy", state.groupBy);
  if (state.grain !== DEFAULT_GRAIN) p.set("grain", state.grain);
  if (state.promql) p.set("promql", state.promql);
  if (state.profileType) p.set("ptype", state.profileType);
  if (state.profileService) p.set("psvc", state.profileService);
  if (state.profileMatcherLabel) p.set("plabel", state.profileMatcherLabel);
  if (state.profileMatcherValue) p.set("pvalue", state.profileMatcherValue);
  if (state.profileCompare) p.set("pcmp", "1");
  if (rangeToParam(state.profileBaseline) !== rangeToParam(DEFAULT_RANGE)) {
    p.set("pbase", rangeToParam(state.profileBaseline));
  }
  if (state.profileId) p.set("pid", state.profileId);
  if (state.tenant) p.set("tenant", state.tenant);
  if (state.dataset) p.set("dataset", state.dataset);
  const s = p.toString();
  return s === "" ? "" : `?${s}`;
}

/**
 * Search params to carry into a *different* signal's URL: the window and
 * tenant context, which stay meaningful across tabs. Everything else
 * (filters, search text, trace/group selection, PromQL, ...) is specific to
 * the signal you're leaving and would either be dead weight or, worse,
 * silently misapplied if a param name ever collided — so it's dropped. Used
 * to build the signal tabs' `<Link>` targets in `ExploreView`.
 */
export function crossSignalSearch(state: ExploreState): string {
  return buildSearch({
    ...DEFAULT_STATE,
    range: state.range,
    live: state.live,
    tenant: state.tenant,
    dataset: state.dataset,
  });
}

/**
 * `update()`'s options. `push: true` marks a patch as entering a
 * conceptually different view (opening a trace, drilling into a group) —
 * a real navigation a user expects Back to step out of, one level at a
 * time — rather than refining state within the current view.
 */
export interface UpdateOptions {
  push?: boolean;
}

export type UpdateFn = (
  patch: Partial<ExploreState>,
  opts?: UpdateOptions,
) => void;

/**
 * URL-backed state: the signal comes from the route's `:signal` path segment
 * (or the static `traces` segment of `/traces/:traceId` — see `routes.tsx`),
 * the open trace id (if any) from `:traceId` on that same route, everything
 * else from search params. `update()` replaces the current history entry by
 * default — refining filters/range/etc. within a view is one entry, not one
 * per keystroke. Pass `{ push: true }` for a patch that enters a genuinely
 * different view (see `UpdateOptions`). Switching signals is the same kind
 * of navigation but goes through `<Link>` instead — see the signal tabs in
 * `ExploreView` and `crossSignalSearch`.
 */
const PATH_STATE_KEYS: ReadonlyArray<keyof ExploreState> = [
  "signal",
  "trace",
  "catalogEntity",
  "catalogPrimary",
  "catalogSecondary",
];

/** True when the patch sets any state that lives in the path, not the search. */
function patchEntersExploreView(patch: Partial<ExploreState>): boolean {
  return PATH_STATE_KEYS.some((key) => key in patch);
}

export function useExploreState(): [ExploreState, UpdateFn] {
  const location = useLocation();
  const navigate = useNavigate();
  const { signal: signalParam, traceId } = useParams<{
    signal?: string;
    traceId?: string;
  }>();
  const onCatalogRoute = location.pathname.split("/")[1] === "catalog";
  const signal =
    traceId !== undefined
      ? "traces"
      : onCatalogRoute
        ? "catalog"
        : signalFromParam(signalParam);
  const state: ExploreState = {
    ...parseExploreState(location.search),
    ...(onCatalogRoute ? parseCatalogPath(location.pathname) : {}),
    signal,
    // react-router's useParams() already URL-decodes route params — a
    // second decodeURIComponent here would double-decode (corrupting a
    // trace id containing a literal "%", or throwing URIError on a
    // malformed one from a pasted/typed value).
    trace: traceId !== undefined ? traceId : "",
  };

  // Off the explore routes (/schema, /manage, /api-keys, …) the signal is
  // only the "logs" default, not something the URL says. A patch that leaves
  // path state alone (tenant/dataset from the top bar or the sticky-context
  // write-back, a range, …) must then keep the current path rather than
  // rebuild it as /logs; only a patch that explicitly enters an explore view
  // (signal, trace, catalog selection) moves away.
  const onExploreRoute =
    signalParam !== undefined || traceId !== undefined || onCatalogRoute;

  const update = useCallback<UpdateFn>(
    (patch, opts) => {
      const next = { ...state, ...patch };
      const path =
        onExploreRoute || patchEntersExploreView(patch)
          ? buildPath(next.signal, next.trace, next)
          : location.pathname;
      navigate(`${path}${buildSearch(next)}`, { replace: !opts?.push });
    },
    // `state` is recomputed each render from location.search/pathname/
    // signalParam/traceId, so depending on those (not `state` itself) still
    // recreates `update` exactly when its captured `state` would go stale.
    [
      navigate,
      location.search,
      location.pathname,
      signalParam,
      traceId,
      onExploreRoute,
    ],
  );

  return [state, update];
}
