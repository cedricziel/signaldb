import { useQueries, useQuery } from "@tanstack/react-query";
import {
  fetchCatalogEntities,
  pinsKey,
  type EntityPin,
} from "../../api/catalog";
import { GROUP_BUDGET, type GroupSort } from "../../api/traceGroups";
import { fetchFieldValueSketch } from "../../api/sourceFields";
import { SkeletonRows } from "../explore/Skeleton";
import { SortTh, useSort } from "../../lib/sortTable";
import {
  facetField,
  upsertTraceFilter,
  type TraceFilter,
} from "../../lib/traceFilters";
import { NOT_SET, compositeKey } from "../../lib/traceGroups";
import {
  formatTimestamp,
  nanosToMs,
  rangeScopeKey,
  resolveRange,
  type ResolvedRange,
} from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { EntityDetail } from "./EntityDetail";
import { redDuration, redErrorClass, redErrorRate, redRate } from "./red";
import {
  DEFAULT_ENTITY_TYPE,
  entityType,
  type EntityTypeDef,
} from "./entityTypes";
import { useCatalogEntityTypes } from "./useEntityTypes";
import "./catalog.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

/** Shared by the list and detail views — the window's length in seconds,
 * for turning a raw count into a rate. Takes the already-resolved range so
 * that a page's rate and its queries describe one `now`, not two. */
export function catalogRangeSeconds(range: ResolvedRange): number {
  return Math.max(1, (range.toMs - range.fromMs) / 1000);
}

/**
 * The entity type the URL names, resolved against the observed set first so
 * a registry-derived type is addressable, then against the curated list — a
 * bookmarked entity type stays openable while its data is out of window.
 *
 * Undefined means "not this URL's type": either the observed set has not
 * landed yet, or nothing in this tenant answers to that id. Both callers
 * must decide what to do with that; substituting a default here is what made
 * every derived entity's detail page render the *service* dashboard.
 */
export function resolveEntityType(
  id: string,
  types: EntityTypeDef[],
): EntityTypeDef | undefined {
  return types.find((e) => e.id === id) ?? entityType(id);
}

export function CatalogView({ state, update }: Props) {
  const range = resolveRange(state.range, Date.now());
  const rangeKey = rangeScopeKey(state);
  // Called before the detail branch below: an early return above a hook
  // changes the hook count between renders of the same instance, and drilling
  // into a row is exactly that transition.
  const { types, isPending, analyzed, asOf } = useCatalogEntityTypes(
    range,
    rangeKey,
  );
  const resolved = resolveEntityType(state.catalogEntity, types);

  if (state.catalogPrimary !== "") {
    // A detail page is entirely an entity type's own: its identity dimensions
    // pin every query on it. Guessing one paints another entity's numbers
    // under this entity's name, so it waits instead.
    if (!resolved) {
      return (
        <div className="traces-note">
          {isPending
            ? "Loading entity types…"
            : `Unknown entity type "${state.catalogEntity}".`}
        </div>
      );
    }
    return (
      <EntityDetail
        entity={resolved}
        range={range}
        state={state}
        update={update}
      />
    );
  }

  // The list, unlike the detail page, has somewhere sensible to land: showing
  // the first observed type beats showing nothing.
  const selected = resolved ?? types[0] ?? entityType(DEFAULT_ENTITY_TYPE)!;

  return (
    <div className="catalog">
      <CatalogNav
        types={types}
        selectedId={selected.id}
        range={range}
        rangeKey={rangeKey}
        analyzed={analyzed}
        asOf={asOf}
        onSelect={(id) => update({ catalogEntity: id })}
      />
      <EntityTable
        key={selected.id}
        entity={selected}
        range={range}
        rangeKey={rangeKey}
        rangeSeconds={catalogRangeSeconds(range)}
        onRowClick={(values) =>
          update({ catalogPrimary: compositeKey(values) }, { push: true })
        }
      />
    </div>
  );
}

function CatalogNav({
  types,
  selectedId,
  range,
  rangeKey,
  analyzed,
  asOf,
  onSelect,
}: {
  types: EntityTypeDef[];
  selectedId: string;
  range: ResolvedRange;
  rangeKey: string;
  analyzed: boolean;
  asOf?: string;
  onSelect: (id: string) => void;
}) {
  const results = useQueries({
    queries: types.map((e) => ({
      queryKey: entityQueryKey(e.id, rangeKey, NAV_SORT),
      queryFn: () => fetchCatalogEntities(e, range, NAV_SORT),
      staleTime: 30_000,
    })),
  });

  return (
    <aside className="sidebar" aria-label="Entity types">
      <div className="sidebar-head">Entities</div>
      <div className="fieldlist">
        {types.map((e, i) => {
          const result = results[i];
          const countLabel = result?.data
            ? result.data.truncated
              ? `${GROUP_BUDGET}+`
              : String(result.data.entities.length)
            : "…";
          return (
            <button
              key={e.id}
              className={`field ${selectedId === e.id ? "open" : ""}`}
              aria-pressed={selectedId === e.id}
              onClick={() => onSelect(e.id)}
            >
              <span>{e.label}</span>
              <span className="facet-active">{countLabel}</span>
            </button>
          );
        })}
      </div>
      {/* Absence of an entity type is a claim about the data; absence of
          metadata is a claim about what we know. Saying so keeps a
          never-compacted deployment from reading as an empty one. */}
      <div className="sidebar-note">
        {analyzed
          ? asOf && `Field metadata as of ${asOf}`
          : "Not analyzed yet — entity types appear once compaction has run."}
      </div>
    </aside>
  );
}

/**
 * A row drills down using whichever identity dimensions have a filter
 * mapping (see `lib/traceFilters.ts`'s FACET_FIELDS) — not necessarily all
 * of them. "service", for instance, groups by both `service.name` and
 * `service.namespace`, but only the former is a mapped facet; filtering
 * Traces on just the name is still meaningful, so the row stays clickable
 * rather than requiring every dimension to be mapped. Every entity type
 * currently registered has at least one mapped dimension, but a future one
 * added ahead of its FACET_FIELDS entry would fall through to
 * {@link isDrillable} and render inert rows instead of silently navigating
 * to an unfiltered trace list.
 */
export function drillFilters(
  entity: EntityTypeDef,
  values: (string | null)[],
): TraceFilter[] {
  let filters: TraceFilter[] = [];
  entity.identity.forEach((field, i) => {
    if (facetField(field) === undefined) return;
    const v = values[i];
    if (v == null) return;
    filters = upsertTraceFilter(filters, { field, value: v });
  });
  return filters;
}

export function isDrillable(entity: EntityTypeDef): boolean {
  return entity.identity.some((field) => facetField(field) !== undefined);
}

/**
 * The cache key for one entity-type aggregate.
 *
 * The nav and the table ask for the same thing about the selected entity
 * type — same aggregate, same sort, same window — so they must agree on the
 * key down to the last element, or the selected type is fetched twice on
 * every paint. They did not: the nav omitted the pin element, so its
 * five-element key never matched the table's six.
 *
 * Deriving both from here is what keeps them in step; a key spelled out at
 * each call site drifts the moment one gains a dimension.
 */
export function entityQueryKey(
  entityId: string,
  rangeKey: string,
  sort: GroupSort,
  pinned: EntityPin[] = [],
): (string | undefined)[] {
  return [
    "catalog-entities",
    entityId,
    rangeKey,
    sort.key,
    sort.dir,
    pinsKey(pinned),
  ];
}

/** The sort the nav's counts are fetched at — and therefore the sort a table
 * must be showing for the two to share a cache entry. */
export const NAV_SORT: GroupSort = { key: "n", dir: "desc" };

/**
 * An empty result, said as precisely as the data allows.
 *
 * "No hosts in this window" and "no host has ever reported" are different
 * findings — the second is a reason to go and look at your instrumentation,
 * the first is a reason to widen the range — and a bare empty table conflates
 * them.
 *
 * The maintained value sketch can tell them apart, and this is the only thing
 * in the catalog it may be asked. It reports `window_scoped: false`: it
 * describes what compaction last saw, never the selected range. So it can say
 * "this attribute has values, just not here", and it must never be used to
 * list them as though they were current — an entity last seen days ago would
 * appear to someone who narrowed to fifteen minutes.
 *
 * Consulted only once the window has come back empty, so the common path
 * costs nothing.
 */
function EmptyEntityState({
  entity,
  range,
}: {
  entity: EntityTypeDef;
  range: ResolvedRange;
}) {
  const primary = entity.identity[0]!;
  const sources = entity.sources ?? ["traces"];
  const sketch = useQuery({
    queryKey: ["catalog-empty-sketch", entity.id, primary, sources[0]],
    queryFn: () => fetchFieldValueSketch(sources[0]!, primary, range),
    staleTime: 5 * 60_000,
  });

  return (
    <div className="traces-note">
      No {entity.label.toLowerCase()} observed in this window — no matching{" "}
      <code>{primary}</code> value seen in {sources.join(" or ")}.
      {sketch.data && (
        <>
          {" "}
          {sketch.data.distinct === 1
            ? "One value has"
            : `${sketch.data.distinct} values have`}{" "}
          been seen outside it
          {sketch.data.asOf ? ` (as of ${sketch.data.asOf})` : ""}
          {sketch.data.examples.length > 0 && (
            <>
              , such as <code>{sketch.data.examples[0]}</code>
            </>
          )}
          . Try a wider time range.
        </>
      )}
    </div>
  );
}

/**
 * The RED table shared by the entity-type list view and (for a
 * `breakdown` dimension, pinned to the parent entity) the detail page —
 * same columns, same sort/loading/empty handling either way. `pinned`
 * narrows the aggregate to one entity's identity values (see
 * `EntityPin` in `api/catalog.ts`); omit it for the top-level list.
 */
export function EntityTable({
  entity,
  range,
  rangeKey,
  rangeSeconds,
  pinned,
  onRowClick,
}: {
  entity: EntityTypeDef;
  range: ResolvedRange;
  rangeKey: string;
  rangeSeconds: number;
  pinned?: EntityPin[];
  /** Omit for a read-only table (e.g. a `topValues` ranking with nothing
   * to drill into) — rows render without a click affordance. */
  onRowClick?: (values: (string | null)[]) => void;
}) {
  const [sort, toggle] = useSort("n", "desc");
  const result = useQuery({
    queryKey: entityQueryKey(entity.id, rangeKey, sort as GroupSort, pinned),
    queryFn: () =>
      fetchCatalogEntities(entity, range, sort as GroupSort, pinned),
  });

  const pending = result.isPending;
  const rows = result.data?.entities ?? [];
  const columns = entity.identity.length + 5;
  const done = !pending && result.data !== undefined;

  return (
    <div className="catalog-main">
      <div className="catalog-headline">
        <span className="catalog-title">{entity.label}</span>
        <span className="catalog-sub">
          discovered from {entity.identity.join(", ")}
          {entity.sources && entity.sources.length > 1
            ? ` across ${entity.sources.join(", ")}`
            : ""}
        </span>
      </div>
      {result.isError && (
        <div className="query-error" role="alert">
          Entities failed: {(result.error as Error).message}
        </div>
      )}
      <table className="trace-table" aria-busy={pending}>
        <thead>
          <tr>
            {entity.identity.map((dim) => (
              <th key={dim}>{dim}</th>
            ))}
            <SortTh
              label="Rate"
              sortKey="n"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="Errors"
              sortKey="errors"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="P50"
              sortKey="p50"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="P95"
              sortKey="p95"
              sort={sort}
              toggle={toggle}
              numeric
            />
            <SortTh
              label="Last seen"
              sortKey="last"
              sort={sort}
              toggle={toggle}
              firstDir="desc"
            />
          </tr>
        </thead>
        <tbody>
          {pending ? (
            <SkeletonRows
              rows={8}
              columns={columns}
              numericFrom={entity.identity.length}
            />
          ) : (
            rows.map((g) => (
              <tr
                key={g.values.join("")}
                className={
                  onRowClick ? "catalog-row-drillable" : "catalog-row-static"
                }
                onClick={onRowClick ? () => onRowClick(g.values) : undefined}
              >
                {g.values.map((v, i) => (
                  <td key={entity.identity[i]} title={v ?? undefined}>
                    {v ?? NOT_SET}
                  </td>
                ))}
                <td className="num">{redRate(g.red, rangeSeconds)}</td>
                <td className={`num${redErrorClass(g.red) ? " err-rate" : ""}`}>
                  {redErrorRate(g.red)}
                </td>
                <td className="num">{redDuration(g.red, "p50Ms")}</td>
                <td className="num">{redDuration(g.red, "p95Ms")}</td>
                <td>{formatTimestamp(nanosToMs(g.lastNs))}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
      {done && rows.length === 0 && (
        <EmptyEntityState entity={entity} range={range} />
      )}
      {result.data?.truncated && (
        <div className="traces-note">
          Showing the top {GROUP_BUDGET} {entity.label.toLowerCase()} by the
          current sort — narrow the time range to see the rest.
        </div>
      )}
    </div>
  );
}
