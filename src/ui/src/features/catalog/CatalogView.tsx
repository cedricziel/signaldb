import { useQueries, useQuery } from "@tanstack/react-query";
import { fetchCatalogEntities, type EntityPin } from "../../api/catalog";
import { GROUP_BUDGET, type GroupSort } from "../../api/traceGroups";
import { SkeletonRows } from "../explore/Skeleton";
import { SortTh, useSort } from "../../lib/sortTable";
import {
  facetField,
  upsertTraceFilter,
  type TraceFilter,
} from "../../lib/traceFilters";
import { NOT_SET, compositeKey, formatRate } from "../../lib/traceGroups";
import {
  formatTimestamp,
  nanosToMs,
  rangeScopeKey,
  resolveRange,
  type ResolvedRange,
} from "../../lib/time";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { formatDurationMsOrDash } from "../../lib/waterfall";
import { EntityDetail } from "./EntityDetail";
import {
  DEFAULT_ENTITY_TYPE,
  ENTITY_TYPES,
  entityType,
  type EntityTypeDef,
} from "./entityTypes";
import "./catalog.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

/** Shared by the list and detail views — the window's length in seconds,
 * for turning a raw count into a rate. */
export function catalogRangeSeconds(state: ExploreState): number {
  const r = resolveRange(state.range, Date.now());
  return Math.max(1, (r.toMs - r.fromMs) / 1000);
}

export function CatalogView({ state, update }: Props) {
  if (state.catalogPrimary !== "") {
    return <EntityDetail state={state} update={update} />;
  }

  const range = resolveRange(state.range, Date.now());
  const rangeKey = rangeScopeKey(state);
  const selected =
    entityType(state.catalogEntity) ?? entityType(DEFAULT_ENTITY_TYPE)!;

  return (
    <div className="catalog">
      <CatalogNav
        selectedId={selected.id}
        range={range}
        rangeKey={rangeKey}
        onSelect={(id) => update({ catalogEntity: id })}
      />
      <EntityTable
        key={selected.id}
        entity={selected}
        range={range}
        rangeKey={rangeKey}
        rangeSeconds={catalogRangeSeconds(state)}
        onRowClick={(values) =>
          update({ catalogPrimary: compositeKey(values) }, { push: true })
        }
      />
    </div>
  );
}

function CatalogNav({
  selectedId,
  range,
  rangeKey,
  onSelect,
}: {
  selectedId: string;
  range: ResolvedRange;
  rangeKey: string;
  onSelect: (id: string) => void;
}) {
  const results = useQueries({
    queries: ENTITY_TYPES.map((e) => ({
      queryKey: ["catalog-entities", e.id, rangeKey, "n", "desc"],
      queryFn: () => fetchCatalogEntities(e, range),
      staleTime: 30_000,
    })),
  });

  return (
    <aside className="sidebar" aria-label="Entity types">
      <div className="sidebar-head">Entities</div>
      <div className="fieldlist">
        {ENTITY_TYPES.map((e, i) => {
          const result = results[i];
          const countLabel = result?.data
            ? result.data.truncated
              ? `${GROUP_BUDGET}+`
              : String(result.data.groups.length)
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
  const pinKey = (pinned ?? []).map((p) => `${p.field}=${p.value}`).join(",");
  const result = useQuery({
    queryKey: [
      "catalog-entities",
      entity.id,
      rangeKey,
      sort.key,
      sort.dir,
      pinKey,
    ],
    queryFn: () =>
      fetchCatalogEntities(entity, range, sort as GroupSort, pinned),
  });

  const pending = result.isPending;
  const rows = result.data?.groups ?? [];
  const columns = entity.identity.length + 6;
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
              label="Count"
              sortKey="n"
              sort={sort}
              toggle={toggle}
              numeric
            />
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
                <td className="num">{g.count}</td>
                <td className="num">{formatRate(g.count, rangeSeconds)}</td>
                <td className={`num${g.errors > 0 ? " err-rate" : ""}`}>
                  {g.errors > 0
                    ? `${Math.round((100 * g.errors) / (g.traceCount ?? g.count))}%`
                    : "–"}
                </td>
                <td className="num">
                  {formatDurationMsOrDash(g.traceCount, g.p50Ms)}
                </td>
                <td className="num">
                  {formatDurationMsOrDash(g.traceCount, g.p95Ms)}
                </td>
                <td>{formatTimestamp(nanosToMs(g.lastNs))}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
      {done && rows.length === 0 && (
        <div className="traces-note">
          No {entity.label.toLowerCase()} observed in this window — no matching{" "}
          <code>{entity.identity[0]}</code> value seen in{" "}
          {(entity.sources ?? ["traces"]).join(" or ")}.
        </div>
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
