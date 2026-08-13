// A catalog entity's own page: its RED numbers pinned to its exact
// identity, a breakdown table (when its entity type defines one — see
// entityTypes.ts), and real example spans. Reused for both drill depths:
// the entity itself (catalogPrimary) and, one level deeper, a breakdown row
// (catalogSecondary) — same component, same query mechanism, just a
// different pinned identity.
import { useQuery } from "@tanstack/react-query";
import { fetchCatalogEntities, type EntityPin } from "../../api/catalog";
import { fetchTraceGroupMembers } from "../../api/traceGroupMembers";
import {
  formatTimestamp,
  nanosToMs,
  rangeToParam,
  resolveRange,
} from "../../lib/time";
import {
  compositeKey,
  formatRate,
  groupLabel,
  parseCompositeKey,
} from "../../lib/traceGroups";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { formatDurationMs } from "../../lib/waterfall";
import { MemberTable } from "../traces/MemberTable";
import {
  catalogRangeSeconds,
  drillFilters,
  EntityTable,
  isDrillable,
} from "./CatalogView";
import {
  DEFAULT_ENTITY_TYPE,
  entityType,
  type EntityTypeDef,
} from "./entityTypes";
import "./catalog.css";

interface Props {
  state: ExploreState;
  update: UpdateFn;
}

/** "Recent" is illustrative context, not the main list — a small, fixed
 * bound rather than the traces tab's user-configurable limit. */
const MEMBER_LIMIT = 25;

export function EntityDetail({ state, update }: Props) {
  const entity =
    entityType(state.catalogEntity) ?? entityType(DEFAULT_ENTITY_TYPE)!;
  const range = resolveRange(state.range, Date.now());
  const rangeKey = `${rangeToParam(state.range)}|${state.tenant}|${state.dataset}`;
  const rangeSeconds = catalogRangeSeconds(state);

  const primaryValues = parseCompositeKey(
    state.catalogPrimary,
    entity.identity,
  );
  const primaryPinned: EntityPin[] = entity.identity
    .map((field, i) => ({ field, value: primaryValues[i] }))
    .filter((p): p is EntityPin => p.value != null);

  const breakdownEntity: EntityTypeDef | undefined = entity.breakdown
    ? {
        id: `${entity.id}::${entity.breakdown.field}`,
        label: entity.breakdown.label,
        singular: entity.breakdown.label,
        identity: [entity.breakdown.field],
        spanKindScope: entity.spanKindScope,
      }
    : undefined;
  const atSecondary =
    breakdownEntity !== undefined && state.catalogSecondary !== "";

  // What's on screen right now: the parent entity, or — one level deeper —
  // the breakdown row. Same query shape either way.
  const current = atSecondary && breakdownEntity ? breakdownEntity : entity;
  const currentPinned: EntityPin[] =
    atSecondary && breakdownEntity
      ? [
          ...primaryPinned,
          {
            field: breakdownEntity.identity[0]!,
            value: state.catalogSecondary,
          },
        ]
      : primaryPinned;
  const currentPinKey = currentPinned
    .map((p) => `${p.field}=${p.value}`)
    .join(",");

  const kpiQuery = useQuery({
    queryKey: ["catalog-entity-kpi", current.id, rangeKey, currentPinKey],
    queryFn: () =>
      fetchCatalogEntities(current, range, undefined, currentPinned),
  });
  const kpiRow = kpiQuery.data?.groups[0];

  const memberDims =
    atSecondary && breakdownEntity
      ? [...entity.identity, breakdownEntity.identity[0]!]
      : entity.identity;
  const memberValues =
    atSecondary && breakdownEntity
      ? [...primaryValues, state.catalogSecondary]
      : primaryValues;
  const membersQuery = useQuery({
    queryKey: [
      "catalog-entity-members",
      entity.id,
      rangeKey,
      compositeKey(memberValues),
    ],
    queryFn: () =>
      fetchTraceGroupMembers(
        memberDims,
        memberValues,
        range,
        [],
        "spans",
        MEMBER_LIMIT,
      ),
  });

  const drillable = isDrillable(entity);
  const openTraces = () => {
    update(
      { signal: "traces", traceFilters: drillFilters(entity, primaryValues) },
      { push: true },
    );
  };

  const title = atSecondary
    ? groupLabel(state.catalogSecondary)
    : groupLabel(state.catalogPrimary);

  return (
    <div className="catalog-main entity-detail">
      <nav className="catalog-breadcrumb" aria-label="Breadcrumb">
        <button
          onClick={() => update({ catalogPrimary: "", catalogSecondary: "" })}
        >
          catalog
        </button>
        <span className="catalog-crumb-sep">/</span>
        <button
          onClick={() => update({ catalogPrimary: "", catalogSecondary: "" })}
        >
          {entity.label}
        </button>
        <span className="catalog-crumb-sep">/</span>
        {atSecondary ? (
          <>
            <button onClick={() => update({ catalogSecondary: "" })}>
              {groupLabel(state.catalogPrimary)}
            </button>
            <span className="catalog-crumb-sep">/</span>
            <span className="catalog-crumb-current">
              {groupLabel(state.catalogSecondary)}
            </span>
          </>
        ) : (
          <span className="catalog-crumb-current">
            {groupLabel(state.catalogPrimary)}
          </span>
        )}
      </nav>

      <div className="catalog-headline">
        <span className="catalog-title">{title}</span>
        {drillable && (
          <button className="act" onClick={openTraces}>
            View matching traces →
          </button>
        )}
      </div>

      {kpiQuery.isError ? (
        <div className="query-error" role="alert">
          Failed to load: {(kpiQuery.error as Error).message}
        </div>
      ) : kpiRow ? (
        <dl className="entity-kpis">
          <div>
            <dt>Count</dt>
            <dd>{kpiRow.count}</dd>
          </div>
          <div>
            <dt>Rate</dt>
            <dd>{formatRate(kpiRow.count, rangeSeconds)}</dd>
          </div>
          <div>
            <dt>Errors</dt>
            <dd className={kpiRow.errors > 0 ? "err-rate" : undefined}>
              {kpiRow.errors > 0
                ? `${Math.round((100 * kpiRow.errors) / kpiRow.count)}%`
                : "–"}
            </dd>
          </div>
          <div>
            <dt>P50</dt>
            <dd>{formatDurationMs(kpiRow.p50Ms)}</dd>
          </div>
          <div>
            <dt>P95</dt>
            <dd>{formatDurationMs(kpiRow.p95Ms)}</dd>
          </div>
          <div>
            <dt>Last seen</dt>
            <dd>{formatTimestamp(nanosToMs(kpiRow.lastNs))}</dd>
          </div>
        </dl>
      ) : (
        !kpiQuery.isPending && (
          <div className="traces-note">No matching spans in this window.</div>
        )
      )}

      {!atSecondary && breakdownEntity && (
        <EntityTable
          entity={breakdownEntity}
          range={range}
          rangeKey={rangeKey}
          rangeSeconds={rangeSeconds}
          pinned={primaryPinned}
          onRowClick={(values) =>
            update({ catalogSecondary: compositeKey(values) }, { push: true })
          }
        />
      )}

      <div className="catalog-headline">
        <span className="catalog-title">Recent matching spans</span>
      </div>
      <MemberTable
        members={membersQuery.data}
        isError={membersQuery.isError}
        errorMessage={
          membersQuery.isError
            ? `Failed to load: ${(membersQuery.error as Error).message}`
            : undefined
        }
        identityLabel="Span"
        emptyMessage="No matching spans in this window."
        onOpenTrace={(traceId) =>
          update({ signal: "traces", trace: traceId }, { push: true })
        }
      />
    </div>
  );
}
