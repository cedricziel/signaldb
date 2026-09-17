// A catalog entity's own page: its RED numbers pinned to its exact
// identity, a breakdown table (when its entity type defines one — see
// entityTypes.ts), and real example spans. Reused for both drill depths:
// the entity itself (catalogPrimary) and, one level deeper, a breakdown row
// (catalogSecondary) — same component, same query mechanism, just a
// different pinned identity.
import { useQuery } from "@tanstack/react-query";
import {
  fetchCatalogEntities,
  pinsKey,
  type EntityPin,
} from "../../api/catalog";
import { fetchTraceGroupMembers } from "../../api/traceGroupMembers";
import { QueryError } from "../../components/QueryError";
import { DependencyBreakdown } from "./DependencyBreakdown";
import { EntityMetricsPanel } from "./EntityMetricsPanel";
import {
  formatTimestampForRange,
  nanosToMs,
  rangeScopeKey,
  type ResolvedRange,
} from "../../lib/time";
import {
  compositeKey,
  groupLabel,
  parseCompositeKey,
} from "../../lib/traceGroups";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { MemberTable } from "../traces/MemberTable";
import { SkeletonLines } from "../explore/Skeleton";
import {
  catalogRangeSeconds,
  drillFilters,
  EntityTable,
  isDrillable,
} from "./CatalogView";
import {
  Observed,
  redDuration,
  redErrorClass,
  redErrorRate,
  redRate,
} from "./red";
import type { EntityTypeDef } from "./entityTypes";
import "./catalog.css";

interface Props {
  /**
   * The entity type this page describes. Resolved by the caller against the
   * observed set rather than looked up here, because the curated registry
   * this module can reach holds only the hand-written types — see
   * {@link resolveEntityType}.
   */
  entity: EntityTypeDef;
  /** The caller's already-resolved window, so both views read one `now`. */
  range: ResolvedRange;
  state: ExploreState;
  update: UpdateFn;
}

/** "Recent" is illustrative context, not the main list — a small, fixed
 * bound rather than the traces tab's user-configurable limit. */
const MEMBER_LIMIT = 25;

export function EntityDetail({ entity, range, state, update }: Props) {
  const rangeKey = rangeScopeKey(state);
  const rangeSeconds = catalogRangeSeconds(range);

  const primaryValues = parseCompositeKey(
    state.catalogPrimary,
    entity.identity,
  );
  // `parseCompositeKey` always returns one entry per identity dimension
  // (null for a "(not set)" segment), so every dimension is pinned — a
  // dimension whose value is null pins to "absent on this record" (see
  // `EntityPin`/`buildEntitySourceDoc` in api/catalog.ts), not left
  // unconstrained. Dropping a null value here used to let the KPI query
  // match *any* value for that dimension, pulling in a different entity's
  // numbers under this one's name.
  const primaryPinned: EntityPin[] = entity.identity.map((field, i) => ({
    field,
    value: primaryValues[i] ?? null,
  }));

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

  const topValuesEntity: EntityTypeDef | undefined = entity.topValues
    ? {
        id: `${entity.id}::${entity.topValues.field}`,
        label: entity.topValues.label,
        singular: entity.topValues.label,
        identity: [entity.topValues.field],
        spanKindScope: entity.spanKindScope,
      }
    : undefined;

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
  const currentPinKey = pinsKey(currentPinned);

  const kpiQuery = useQuery({
    queryKey: ["catalog-entity-kpi", current.id, rangeKey, currentPinKey],
    queryFn: () =>
      fetchCatalogEntities(current, range, undefined, currentPinned),
  });
  const kpiRow = kpiQuery.data?.entities[0];

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
    const filters = drillFilters(entity, primaryValues);
    // At the breakdown level (an operation within a service, say) the
    // parent entity's own filters say nothing about *which* operation was
    // drilled into — without this, "View matching traces →" from an
    // operation page dropped the operation and showed every trace for the
    // whole service.
    const withBreakdown =
      atSecondary && breakdownEntity?.identity[0] === "span.name"
        ? [...filters, { field: "name", value: state.catalogSecondary }]
        : filters;
    update(
      { signal: "traces", traceFilters: withBreakdown },
      { push: true },
    );
  };

  const title = atSecondary
    ? groupLabel(state.catalogSecondary)
    : groupLabel(state.catalogPrimary);

  const kpiBody = kpiQuery.isError ? (
    <QueryError what="this entity" error={kpiQuery.error} />
  ) : kpiQuery.isPending ? (
    <SkeletonLines lines={6} />
  ) : kpiRow ? (
    <dl className="entity-kpis">
      <div>
        <dt>Signals</dt>
        <dd>
          <Observed observations={kpiRow.observations} />
        </dd>
      </div>
      <div>
        <dt>Rate</dt>
        <dd>{redRate(kpiRow.red, rangeSeconds)}</dd>
      </div>
      <div>
        <dt>Errors</dt>
        <dd className={redErrorClass(kpiRow.red) ? "err-rate" : undefined}>
          {redErrorRate(kpiRow.red)}
        </dd>
      </div>
      <div>
        <dt>P50</dt>
        <dd>{redDuration(kpiRow.red, "p50Ms")}</dd>
      </div>
      <div>
        <dt>P95</dt>
        <dd>{redDuration(kpiRow.red, "p95Ms")}</dd>
      </div>
      <div>
        <dt>Last seen</dt>
        <dd>{formatTimestampForRange(nanosToMs(kpiRow.lastNs), range)}</dd>
      </div>
    </dl>
  ) : (
    <div className="view-note">No matching spans in this window.</div>
  );

  return (
    <div className="catalog-main entity-detail">
      <nav className="catalog-breadcrumb" aria-label="Breadcrumb">
        <button
          onClick={() =>
            update(
              { catalogPrimary: "", catalogSecondary: "" },
              { push: true },
            )
          }
        >
          catalog
        </button>
        <span className="catalog-crumb-sep">/</span>
        <button
          onClick={() =>
            update(
              { catalogPrimary: "", catalogSecondary: "" },
              { push: true },
            )
          }
        >
          {entity.label}
        </button>
        <span className="catalog-crumb-sep">/</span>
        {atSecondary ? (
          <>
            <button
              onClick={() => update({ catalogSecondary: "" }, { push: true })}
            >
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
          <button className="btn" onClick={openTraces}>
            View matching traces →
          </button>
        )}
      </div>

      {kpiBody}

      {/* Pinned to the entity, never to `currentPinned`: a breakdown row is a
          dimension within the entity, not something a resource attribute
          identifies, so metrics pinned to it could not exist. */}
      <EntityMetricsPanel
        entity={entity}
        pinned={primaryPinned}
        range={range}
        rangeKey={rangeKey}
      />

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

      {topValuesEntity && (
        <EntityTable
          entity={topValuesEntity}
          range={range}
          rangeKey={rangeKey}
          rangeSeconds={rangeSeconds}
          pinned={currentPinned}
        />
      )}

      {entity.id === "service" && !atSecondary && primaryValues[0] && (
        <div className="catalog-main">
          <div className="catalog-headline">
            <span className="catalog-title">Time by dependency</span>
            <span className="catalog-sub">
              discovered from db.system.name, http.request.method, rpc.system,
              messaging.system
            </span>
          </div>
          <DependencyBreakdown
            serviceName={primaryValues[0]}
            range={range}
            rangeKey={rangeKey}
          />
        </div>
      )}

      <div className="catalog-headline">
        <span className="catalog-title">Recent matching spans</span>
      </div>
      <MemberTable
        members={membersQuery.data}
        error={membersQuery.error}
        what="spans"
        identityLabel="Span"
        emptyMessage="No matching spans in this window."
        onOpenTrace={(traceId) =>
          update({ signal: "traces", trace: traceId }, { push: true })
        }
      />
    </div>
  );
}
