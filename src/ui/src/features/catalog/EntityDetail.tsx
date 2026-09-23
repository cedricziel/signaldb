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
import { KpiCard, KpiStrip } from "../../components/KpiCard";
import { Sparkline } from "../../components/Sparkline";
import { DependencyBreakdown } from "./DependencyBreakdown";
import { EntityErrorGroups } from "./EntityErrorGroups";
import { EntityMetricsPanel } from "./EntityMetricsPanel";
import { useEntityKpis } from "./useEntityKpis";
import { errorRatePercent, pctChange, ppChange } from "./entityKpiFormat";
import type { KpiChangeFigure } from "./entityKpiFormat";
import {
  formatTimestampForRange,
  nanosToMs,
  rangeScopeKey,
  type ResolvedRange,
} from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import { formatRatePerSec } from "../../lib/traceGroups";
import { formatTimestamp } from "../../lib/vizFormat";
import type { LabelFilter } from "../../lib/filters";
import {
  compositeKey,
  groupLabel,
  parseCompositeKey,
} from "../../lib/traceGroups";
import type { ExploreState, UpdateFn } from "../../lib/urlState";
import { MemberTable } from "../../components/MemberTable";
import { SkeletonLines } from "../explore/Skeleton";
import {
  catalogRangeSeconds,
  drillFilters,
  EntityTable,
  isDrillable,
} from "./CatalogView";
import { OperationsTable } from "./OperationsTable";
import { Observed } from "./red";
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

/** Identity fields that describe the emitting resource, which OTel logs
 * carry the same way spans do — as opposed to a span-only attribute
 * (`db.namespace`, `messaging.destination.name`, the operation breakdown's
 * `span.name`), which a log record has no equivalent for. */
const LOG_COMPATIBLE_FIELDS = new Set([
  "service.name",
  "service.namespace",
  "host.name",
  "k8s.pod.name",
  "k8s.namespace.name",
  "k8s.node.name",
  "container.name",
  "process.pid",
]);

/** The KPI cards' sparklines bucket at ~a minute; shared by all three so a
 * hover reads the same resolution across the strip. */
function sparklineLabel(x: number): string {
  return formatTimestamp(x, 60_000);
}

/** Which way a KPI card's own tone runs isn't fixed to the figure's
 * direction — an error rate going up is bad, a rate going up is neither. */
function toKpiChange(
  figure: KpiChangeFigure,
  kind: "neutral" | "errors" | "duration",
) {
  const tone: "good" | "bad" | "neutral" =
    figure.direction === "flat" || kind === "neutral"
      ? "neutral"
      : figure.direction === "up"
        ? "bad"
        : "good";
  return { ...figure, tone };
}

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

  // Only for the "Signals" badge next to the title — which sources cover
  // this entity at all isn't something the traces-only KPI query below can
  // answer (it has no metrics/logs signal to report on).
  const kpiQuery = useQuery({
    queryKey: ["catalog-entity-kpi", current.id, rangeKey, currentPinKey],
    queryFn: () =>
      fetchCatalogEntities(current, range, undefined, currentPinned),
  });
  const kpiRow = kpiQuery.data?.entities[0];

  const kpisQuery = useEntityKpis(current, range, rangeKey, currentPinned);
  const kpis = kpisQuery.data;

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
    update({ signal: "traces", traceFilters: withBreakdown }, { push: true });
  };

  // Only the entity's own identity dimensions that a log record can also
  // carry — an entity type pinned on a span-only attribute (`db.namespace`,
  // `messaging.destination.name`, an operation's `span.name`) has no
  // equivalent scope in Logs, so that button is left off entirely rather
  // than jumping to a Logs view that silently ignores part of the entity.
  const canOpenLogs =
    drillable && entity.identity.every((f) => LOG_COMPATIBLE_FIELDS.has(f));
  const openLogs = () => {
    const filters: LabelFilter[] = entity.identity.flatMap((field, i) => {
      const v = primaryValues[i];
      return v == null ? [] : [{ label: field, op: "=" as const, value: v }];
    });
    update({ signal: "logs", filters }, { push: true });
  };

  const title = atSecondary
    ? groupLabel(state.catalogSecondary)
    : groupLabel(state.catalogPrimary);

  const kpiBody = kpisQuery.isError ? (
    <QueryError what="this entity" error={kpisQuery.error} />
  ) : kpisQuery.isPending ? (
    <SkeletonLines lines={6} />
  ) : kpis?.current ? (
    <>
      <KpiStrip>
        <KpiCard
          label="Rate"
          value={formatRatePerSec(kpis.current.ratePerSec)}
          change={
            kpis.previous &&
            toKpiChange(
              pctChange(kpis.current.ratePerSec, kpis.previous.ratePerSec),
              "neutral",
            )
          }
          detail={`peak ${formatRatePerSec(kpis.current.peakRatePerSec)} · ${kpis.current.count.toLocaleString()} total`}
        >
          <Sparkline
            points={kpis.series.rate.map((p) => ({ x: p.tMs, v: p.value }))}
            width="100%"
            tone="neutral"
            valueLabel="rate"
            formatValue={formatRatePerSec}
            formatLabel={sparklineLabel}
          />
        </KpiCard>
        <KpiCard
          label="Errors"
          value={errorRatePercent(kpis.current.errorRate)}
          valueTone={kpis.current.errorRate > 0 ? "error" : "neutral"}
          change={
            kpis.previous &&
            toKpiChange(
              ppChange(
                kpis.current.errorRate * 100,
                kpis.previous.errorRate * 100,
              ),
              "errors",
            )
          }
          detail={`${Math.round(kpis.current.errorRate * kpis.current.count).toLocaleString()} failed`}
        >
          <Sparkline
            points={kpis.series.errorRate.map((p) => ({
              x: p.tMs,
              v: p.value,
            }))}
            width="100%"
            tone="error"
            valueLabel="error rate"
            formatValue={errorRatePercent}
            formatLabel={sparklineLabel}
          />
        </KpiCard>
        <KpiCard
          label="Duration"
          value={formatDurationMs(kpis.current.p95Ms)}
          change={
            kpis.previous &&
            toKpiChange(
              pctChange(kpis.current.p95Ms, kpis.previous.p95Ms),
              "duration",
            )
          }
          detail={`p50 ${formatDurationMs(kpis.current.p50Ms)} · p99 ${formatDurationMs(kpis.current.p99Ms)}`}
        >
          <Sparkline
            points={kpis.series.p95.map((p) => ({ x: p.tMs, v: p.value }))}
            width="100%"
            tone="accent"
            valueLabel="p95"
            formatValue={formatDurationMs}
            formatLabel={sparklineLabel}
          />
        </KpiCard>
      </KpiStrip>
      <div className="entity-last-seen">
        Last seen{" "}
        {formatTimestampForRange(nanosToMs(kpis.current.lastNs), range)}
      </div>
    </>
  ) : (
    <div className="view-note">No matching spans in this window.</div>
  );

  return (
    <div className="catalog-main entity-detail">
      <nav className="catalog-breadcrumb" aria-label="Breadcrumb">
        <button
          onClick={() =>
            update({ catalogPrimary: "", catalogSecondary: "" }, { push: true })
          }
        >
          catalog
        </button>
        <span className="catalog-crumb-sep">/</span>
        <button
          onClick={() =>
            update({ catalogPrimary: "", catalogSecondary: "" }, { push: true })
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
        <div className="entity-detail-title">
          <span className="catalog-title">{title}</span>
          {kpiRow && <Observed observations={kpiRow.observations} />}
        </div>
        {drillable && (
          <div className="entity-detail-actions">
            {canOpenLogs && (
              <button className="btn" onClick={openLogs}>
                Logs
              </button>
            )}
            <button className="btn" onClick={openTraces}>
              Traces
            </button>
          </div>
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
        <OperationsTable
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

      {entity.id === "service" && !atSecondary && primaryValues[0] && (
        <EntityErrorGroups
          serviceName={primaryValues[0]}
          range={range}
          rangeKey={rangeKey}
          update={update}
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
        emptyMessage="No spans in this range"
        onOpenTrace={(traceId) =>
          update({ signal: "traces", trace: traceId }, { push: true })
        }
      />
    </div>
  );
}
