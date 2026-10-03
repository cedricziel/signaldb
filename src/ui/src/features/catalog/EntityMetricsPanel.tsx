// The metrics that measure one Catalog entity, over the selected window.
//
// Which metrics those are is the registry's answer, not this component's (see
// `useEntityMetrics`); which of them the window actually holds is the
// querier's. Nothing here names a metric, so an entity type nobody
// anticipated — including one a tenant's own registry introduces — gets the
// same panel with no code change.
import { useQuery } from "@tanstack/react-query";
import { fetchEntityMetricSeries } from "../../api/entityMetricSeries";
import { irSeriesToPromSeries } from "../../api/ir/metrics";
import { pinsKey, type EntityPin } from "../../api/catalog";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import type { ObservedMetric } from "../../api/entityMetrics";
import type { EntityTypeDef } from "./entityTypes";
import { MetricsChart } from "../metrics/MetricsChart";
import { SkeletonLines } from "../explore/Skeleton";
import { useEntityMetrics } from "./useEntityMetrics";
import "./catalog.css";

/**
 * Tiles drawn at once. A host associates 45 metrics in OTel 1.43, and a wall
 * of 45 charts is not a page anyone reads — but a truncated one that says
 * nothing reads as "this is everything", so the count is always stated.
 */
export const METRIC_TILE_CAP = 12;

/** Observed names listed in the "did not match" note before "and N more". */
const UNMATCHED_NAMES_SHOWN = 6;

/** Points per series. Enough shape to read a trend at tile size. */
const TILE_POINTS = 60;

interface Props {
  entity: EntityTypeDef;
  /** The entity's own identity pins — the same ones its RED aggregate uses. */
  pinned: EntityPin[];
  range: ResolvedRange;
  rangeKey: string;
}

export function EntityMetricsPanel({ entity, pinned, range, rangeKey }: Props) {
  const {
    metrics,
    isPending: lookupPending,
    isError: lookupFailed,
    error: lookupError,
    associated,
    unmatched,
  } = useEntityMetrics(entity, range, rangeKey);
  const names = metrics.map((m) => m.name).join(",");

  // `api/entityMetricSeries.ts` compiles every pin as a plain equality
  // match; it has no "not exists" case for a null pin the way
  // `buildEntitySourceDoc` does (see `EntityPin` in api/catalog.ts). Dropping
  // a null pin instead of compiling it would widen the series to every value
  // of that dimension while the KPIs above stay scoped to the absent value,
  // so the panel is hidden rather than shown scoped wrong.
  const hasUnsetPin = pinned.some((p) => p.value === null);

  const series = useQuery({
    queryKey: [
      "entity-metric-series",
      entity.id,
      rangeKey,
      names,
      pinsKey(pinned),
    ],
    queryFn: () =>
      fetchEntityMetricSeries(
        metrics,
        pinned,
        range,
        // The same snapped step the Metrics tab uses, so a tile and a chart of
        // the same metric bucket identically.
        durationToSeconds(stepForRange(range, TILE_POINTS)) ?? 60,
      ),
    enabled: metrics.length > 0 && !hasUnsetPin,
  });

  // Failing to ask which metrics describe this entity is not the same answer
  // as "none describe it", and rendering nothing for both hides the breakage.
  if (lookupFailed) {
    return (
      <QueryError
        what={`this ${entity.singular}'s metrics`}
        error={lookupError}
      />
    );
  }

  // Still asking: an empty list here is not yet an answer.
  if (lookupPending) return <SkeletonLines lines={4} />;

  // The registry describes this entity by metrics, and the window holds
  // metrics in the same namespaces, yet no name matches: the emitter is
  // probably spelling them differently. Say so, rather than render nothing.
  const namesMismatch = metrics.length === 0 && unmatched.length > 0;

  // Nothing the registry associates with this entity type — so there is no
  // panel to draw, rather than an empty one to explain.
  if (metrics.length === 0 && !namesMismatch) return null;

  if (hasUnsetPin && !namesMismatch) {
    return (
      <div className="view-note">
        Metrics are not shown for an unset identity dimension.
      </div>
    );
  }

  const observed = metrics.filter((m) => series.data?.has(m.name));
  const shown = observed.slice(0, METRIC_TILE_CAP);

  // Five states, named rather than nested: names did not match, failed, still
  // asking, asked and this window holds nothing, and charts.
  let body;
  if (namesMismatch) {
    body = (
      <UnmatchedNote
        entity={entity}
        associated={associated}
        unmatched={unmatched}
      />
    );
  } else if (series.isError) {
    body = <QueryError what="metric series" error={series.error} />;
  } else if (observed.length === 0) {
    body = series.isPending ? (
      <SkeletonLines lines={4} />
    ) : (
      <EmptyState title={`No metrics for this ${entity.singular} in this range`} />
    );
  } else {
    body = (
      <>
        <div className="metric-tiles">
          {shown.map((m) => (
            <MetricTile
              key={m.name}
              metric={m}
              series={series.data!.get(m.name)!}
            />
          ))}
        </div>
        {observed.length > shown.length && (
          <div className="view-note">
            Showing {shown.length} of {observed.length} metrics.
          </div>
        )}
      </>
    );
  }

  return (
    <section className="entity-metrics">
      <div className="catalog-headline">
        <span className="catalog-title">Metrics</span>
        <span className="catalog-sub">
          discovered from registry entity <code>{entity.registryEntity}</code>
        </span>
      </div>

      {body}
    </section>
  );
}

function UnmatchedNote({
  entity,
  associated,
  unmatched,
}: {
  entity: EntityTypeDef;
  associated: readonly string[];
  unmatched: readonly string[];
}) {
  return (
    <div className="view-note" role="status">
      <strong>Metric names did not match.</strong> The registry associates{" "}
      {associated.length} metrics with this {entity.singular} (for example{" "}
      <code>{associated[0]}</code>), and this range holds {unmatched.length} in
      the same namespace, but none share a name:{" "}
      {unmatched.slice(0, UNMATCHED_NAMES_SHOWN).map((n, i) => (
        <span key={n}>
          {i > 0 && ", "}
          <code>{n}</code>
        </span>
      ))}
      {unmatched.length > UNMATCHED_NAMES_SHOWN &&
        ` and ${unmatched.length - UNMATCHED_NAMES_SHOWN} more`}
      . If the emitter does not follow semantic conventions, list its names as{" "}
      <code>aliases</code> on the metric definitions in a schema registry.
    </div>
  );
}

function MetricTile({
  metric,
  series,
}: {
  metric: ObservedMetric;
  series: Parameters<typeof irSeriesToPromSeries>[0];
}) {
  return (
    <figure className="metric-tile">
      <figcaption>
        <span className="metric-tile-name" title={metric.name}>
          {metric.name}
        </span>
        {/* The instrument is not decoration: a cumulative counter charted as
            a level would otherwise read as a rate. */}
        <span
          className="metric-tile-meta"
          title={`${metric.instrument} · ${metric.unit}`}
        >
          {metric.instrument} · {metric.unit}
        </span>
      </figcaption>
      {metric.aliasOf && (
        <div className="metric-tile-alias" title={metric.aliasOf}>
          alias of {metric.aliasOf}
        </div>
      )}
      <MetricsChart
        series={irSeriesToPromSeries(series)}
        height={120}
        unit={metric.unit}
      />
    </figure>
  );
}
