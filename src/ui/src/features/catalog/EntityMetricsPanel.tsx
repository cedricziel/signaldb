// The metrics that measure one Catalog entity, over the selected window.
//
// Which metrics those are is the registry's answer, not this component's (see
// `useEntityMetrics`); which of them the window actually holds is the
// querier's. Nothing here names a metric, so an entity type nobody
// anticipated — including one a tenant's own registry introduces — gets the
// same panel with no code change.
import { useQuery } from "@tanstack/react-query";
import { fetchEntityMetricSeries } from "../../api/entityMetricSeries";
import { irSeriesToPromSeries } from "../../api/metricsIr";
import { pinsKey, type EntityPin } from "../../api/catalog";
import {
  durationToSeconds,
  stepForRange,
  type ResolvedRange,
} from "../../lib/time";
import type { MetricHit } from "../schema/api";
import type { EntityTypeDef } from "./entityTypes";
import { MetricsChart } from "../metrics/MetricsChart";
import { useEntityMetrics } from "./useEntityMetrics";
import "./catalog.css";

/**
 * Tiles drawn at once. A host associates 45 metrics in OTel 1.43, and a wall
 * of 45 charts is not a page anyone reads — but a truncated one that says
 * nothing reads as "this is everything", so the count is always stated.
 */
export const METRIC_TILE_CAP = 12;

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
  const { metrics, isError: lookupFailed } = useEntityMetrics(
    entity,
    range,
    rangeKey,
  );
  const names = metrics.map((m) => m.name).join(",");

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
    enabled: metrics.length > 0,
  });

  // Failing to ask which metrics describe this entity is not the same answer
  // as "none describe it", and rendering nothing for both hides the breakage.
  if (lookupFailed) {
    return (
      <div className="query-error" role="alert">
        Could not look up which metrics describe this {entity.singular}.
      </div>
    );
  }

  // Nothing the registry associates with this entity type — so there is no
  // panel to draw, rather than an empty one to explain.
  if (metrics.length === 0) return null;

  const observed = metrics.filter((m) => series.data?.has(m.name));
  const shown = observed.slice(0, METRIC_TILE_CAP);

  // Four states, named rather than nested: failed, still asking, asked and
  // this window holds nothing, and charts.
  let body;
  if (series.isError) {
    body = (
      <div className="query-error" role="alert">
        Failed to load: {(series.error as Error).message}
      </div>
    );
  } else if (observed.length === 0) {
    body = series.isPending ? null : (
      <div className="view-note">
        No metric data for this {entity.singular} in this window.
      </div>
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

function MetricTile({
  metric,
  series,
}: {
  metric: MetricHit;
  series: Parameters<typeof irSeriesToPromSeries>[0];
}) {
  return (
    <figure className="metric-tile">
      <figcaption>
        <span className="metric-tile-name">{metric.name}</span>
        {/* The instrument is not decoration: a cumulative counter charted as
            a level would otherwise read as a rate. */}
        <span className="metric-tile-meta">
          {metric.instrument} · {metric.unit}
        </span>
      </figcaption>
      <MetricsChart
        series={irSeriesToPromSeries(series)}
        height={120}
        unit={metric.unit}
      />
    </figure>
  );
}
