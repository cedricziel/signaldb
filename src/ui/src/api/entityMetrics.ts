/**
 * The metrics that describe a Catalog entity.
 *
 * The schema registry knows which entity a metric measures
 * (`entity_associations`), but cannot be asked the question in that
 * direction: `/api/v1/schema/metrics` takes a name prefix and a clamped
 * limit, with no cursor and no association filter (see #1360). Nor is the
 * entity's own name a usable prefix — a host's metrics are `system.*`.
 *
 * So the join runs from the data toward the registry: discover which metric
 * names the window actually holds, look those up, and keep the ones the
 * entity associates with. Every step is bounded by what the tenant emits
 * rather than by how large the registry is.
 */
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { searchMetrics, type MetricHit } from "../features/schema/api";
import type { EntityTypeDef } from "../features/catalog/entityTypes";

/**
 * The IR sources metric points live in, by the shape of the row.
 *
 * Two sources for one OTel signal: a `metrics` row is a scalar sample, a
 * `metrics_histogram` row is a whole bucketed histogram. They are named by
 * role rather than listed, because which one a metric belongs to is decided
 * per metric from its instrument — asking the wrong one is not a slow query
 * but a rejected one.
 */
export const METRIC_SOURCES = {
  scalar: "metrics",
  histogram: "metrics_histogram",
} as const;

/** One response's series, as the IR envelope carries them. */
export type IrSeries = NonNullable<QueryIrResponse["series"]>;

/**
 * Whether a metric's rows live in the histogram source.
 *
 * A `metrics_histogram` row is a whole bucketed histogram rather than a
 * scalar, so it carries no value column at all: a scalar aggregate over it is
 * rejected outright ("requires a numeric field, got string"). The instrument
 * the registry declares is what says which source can answer for a metric, so
 * nothing is ever asked of a source that cannot.
 */
export function isHistogram(instrument: string): boolean {
  return instrument === "histogram" || instrument === "exponentialhistogram";
}

/**
 * The quantile charted for a histogram. Buckets have no single level to plot,
 * so the tail is charted — the number a duration histogram exists to answer.
 *
 * One constant, because the entity page's tiles and the list's sparkline
 * column chart the same metric for the same entity: two would be free to
 * disagree, and the disagreement would be invisible.
 */
export const HISTOGRAM_QUANTILE = 0.95;

/**
 * How to collapse a step's worth of points, per instrument.
 *
 * A gauge and an updowncounter are levels: their average over the step is the
 * level. A counter is cumulative, and averaging it produces a number that is
 * neither the count nor the rate — its step maximum at least stays monotonic
 * and readable as a total. The honest fix for counters is a rate stage in the
 * IR, which does not exist yet (see design.md, Risks).
 */
export function aggFor(instrument: string): "avg" | "max" {
  return instrument === "counter" ? "max" : "avg";
}

/**
 * Every distinct metric name in the window, as an IR aggregate.
 *
 * No `where` clause: the point is to learn what exists, and the entity's own
 * pins would answer a different question — a metric can carry a resource
 * attribute without being *about* that entity.
 */
export function buildObservedMetricNamesDoc(
  source: string,
  range: ResolvedRange,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: source,
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "table",
    pipeline: [
      { aggregate: { by: ["metric.name"], aggs: [{ fn: "count", as: "n" }] } },
    ],
  };
}

/** The window's observed metric names. Empty means "nothing written in this
 * window", never "this tenant has no metrics" — the caller can widen. */
export async function discoverObservedMetricNames(
  source: string,
  range: ResolvedRange,
): Promise<string[]> {
  const res = await runIrQuery(buildObservedMetricNamesDoc(source, range));
  return (res.rows ?? []).map((row) => String(row[0]));
}

/**
 * The distinct first name segments, in first-seen order — the unit the
 * registry can be searched by, since its endpoint takes a prefix.
 *
 * Split on the dot *and* the underscore, because both spell a namespace: OTel
 * names are dotted (`system.cpu.time`) but anything scraped from a Prometheus
 * exporter is not (`otelcol_exporter_sent_spans`). Splitting on the dot alone
 * turned a real deployment's 80 observed names into 39 prefixes — one request
 * per collector metric, which is the fan-out batching by prefix exists to
 * prevent. The search is a plain string prefix, so the first word covers the
 * whole family; over-broad prefixes only widen a response that is filtered
 * back to the observed names anyway.
 */
export function nameSegments(names: string[]): string[] {
  const seen = new Set<string>();
  for (const name of names) {
    seen.add(name.split(/[._]/)[0]!);
  }
  return [...seen];
}

/**
 * Definitions for the given metric names, one prefix search per segment.
 *
 * A search answers with the registry's whole namespace for that prefix, so
 * the result is narrowed back to the names asked for — a deployment that
 * writes `system.cpu.time` must not be told it also has the other 44
 * `system.*` metrics semconv declares.
 */
export async function fetchMetricDefinitions(
  names: string[],
): Promise<MetricHit[]> {
  if (names.length === 0) return [];
  const wanted = new Set(names);
  const bySegment = await Promise.all(
    nameSegments(names).map((segment) => searchMetrics(segment)),
  );
  return bySegment.flat().filter((def) => wanted.has(def.name));
}

/**
 * The definitions that measure this entity type, by the registry's own
 * association.
 *
 * An entity type no visible registry declares carries no registry name, so
 * there is nothing to associate against and nothing to draw — the same answer
 * as an entity type the registry declares but associates no metric with.
 */
export function metricsForEntity(
  definitions: MetricHit[],
  entity: EntityTypeDef,
): MetricHit[] {
  const name = entity.registryEntity;
  if (name === undefined) return [];
  return definitions.filter((def) => def.entity_associations.includes(name));
}
