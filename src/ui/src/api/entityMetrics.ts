/**
 * The metrics that describe a Catalog entity.
 *
 * Two questions, asked of whoever can answer them, then intersected: the
 * registry says which metrics describe an entity (recorded on the entity, so
 * it is read rather than reconstructed), and the window says which metrics
 * exist. Only their intersection can be charted. A definition may declare
 * other names emitters report it under (`aliases`), and an observed alias
 * counts as observed: the tile is drawn under the name the emitter wrote.
 *
 * The definition lookup — instrument and unit, which the tiles need — is the
 * one step still shaped by an API limit: `/api/v1/schema/metrics` searches by
 * name prefix only, so definitions are collected a name-family at a time and
 * narrowed back. Asking after the intersection keeps that to a call or two.
 */
import type { IrStage, QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos, type ResolvedRange } from "../lib/time";
import {
  resolveEntity,
  searchMetrics,
  type MetricHit,
} from "../features/schema/api";

export const METRICS_SOURCE = "metrics";

export const HISTOGRAM_ROWS: IrStage = {
  where: { field: "metric.type", op: "eq", value: "histogram" },
};

export const NON_SCALAR_METRIC_TYPES = [
  "histogram",
  "exponential_histogram",
  "summary",
];

/**
 * A registry definition as observed in a window.
 *
 * `name` is the name the window holds — what the series are queried and
 * labelled by — which is an alias when `aliasOf` names the definition's own.
 */
export type ObservedMetric = MetricHit & { aliasOf?: string };

/** One response's series, as the IR envelope carries them. */
export type IrSeries = NonNullable<QueryIrResponse["series"]>;

/**
 * Whether a metric is charted through the quantile stage. A histogram row's
 * `metric.value` is null, so a scalar aggregate over it charts nothing.
 * Exponential histograms are left out: the stage refuses them until
 * exponential-histogram quantiles land.
 */
export function isHistogram(instrument: string): boolean {
  return instrument === "histogram";
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
 * The metric names this registry entity is described by.
 *
 * The association is recorded on the entity, so it can be read directly
 * rather than reconstructed: every visible registry's answer, merged. That
 * matters beyond convenience — a host is described by `nfs.*` as much as by
 * `system.*`, a family no amount of prefix-guessing from the entity's own
 * name would reach.
 */
export async function fetchEntityMetricNames(
  registryEntity: string,
): Promise<string[]> {
  const resolution = await resolveEntity(registryEntity);
  const names = new Set<string>();
  for (const hit of resolution.hits) {
    for (const name of hit.metrics ?? []) names.add(name);
  }
  return [...names];
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
  return [...new Set(names.map(firstSegment))];
}

/** A metric name's namespace: everything before the first `.` or `_`. */
function firstSegment(name: string): string {
  return name.split(/[._]/)[0]!;
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
 * The definitions' names, and their aliases, that the window holds.
 *
 * A definition whose canonical name and alias were both observed yields both:
 * they are two series of real data. A name claimed by several definitions is
 * charted once, under the first.
 */
export function matchObservedMetrics(
  definitions: MetricHit[],
  observed: string[],
): ObservedMetric[] {
  const inWindow = new Set(observed);
  const seen = new Set<string>();
  const out: ObservedMetric[] = [];
  for (const def of definitions) {
    for (const name of [def.name, ...(def.aliases ?? [])]) {
      if (!inWindow.has(name) || seen.has(name)) continue;
      seen.add(name);
      out.push(name === def.name ? def : { ...def, name, aliasOf: def.name });
    }
  }
  return out;
}

/**
 * Observed names in the namespaces the entity's metrics live in.
 *
 * Only meaningful when nothing matched: it is the evidence that the emitter
 * is writing this entity's metrics under names the registry does not know,
 * as opposed to not writing them at all. Namespaces are first name segments,
 * split the same way `nameSegments` does.
 */
export function unmatchedObservedNames(
  associated: string[],
  observed: string[],
): string[] {
  const namespaces = new Set(nameSegments(associated));
  return observed.filter((name) => namespaces.has(firstSegment(name))).sort();
}
