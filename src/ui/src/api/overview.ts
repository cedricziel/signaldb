// Queries behind the System Overview page (`/overview`): tenant-wide RED for
// root spans, per-service activity, deploys, ingest volume per signal, and
// the slowest endpoints. Everything is a Query IR read; the service list,
// error groups and service graph reuse `./catalog`, `./errors` and
// `./serviceGraph`, scoped by the same environment `where` stage built here.
import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { pinsWhere, spanKindWhere, type EntityPin } from "./catalog";
import {
  buildEntityCountSeriesDoc,
  fetchEntityKpis,
  type EntityKpis,
  type SeriesPoint,
} from "./entityDetailStats";
import { runIrQuery } from "./queryIr";
import { ROOT_SPAN_SENTINEL } from "./traceGroups";
import type { EntityTypeDef } from "../features/catalog/entityTypes";
import { toLokiLabel } from "../lib/labelSuggestions";
import { compositeKey } from "../lib/traceGroups";
import { msToNanos, type ResolvedRange } from "../lib/time";
import type { VolumeSeries } from "../components/SignalHistogram";

const NANOS_PER_MS = 1_000_000;

/** The resource attribute the environment picker filters on (OTel semconv
 * `deployment.environment.name`). */
export const ENV_FIELD = "deployment.environment.name";

/** The environment scope as entity pins — none for "every environment". */
export function envPins(env: string): EntityPin[] {
  return env ? [{ field: ENV_FIELD, value: env }] : [];
}

/** The environment scope as bare `where` stages. */
export function envWhere(env: string): Record<string, unknown>[] {
  return pinsWhere(envPins(env));
}

function rangeDoc(range: ResolvedRange) {
  return { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) };
}

function decodePoints(points: unknown[]): SeriesPoint[] {
  return points.flatMap((p): SeriesPoint[] => {
    const [tNs, v] = p as [unknown, unknown];
    if (typeof tNs !== "number" || typeof v !== "number") return [];
    return [{ tMs: Math.round(tNs / NANOS_PER_MS), value: v }];
  });
}

// ---- system KPIs --------------------------------------------------------

/** The whole tenant as one "entity": no identity, so every stats/series
 * builder in `./entityDetailStats` answers with a single row/series. */
const SYSTEM_ENTITY: EntityTypeDef = {
  id: "system",
  label: "System",
  singular: "system",
  identity: [],
};

/** Root spans only — `parent_span_id` is the all-zero sentinel on a root
 * (see `ROOT_SPAN_SENTINEL`), so request rate and p95 are end-to-end. */
const ROOT_PIN: EntityPin = {
  field: "parent_span_id",
  value: ROOT_SPAN_SENTINEL,
};

/** Request rate, error rate and p95 over root spans, for this window and
 * the one before it, with `stepSeconds`-bucketed series. */
export function fetchSystemKpis(
  range: ResolvedRange,
  env: string,
  stepSeconds: number,
): Promise<EntityKpis> {
  return fetchEntityKpis(
    SYSTEM_ENTITY,
    range,
    [ROOT_PIN, ...envPins(env)],
    stepSeconds,
  );
}

// ---- per-service activity ---------------------------------------------

/** One series per service (Server spans, as the catalog's Rate column
 * counts), keyed by the catalog's composite identity key. */
export async function fetchServiceActivity(
  service: EntityTypeDef,
  range: ResolvedRange,
  env: string,
  stepSeconds: number,
): Promise<Map<string, SeriesPoint[]>> {
  const res = await runIrQuery(
    buildEntityCountSeriesDoc(service, range, envPins(env), stepSeconds, false),
  );
  return seriesByIdentity(res, service.identity);
}

/** Series indexed by the composite of their identity labels — the series
 * envelope sanitizes label names (`service.name` → `service_name`). */
export function seriesByIdentity(
  res: QueryIrResponse,
  identity: string[],
): Map<string, SeriesPoint[]> {
  const out = new Map<string, SeriesPoint[]>();
  for (const s of res.series ?? []) {
    const key = compositeKey(
      identity.map((f) => s.labels[toLokiLabel(f)] ?? null),
    );
    out.set(key, decodePoints(s.points));
  }
  return out;
}

// ---- deploys -----------------------------------------------------------

export interface VersionSighting {
  service: string;
  version: string;
  firstMs: number;
  lastMs: number;
}

export interface Deploy {
  service: string;
  version: string;
  /** When the new version's first span arrived. */
  atMs: number;
}

/** Every (service, version) pair seen in the window, with its first and
 * last span. A deploy is read off these: see `deploysFromSightings`. */
export function buildVersionSightingsDoc(
  range: ResolvedRange,
  env: string,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      { where: { field: "service.version", op: "exists" } },
      ...envWhere(env),
      {
        aggregate: {
          by: ["service.name", "service.version"],
          aggs: [
            { fn: "min", of: "start_time_unix_nano", as: "first" },
            { fn: "max", of: "start_time_unix_nano", as: "last" },
          ],
        },
      },
      { order: [{ of: "first", dir: "asc" }] },
      { limit: 1000 },
    ],
  };
}

/** Decodes `[service, version, first, last]` rows positionally — the server
 * answers with physical column names. */
export function decodeVersionSightings(
  res: QueryIrResponse,
): VersionSighting[] {
  return (res.rows ?? []).flatMap((row): VersionSighting[] => {
    const [service, version, first, last] = row as unknown[];
    if (typeof service !== "string" || typeof version !== "string") return [];
    if (typeof first !== "number" || typeof last !== "number") return [];
    return [
      {
        service,
        version,
        firstMs: Math.round(first / NANOS_PER_MS),
        lastMs: Math.round(last / NANOS_PER_MS),
      },
    ];
  });
}

/**
 * There's no deploy event stream, so a deploy is inferred from spans: a
 * service's version whose first span in the window comes after another
 * version of the same service was already reporting. A service seen with a
 * single version, or the earliest version of each service, is not a deploy
 * — it may simply have been running since before the window opened.
 */
export function deploysFromSightings(sightings: VersionSighting[]): Deploy[] {
  const byService = new Map<string, VersionSighting[]>();
  for (const s of sightings) {
    byService.set(s.service, [...(byService.get(s.service) ?? []), s]);
  }
  const deploys: Deploy[] = [];
  for (const [service, versions] of byService) {
    const sorted = [...versions].sort((a, b) => a.firstMs - b.firstMs);
    for (const v of sorted.slice(1)) {
      deploys.push({ service, version: v.version, atMs: v.firstMs });
    }
  }
  return deploys.sort((a, b) => a.atMs - b.atMs);
}

/** The newest version each service reported in the window. */
export function latestVersions(
  sightings: VersionSighting[],
): Map<string, VersionSighting> {
  const out = new Map<string, VersionSighting>();
  for (const s of sightings) {
    const prev = out.get(s.service);
    if (!prev || s.firstMs > prev.firstMs) out.set(s.service, s);
  }
  return out;
}

export async function fetchVersionSightings(
  range: ResolvedRange,
  env: string,
): Promise<VersionSighting[]> {
  return decodeVersionSightings(
    await runIrQuery(buildVersionSightingsDoc(range, env)),
  );
}

// ---- ingest volume -----------------------------------------------------

export type IngestSignal = "logs" | "traces" | "metrics" | "profiles";

export const INGEST_SIGNALS: IngestSignal[] = [
  "logs",
  "traces",
  "metrics",
  "profiles",
];

/** Sources counted under each signal — histogram metrics are a separate
 * source but the same signal to a reader. */
const INGEST_SOURCES: Record<IngestSignal, string[]> = {
  logs: ["logs"],
  traces: ["traces"],
  metrics: ["metrics", "metrics_histogram"],
  profiles: ["profiles"],
};

export function buildRecordCountDoc(
  source: string,
  range: ResolvedRange,
  env: string,
  stepSeconds: number,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: source,
    range: rangeDoc(range),
    result: "series",
    pipeline: [
      ...envWhere(env),
      {
        aggregate: {
          by: [],
          aggs: [{ fn: "count", as: "n" }],
          step: `${stepSeconds}s`,
        },
      },
    ],
  };
}

/**
 * Records accepted per signal per step. The server exposes no per-tenant
 * byte counts to the UI, so ingest is measured in records (spans, log
 * records, metric points, profiles). A source that fails (profiles not
 * enabled for the tenant, say) counts as empty rather than failing the
 * whole chart.
 */
export async function fetchIngestVolume(
  range: ResolvedRange,
  env: string,
  stepSeconds: number,
): Promise<VolumeSeries[]> {
  return Promise.all(
    INGEST_SIGNALS.map(async (signal): Promise<VolumeSeries> => {
      const perSource = await Promise.all(
        INGEST_SOURCES[signal].map((source) =>
          runIrQuery(buildRecordCountDoc(source, range, env, stepSeconds))
            .then((res) => decodePoints(res.series?.[0]?.points ?? []))
            .catch(() => [] as SeriesPoint[]),
        ),
      );
      const byT = new Map<number, number>();
      for (const p of perSource.flat()) {
        byT.set(p.tMs, (byT.get(p.tMs) ?? 0) + p.value);
      }
      return {
        key: signal,
        points: [...byT.entries()].sort((a, b) => a[0] - b[0]),
      };
    }),
  );
}

// ---- slowest endpoints -------------------------------------------------

export interface Endpoint {
  name: string;
  service: string | null;
  count: number;
  p95Ms: number;
  p99Ms: number;
}

/** Server spans grouped by operation and service, slowest p95 first. */
export function buildSlowestEndpointsDoc(
  range: ResolvedRange,
  env: string,
  limit: number,
): QueryIrRequest {
  return {
    irVersion: 1,
    from: "traces",
    range: rangeDoc(range),
    result: "table",
    pipeline: [
      ...spanKindWhere("Server"),
      ...envWhere(env),
      {
        aggregate: {
          by: ["span.name", "service.name"],
          aggs: [
            { fn: "count", as: "n" },
            { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
            { fn: "quantile", of: "duration", arg: 0.99, as: "p99" },
          ],
        },
      },
      { order: [{ of: "p95", dir: "desc" }] },
      { limit },
    ],
  };
}

export function decodeEndpoints(res: QueryIrResponse): Endpoint[] {
  return (res.rows ?? []).flatMap((row): Endpoint[] => {
    const [name, service, n, p95, p99] = row as unknown[];
    if (typeof name !== "string") return [];
    const num = (v: unknown) => (typeof v === "number" ? v : 0);
    return [
      {
        name,
        service: typeof service === "string" ? service : null,
        count: num(n),
        p95Ms: num(p95) / NANOS_PER_MS,
        p99Ms: num(p99) / NANOS_PER_MS,
      },
    ];
  });
}

export async function fetchSlowestEndpoints(
  range: ResolvedRange,
  env: string,
  limit = 6,
): Promise<Endpoint[]> {
  return decodeEndpoints(
    await runIrQuery(buildSlowestEndpointsDoc(range, env, limit)),
  );
}
