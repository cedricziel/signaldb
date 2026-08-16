// Client for the router's Pyroscope-compatible discovery endpoints
// (/pyroscope/profile-types, /label-names, /label-values). Actual flamegraph
// data is fetched through the native Query IR API (see api/profilesIr.ts) —
// these compat endpoints stay only because there's no Query IR equivalent
// for "what distinct values does this signal have" yet.

import type { ResolvedRange } from "../lib/time";
import {
  ApiError,
  retryAfterMsFrom,
  retryingFetch,
  tenantHeaders,
} from "./http";

/** A profile kind, e.g. `{ID: "cpu:nanoseconds", sampleType: "cpu", ...}`. */
export interface ProfileType {
  ID: string;
  name: string;
  sampleType: string;
  sampleUnit: string;
  periodType?: string;
  periodUnit?: string;
}

/**
 * Flamebearer profile, as Grafana/Pyroscope render it. `levels` is a
 * flattened tree, one array per depth: each frame is a delta-encoded
 * quadruple `[offset, total, self, nameIndex]` where `offset` is the gap in
 * ticks from the end of the previous frame at that level, and `nameIndex`
 * points into `names`.
 */
export interface Flamebearer {
  names: string[];
  levels: number[][];
  numTicks: number;
  maxSelf: number;
}

export interface RenderResponse {
  flamebearer: Flamebearer;
  metadata: {
    format: "single" | "double";
    spyName?: string;
    sampleRate: number;
    units: string;
    name: string;
  };
}

async function pyroscopeFetch<T>(
  path: string,
  params: URLSearchParams,
): Promise<T> {
  const query = params.size > 0 ? `?${params}` : "";
  const res = await retryingFetch(`/pyroscope/${path}${query}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Pyroscope API ${path} failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
      retryAfterMsFrom(res),
    );
  }
  return (await res.json()) as T;
}

/** The router's time params are unix seconds. */
function rangeParams(range: ResolvedRange): URLSearchParams {
  return new URLSearchParams({
    from: String(Math.floor(range.fromMs / 1000)),
    until: String(Math.ceil(range.toMs / 1000)),
  });
}

export async function pyroscopeProfileTypes(
  range: ResolvedRange,
): Promise<ProfileType[]> {
  return pyroscopeFetch<ProfileType[]>("profile-types", rangeParams(range));
}

/** Distinct `service_name` label values in the range. */
export async function pyroscopeServices(
  range: ResolvedRange,
): Promise<string[]> {
  return pyroscopeLabelValues("service_name", range);
}

/** Label (attribute key) names in the range, excluding `service_name` —
 * that's its own dedicated selector, not a matcher choice. */
export async function pyroscopeLabelNames(
  range: ResolvedRange,
): Promise<string[]> {
  const res = await pyroscopeFetch<{ names: string[] }>(
    "label-names",
    rangeParams(range),
  );
  return res.names.filter((n) => n !== "service_name");
}

/** Distinct values of a label (attribute key) in the range. */
export async function pyroscopeLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const params = rangeParams(range);
  params.set("label", label);
  const res = await pyroscopeFetch<{ names: string[] }>("label-values", params);
  return res.names;
}
