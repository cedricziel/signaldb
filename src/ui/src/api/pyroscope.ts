// Client for the router's Pyroscope-compatible profiles API (/pyroscope).

import type { ResolvedRange } from "../lib/time";
import { ApiError, tenantHeaders } from "./http";

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
  const res = await fetch(`/pyroscope/${path}${query}`, {
    headers: tenantHeaders(),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new ApiError(
      `Pyroscope API ${path} failed (${res.status}): ${body.slice(0, 300)}`,
      res.status,
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
  const params = rangeParams(range);
  params.set("label", "service_name");
  const res = await pyroscopeFetch<{ names: string[] }>("label-values", params);
  return res.names;
}

/**
 * Render a flamebearer for a profile type, optionally filtered to a single
 * service (the only matcher the router honors). `typeId` is a ProfileType
 * `ID` such as `cpu:nanoseconds`.
 */
export async function pyroscopeRender(
  typeId: string,
  service: string,
  range: ResolvedRange,
): Promise<RenderResponse> {
  const query = service ? `${typeId}{service_name="${service}"}` : typeId;
  const params = rangeParams(range);
  params.set("query", query);
  return pyroscopeFetch<RenderResponse>("render", params);
}
