// Client for the router's Pyroscope-compatible discovery endpoints
// (/pyroscope/profile-types, /label-names, /label-values), layered over the
// generated OpenAPI SDK — the UI never hand-writes the HTTP call for
// endpoints the SDK covers (see docs/architecture/openapi-codegen.md,
// "Adding or changing an endpoint"), mirroring api/loki.ts. Actual flamegraph
// data is fetched through the native Query IR API (see api/profilesIr.ts) —
// these compat endpoints stay only because there's no Query IR equivalent
// for "what distinct values does this signal have" yet.
import "./client";

import {
  pyroscopeLabelNames as apiPyroscopeLabelNames,
  pyroscopeLabelValues as apiPyroscopeLabelValues,
  pyroscopeProfileTypes as apiPyroscopeProfileTypes,
} from "./gen";
import type { ResolvedRange } from "../lib/time";
import { ApiError, retryAfterMsFrom, tenantHeaders } from "./http";

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

/**
 * Unwrap a generated-client result, throwing `ApiError` on an HTTP-level
 * failure (matching the original hand-written client's behavior for these
 * endpoints — it never inspected a body-level `status` field).
 */
function unwrap<T>(
  what: string,
  res: { error?: unknown; data?: T; response?: Response },
): T {
  if (res.error || res.data === undefined) {
    const status = res.response?.status ?? 500;
    const detail = typeof res.error === "string" ? `: ${res.error}` : "";
    throw new ApiError(
      `Pyroscope API ${what} failed (${status})${detail}`,
      status,
      retryAfterMsFrom(res.response),
    );
  }
  return res.data;
}

/** The router's time params are unix seconds. */
function rangeQuery(range: ResolvedRange): { from: string; until: string } {
  return {
    from: String(Math.floor(range.fromMs / 1000)),
    until: String(Math.ceil(range.toMs / 1000)),
  };
}

export async function pyroscopeProfileTypes(
  range: ResolvedRange,
): Promise<ProfileType[]> {
  const res = await apiPyroscopeProfileTypes({
    query: rangeQuery(range),
    headers: tenantHeaders(),
  });
  return unwrap("profile-types", res);
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
  const res = await apiPyroscopeLabelNames({
    query: rangeQuery(range),
    headers: tenantHeaders(),
  });
  return unwrap("label-names", res).names.filter((n) => n !== "service_name");
}

/** Distinct values of a label (attribute key) in the range. */
export async function pyroscopeLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const res = await apiPyroscopeLabelValues({
    query: { ...rangeQuery(range), label },
    headers: tenantHeaders(),
  });
  return unwrap("label-values", res).names;
}
