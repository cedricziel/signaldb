// Flamegraph data for the Profiles tab, exclusively over the native Query IR
// API — mirrors the mcp-server's `get_profile` tool, which submits the same
// `flamegraph`-enveloped `profiles` query shape. Discovery (which services,
// sample types, or attribute keys/values exist) has no Query IR equivalent
// yet, so that stays on the Pyroscope-compat endpoints in api/pyroscope.ts;
// this module is the only thing that actually renders a flamegraph.

import { runIrQuery } from "./queryIr";
import { ApiError } from "./http";
import { msToNanos, type ResolvedRange } from "../lib/time";
import type { RenderResponse } from "./pyroscope";

/** An additional attribute matcher beyond service/sample-type — the field
 * is a bare attribute key, which Query IR coalesces across the profile's
 * resource/scope/profile-level attribute containers. */
export interface AttributeMatcher {
  label: string;
  value: string;
}

export interface FlamegraphQuery {
  service?: string;
  sampleType?: string;
  matcher?: AttributeMatcher;
}

export interface FlamegraphFetch {
  render: RenderResponse;
  /** True when the match set exceeded the server's per-query row cap and
   * the flamegraph only reflects the first N profiles matched. */
  truncated: boolean;
}

function whereEq(field: string, value: string): Record<string, unknown> {
  return { where: { field, op: "eq", value } };
}

function toRenderResponse(
  fg: { names: string[]; levels: number[][]; total: number; max_self: number },
  name: string,
): RenderResponse {
  return {
    flamebearer: {
      names: fg.names,
      levels: fg.levels,
      numTicks: fg.total,
      maxSelf: fg.max_self,
    },
    metadata: {
      format: "single",
      sampleRate: 100,
      units: "samples",
      name,
    },
  };
}

/** Aggregate every profile matching the query into one flamegraph. */
export async function fetchFlamegraph(
  range: ResolvedRange,
  query: FlamegraphQuery,
): Promise<FlamegraphFetch> {
  const pipeline: Record<string, unknown>[] = [];
  if (query.service) pipeline.push(whereEq("service.name", query.service));
  if (query.sampleType) pipeline.push(whereEq("sample.type", query.sampleType));
  if (query.matcher?.label) {
    pipeline.push(whereEq(query.matcher.label, query.matcher.value));
  }

  const res = await runIrQuery({
    irVersion: 1,
    from: "profiles",
    range: { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) },
    result: "flamegraph",
    pipeline,
  });
  if (!res.flamegraph) {
    throw new ApiError("profiles query returned no flamegraph envelope", 500);
  }
  return {
    render: toRenderResponse(res.flamegraph, query.sampleType ?? ""),
    truncated: res.flamegraph.truncated,
  };
}

/**
 * Fetch one exact profile by id — the render path behind a trace's linked
 * "Profile for this span" action. Scoped to the last 30 days (matching the
 * mcp-server default): `profile.id` is an exact-match filter, so precise
 * time bounds aren't needed for correctness, only to keep the scan bounded.
 */
export async function fetchFlamegraphById(
  profileId: string,
): Promise<RenderResponse> {
  const res = await runIrQuery({
    irVersion: 1,
    from: "profiles",
    range: { from: "now-30d", to: "now" },
    result: "flamegraph",
    pipeline: [whereEq("profile.id", profileId)],
  });
  if (!res.flamegraph || res.flamegraph.names.length === 0) {
    throw new ApiError(`Profile ${profileId} not found`, 404);
  }
  return toRenderResponse(res.flamegraph, profileId);
}
