// Client for the router's Loki-compatible label endpoints (/loki/api/v1),
// layered over the generated OpenAPI SDK — the UI never hand-writes the HTTP
// call for endpoints the SDK covers (see docs/architecture/openapi-codegen.md,
// "Adding or changing an endpoint"), mirroring api/queryIr.ts. Row/histogram
// queries moved to the Query IR (api/ir/logs.ts); the label pickers stay
// here until they move to `describe`.
import "./client";

import { logqlLabels, logqlLabelValues } from "./gen";
import { msToNanos, type ResolvedRange } from "../lib/time";
import { ApiError, retryAfterMsFrom, tenantHeaders } from "./http";

/**
 * Unwrap a generated-client result carrying `{resultType, result}` data.
 * Only checks the HTTP-level outcome (matching the original hand-written
 * client, which never inspected a body-level `status` field for these
 * endpoints) — an HTTP failure throws `ApiError` with the status embedded.
 */
function unwrapLoki<T>(
  res: { error?: unknown; data?: unknown; response?: Response },
  what: string,
): T {
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    const detail = typeof res.error === "string" ? `: ${res.error}` : "";
    throw new ApiError(
      `${what} failed (${status})${detail}`,
      status,
      retryAfterMsFrom(res.response),
    );
  }
  return (res.data as { data: T }).data;
}

export async function lokiLabels(range: ResolvedRange): Promise<string[]> {
  const res = await logqlLabels({
    query: { start: msToNanos(range.fromMs), end: msToNanos(range.toMs) },
    headers: tenantHeaders(),
  });
  return unwrapLoki<string[] | undefined>(res, "Loki labels") ?? [];
}

export async function lokiLabelValues(
  label: string,
  range: ResolvedRange,
): Promise<string[]> {
  const res = await logqlLabelValues({
    path: { name: label },
    query: { start: msToNanos(range.fromMs), end: msToNanos(range.toMs) },
    headers: tenantHeaders(),
  });
  return unwrapLoki<string[] | undefined>(res, "Loki label values") ?? [];
}
