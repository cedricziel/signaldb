// Native Query IR client, layered over the generated OpenAPI SDK. The UI never
// hand-writes the HTTP call — it delegates to the generated `queryIr` operation
// and unwraps the result envelope, mirroring api/management.ts.
import "./client";

import { queryIr, type QueryIrRequestBody, type QueryIrResponse } from "./gen";
import { ApiError, retryAfterMsFrom, tenantHeaders } from "./http";
import { msToNanos, type ResolvedRange } from "../lib/time";

/** Submit a single-document or multi-query (formula) IR request and return
 * the enveloped result — both shapes respond with the same `QueryIrResponse`
 * envelope, discriminated at the request level by the presence of `queries`
 * (see `QueryIrRequestBody`). */
export async function runIrQuery(
  doc: QueryIrRequestBody,
): Promise<QueryIrResponse> {
  const res = await queryIr({ body: doc, headers: tenantHeaders() });
  if (res.error || !res.data) {
    const status = res.response?.status ?? 500;
    // `error` is the rate-limited/`ApiErrorBody` envelope's human-readable
    // field (see `ApiErrorBody` in `./gen`), never a bare string — but the
    // request can also fail before a body decodes, so check defensively.
    const error: unknown = res.error;
    const detail =
      error && typeof error === "object" && "error" in error
        ? String((error as { error: unknown }).error)
        : undefined;
    throw new ApiError(
      detail ?? `IR query failed (${status})`,
      status,
      retryAfterMsFrom(res.response),
    );
  }
  return res.data;
}

/** A `rows`/`table` response row keyed by column name. */
export type IrRow = Record<string, unknown>;

/** Rows as objects keyed by the response's column names. */
export function namedRows(res: QueryIrResponse): IrRow[] {
  const names = (res.columns ?? []).map((c) => c.name);
  return (res.rows ?? []).map((row) => {
    const cells = row as unknown[];
    const out: IrRow = {};
    names.forEach((n, i) => {
      out[n] = cells[i];
    });
    return out;
  });
}

/** The response column a grouped or projected logical field comes back
 * under: its dots become underscores (`gen_ai.agent.name` →
 * `gen_ai_agent_name`). An aggregate's column is its `as`. */
export function irColumn(field: string): string {
  return field.replace(/\./g, "_");
}

/** A document's `range`, in nanosecond strings. */
export function rangeDoc(range: ResolvedRange): { from: string; to: string } {
  return { from: msToNanos(range.fromMs), to: msToNanos(range.toMs) };
}

export interface IrPoint {
  tMs: number;
  value: number;
}

/** A `series` envelope's `[t_ns, value]` points, dropping gaps. */
export function decodePoints(points: unknown[]): IrPoint[] {
  return points.flatMap((p): IrPoint[] => {
    const [tNs, v] = p as [unknown, unknown];
    if (typeof tNs !== "number" || typeof v !== "number") return [];
    return [{ tMs: Math.round(tNs / 1_000_000), value: v }];
  });
}
