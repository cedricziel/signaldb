// Ingest verification for the Instrumentation page's "Verification" section:
// one Query IR count-in-window per signal, so "is telemetry actually
// arriving" is answered off the same query surface as everything else in the
// UI (see docs/users/querying-ir.md) rather than a bespoke check.

import type { QueryIrRequest, QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { msToNanos } from "../lib/time";

/** The Instrumentation page's four verified signals, in display order. The
 * profiles source name mirrors profilesIr.ts's flamegraph queries. */
export const INGEST_STATUS_SIGNALS = [
  "traces",
  "logs",
  "metrics",
  "profiles",
] as const;

export type IngestStatusSignal = (typeof INGEST_STATUS_SIGNALS)[number];

/** The verification window: recent enough that "Receiving" reflects data
 * arriving right now, not a source that stopped hours ago. */
export const INGEST_STATUS_WINDOW_MS = 15 * 60_000;

/** Build the count-in-window IR document for one signal. */
export function buildIngestStatusDoc(
  signal: IngestStatusSignal,
  nowMs: number = Date.now(),
): QueryIrRequest {
  return {
    irVersion: 1,
    from: signal,
    range: {
      from: msToNanos(nowMs - INGEST_STATUS_WINDOW_MS),
      to: msToNanos(nowMs),
    },
    result: "table",
    pipeline: [{ aggregate: { aggs: [{ fn: "count", as: "n" }] } }],
  };
}

/** Decode the `table` envelope's single count cell — absent (no rows, an
 * empty window) reads as zero, never an error. */
function countFromResponse(res: QueryIrResponse): number {
  const cell = res.rows?.[0]?.[0];
  return typeof cell === "number" ? cell : 0;
}

/** Count of `signal` records ingested in the last
 * {@link INGEST_STATUS_WINDOW_MS}. */
export async function fetchIngestStatus(
  signal: IngestStatusSignal,
  nowMs?: number,
): Promise<number> {
  const doc = buildIngestStatusDoc(signal, nowMs);
  return countFromResponse(await runIrQuery(doc));
}
