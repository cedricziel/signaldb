// Span kind (SERVER/CLIENT/INTERNAL/PRODUCER/CONSUMER) for the spans of one
// trace, over Query IR — deliberately NOT part of the Tempo-compat wire
// format (api/tempo.ts's WireSpan/TempoSpan/tempoGetTrace stay untouched);
// this is a separate enrichment merged in only at render time.

import { runIrQuery } from "./queryIr";
import { msToNanos } from "../lib/time";

function whereEq(field: string, value: string): Record<string, unknown> {
  return { where: { field, op: "eq", value } };
}

/** Span id -> span kind ("SERVER", "CLIENT", "INTERNAL", "PRODUCER",
 * "CONSUMER"), for every span in the given trace. `fromMs`/`toMs` should
 * bracket the trace's own time extent, not necessarily the viewer's
 * currently-selected explore range — a trace opened by pasting its ID can
 * fall outside it. */
export async function fetchSpanKinds(
  traceId: string,
  fromMs: number,
  toMs: number,
): Promise<Record<string, string>> {
  const res = await runIrQuery({
    irVersion: 1,
    from: "traces",
    range: { from: msToNanos(fromMs), to: msToNanos(toMs) },
    result: "rows",
    fields: ["span_id", "span_kind"],
    pipeline: [whereEq("trace_id", traceId)],
  });

  const kinds: Record<string, string> = {};
  for (const row of res.rows ?? []) {
    const [spanId, kind] = row;
    if (typeof spanId === "string" && typeof kind === "string") {
      kinds[spanId] = kind;
    }
  }
  return kinds;
}
