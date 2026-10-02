// Live tail over the native Query IR endpoint (irVersion 15 `tail`): poll
// `POST /api/v1/query` with the previous response's cursor, keeping only
// what arrived since. Polls again at once while the server reports a backlog
// (`caught_up: false`), else every `intervalMs`. A failure backs off (as
// long as a 429 asks); an expired or rejected cursor (410/400) restarts.
import { useEffect, useState } from "react";

import type { QueryIrRequest, QueryIrResponse, QueryWarning } from "../api/gen";
import { ApiError } from "../api/http";
import { runIrQuery } from "../api/queryIr";

const MAX_BACKOFF_MS = 30_000;
/** `tail_lagged` warnings kept across polls. */
const KEPT_LAG_WARNINGS = 3;

export interface IrTail<T> {
  /** Newest first, at most `cap` entries. */
  rows: T[];
  warnings: QueryWarning[];
  error: unknown;
  /** Whether the first call has answered. */
  started: boolean;
}

export function useIrTail<T>(
  doc: QueryIrRequest | null,
  decode: (res: QueryIrResponse) => T[],
  cap: number,
  intervalMs = 2_000,
): IrTail<T> {
  const [state, setState] = useState<IrTail<T>>({
    rows: [],
    warnings: [],
    error: null,
    started: false,
  });
  // A changed document starts a new tail.
  const docKey = doc ? JSON.stringify(doc) : null;

  useEffect(() => {
    setState({ rows: [], warnings: [], error: null, started: false });
    if (!doc) return;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let cursor: string | undefined;
    let failures = 0;
    const tick = async () => {
      try {
        const res = await runIrQuery({
          ...doc,
          tail: { ...doc.tail, ...(cursor ? { cursor } : {}) },
        });
        if (cancelled) return;
        if (!res.tail) throw new Error("the server returned no tail cursor");
        cursor = res.tail.cursor;
        failures = 0;
        const fresh = decode(res).reverse();
        setState((prev) => ({
          rows: [...fresh, ...prev.rows].slice(0, cap),
          warnings: [
            ...prev.warnings
              .filter((w) => w.code === "tail_lagged")
              .slice(-KEPT_LAG_WARNINGS),
            ...(res.warnings ?? []),
          ],
          error: null,
          started: true,
        }));
        timer = setTimeout(tick, res.tail.caught_up ? intervalMs : 0);
      } catch (error) {
        if (cancelled) return;
        const status = error instanceof ApiError ? error.status : undefined;
        if (status === 410 || status === 400) cursor = undefined;
        failures += 1;
        const backoff = Math.min(intervalMs * 2 ** failures, MAX_BACKOFF_MS);
        const asked = error instanceof ApiError ? error.retryAfterMs : null;
        setState((prev) => ({ ...prev, error, started: true }));
        timer = setTimeout(tick, asked ?? backoff);
      }
    };
    void tick();
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // `doc` is captured through `docKey`; `decode` is a stable function.
  }, [docKey, cap, intervalMs]);

  return state;
}
