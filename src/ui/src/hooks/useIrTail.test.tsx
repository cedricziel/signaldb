import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import type { QueryIrRequest, QueryIrResponse } from "../api/gen";
import { client } from "../api/gen/client.gen";
import { resetApiClient, stubApiFetch } from "../test/apiClient";
import { useIrTail } from "./useIrTail";

afterEach(() => resetApiClient());

const doc: QueryIrRequest = {
  irVersion: 15,
  from: "logs",
  range: { from: "now-15m", to: "now" },
  result: "rows",
  pipeline: [],
  tail: {},
};

const bodies = (res: QueryIrResponse) =>
  (res.rows ?? []).map((r) => String((r as unknown[])[0]));

function response(rows: string[], cursor: string, caughtUp: boolean) {
  return {
    result: "rows",
    window: { start_ns: 0, end_ns: 1 },
    columns: [{ name: "body", type: "string" }],
    rows: rows.map((r) => [r]),
    tail: {
      cursor,
      settled_through_ns: 0,
      settle_ns: 10_000_000_000,
      caught_up: caughtUp,
    },
    warnings:
      cursor === "c2"
        ? [{ code: "tail_lagged", message: "the tail skipped forward" }]
        : [],
  };
}

/** Stub the transport with a `[status, body]` per call, recording each
 * request's `tail`. */
function stubCalls(reply: (call: number) => [number, unknown]) {
  const tails: unknown[] = [];
  client.setConfig({
    baseUrl: "http://localhost",
    fetch: async (input: RequestInfo | URL) => {
      const body = JSON.parse(await (input as Request).clone().text());
      tails.push(body.tail);
      const [status, payload] = reply(tails.length - 1);
      return new Response(JSON.stringify(payload), {
        status,
        headers: { "Content-Type": "application/json" },
      });
    },
  });
  return tails;
}

const failure = { status: "error", errorType: "x", error: "x" };

describe("useIrTail", () => {
  it("polls with the cursor, at once while behind, newest rows first", async () => {
    const calls = stubApiFetch((call: number) =>
      call === 0
        ? response(["a", "b"], "c1", false)
        : response(call === 1 ? ["c"] : [], "c2", true),
    );
    const { result, unmount } = renderHook(() =>
      useIrTail(doc, bodies, 10, 60_000),
    );

    await waitFor(() => expect(result.current.rows).toEqual(["c", "b", "a"]));
    expect((calls[0]!.body as QueryIrRequest).tail).toEqual({});
    expect((calls[1]!.body as QueryIrRequest).tail).toEqual({ cursor: "c1" });
    expect(result.current.warnings[0]?.code).toBe("tail_lagged");
    // Caught up: the next poll waits for the interval.
    expect(calls).toHaveLength(2);
    unmount();
  });

  it("keeps at most `cap` rows and does nothing without a document", async () => {
    stubApiFetch(response(["a", "b", "c"], "c1", true));
    const { result } = renderHook(() => useIrTail(doc, bodies, 2, 60_000));
    await waitFor(() => expect(result.current.rows).toEqual(["c", "b"]));

    const idle = renderHook(() => useIrTail(null, bodies, 2));
    expect(idle.result.current.started).toBe(false);
  });

  it("restarts without a cursor after a 410 and keeps a lag warning", async () => {
    const tails = stubCalls((call) =>
      call === 0
        ? [200, response(["a"], "c2", false)]
        : call === 1
          ? [410, failure]
          : [200, response(["b"], "c3", call < 3 ? false : true)],
    );
    const { result, unmount } = renderHook(() => useIrTail(doc, bodies, 10, 1));
    await waitFor(() => expect(tails.length).toBeGreaterThanOrEqual(3));
    expect(tails.slice(0, 3)).toEqual([{}, { cursor: "c2" }, {}]);
    await waitFor(() => expect(result.current.rows[0]).toBe("b"));
    expect(result.current.warnings.map((w) => w.code)).toContain("tail_lagged");
    unmount();
  });

  it("treats a response without a tail as an error and keeps the cursor", async () => {
    const { tail: _, ...untailed } = response([], "c9", true);
    const tails = stubCalls((call) =>
      call === 0
        ? [200, response(["a"], "c1", false)]
        : [200, call === 1 ? untailed : response([], "c1", true)],
    );
    const { result, unmount } = renderHook(() => useIrTail(doc, bodies, 10, 1));
    await waitFor(() => expect(tails.length).toBeGreaterThanOrEqual(3));
    expect(tails[2]).toEqual({ cursor: "c1" });
    expect(result.current.rows).toEqual(["a"]);
    unmount();
  });
});
