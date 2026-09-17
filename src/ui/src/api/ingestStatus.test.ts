import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildIngestStatusDoc,
  fetchIngestStatus,
  INGEST_STATUS_SIGNALS,
  INGEST_STATUS_WINDOW_MS,
} from "./ingestStatus";
import { client } from "./gen/client.gen";
import { msToNanos } from "../lib/time";

beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function tableResponse(rows: unknown[][]) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [],
    rows,
  };
}

describe("buildIngestStatusDoc", () => {
  it("counts records over the last 15 minutes for the given signal", () => {
    const doc = buildIngestStatusDoc("traces", 1_000_000_000);
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: {
        from: msToNanos(1_000_000_000 - INGEST_STATUS_WINDOW_MS),
        to: msToNanos(1_000_000_000),
      },
      result: "table",
      pipeline: [{ aggregate: { aggs: [{ fn: "count", as: "n" }] } }],
    });
  });

  it.each(INGEST_STATUS_SIGNALS)("uses %s as the source", (signal) => {
    expect(buildIngestStatusDoc(signal, 0).from).toBe(signal);
  });

  it("defaults to the current time when none is given", () => {
    const before = Date.now();
    const doc = buildIngestStatusDoc("logs");
    const after = Date.now();
    const toMs = Number(BigInt(doc.range.to) / 1_000_000n);
    expect(toMs).toBeGreaterThanOrEqual(before);
    expect(toMs).toBeLessThanOrEqual(after);
  });
});

describe("fetchIngestStatus", () => {
  it("returns the count cell from the table response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse(tableResponse([[42]]))),
    );

    await expect(fetchIngestStatus("metrics", 0)).resolves.toBe(42);
  });

  it("returns 0 when the table has no rows", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse(tableResponse([]))),
    );

    await expect(fetchIngestStatus("profiles", 0)).resolves.toBe(0);
  });

  it("propagates a query failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse({ error: "boom" }, 500)),
    );

    await expect(fetchIngestStatus("traces", 0)).rejects.toThrow();
  });
});
