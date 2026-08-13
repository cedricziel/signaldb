import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchSpanKinds } from "./spanKinds";
import { client } from "./gen/client.gen";

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

describe("fetchSpanKinds", () => {
  it("filters by trace id and maps span_id to span_kind", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        result: "rows",
        window: { start_ns: 0, end_ns: 0 },
        columns: [
          { name: "span_id", type: "string" },
          { name: "span_kind", type: "string" },
        ],
        rows: [
          ["s1", "SERVER"],
          ["s2", "CLIENT"],
        ],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const kinds = await fetchSpanKinds("t1", 0, 60_000);
    expect(kinds).toEqual({ s1: "SERVER", s2: "CLIENT" });

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body.from).toBe("traces");
    expect(body.result).toBe("rows");
    expect(body.fields).toEqual(["span_id", "span_kind"]);
    expect(body.pipeline).toEqual([
      { where: { field: "trace_id", op: "eq", value: "t1" } },
    ]);
  });

  it("skips malformed rows rather than throwing", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        result: "rows",
        window: { start_ns: 0, end_ns: 0 },
        columns: [],
        rows: [
          ["s1", null],
          [null, "SERVER"],
          ["s2", "CLIENT"],
        ],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const kinds = await fetchSpanKinds("t1", 0, 60_000);
    expect(kinds).toEqual({ s2: "CLIENT" });
  });

  it("returns an empty map when no rows come back", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        result: "rows",
        window: { start_ns: 0, end_ns: 0 },
        columns: [],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const kinds = await fetchSpanKinds("t1", 0, 60_000);
    expect(kinds).toEqual({});
  });
});
