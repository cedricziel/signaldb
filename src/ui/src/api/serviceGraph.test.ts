import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchServiceGraph } from "./serviceGraph";
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

const range = { fromMs: 0, toMs: 60_000 };

describe("fetchServiceGraph", () => {
  it("requests the graph envelope over traces at IR version 8", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        result: "graph",
        graph: { nodes: [], edges: [], dropped_nodes: 0 },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchServiceGraph(range);

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body).toMatchObject({
      from: "traces",
      irVersion: 8,
      result: "graph",
      range: { from: "0", to: "60000000000" },
    });
    expect(body.focus).toBeUndefined();
    expect(body.depth).toBeUndefined();
  });

  it("passes focus and depth when scoping to a service's neighbourhood", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        result: "graph",
        graph: { nodes: [], edges: [], dropped_nodes: 0 },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchServiceGraph(range, { focus: "checkout", depth: 2 });

    const req = fetchMock.mock.calls[0]?.[0] as Request;
    const body = await req.clone().json();
    expect(body.focus).toBe("checkout");
    expect(body.depth).toBe(2);
  });

  it("returns the graph from the response envelope", async () => {
    const graph = {
      nodes: [
        {
          id: "service:checkout",
          name: "checkout",
          kind: "service" as const,
          request_rate: 12,
          error_rate: 0.01,
          p95_ns: 1_000_000,
        },
      ],
      edges: [
        {
          source: "service:api",
          target: "service:checkout",
          count: 100,
          rate: 1.6,
          error_rate: 0.02,
          p95_ns: 2_000_000,
        },
      ],
      dropped_nodes: 3,
    };
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse({ result: "graph", graph })),
    );

    const result = await fetchServiceGraph(range);
    expect(result).toEqual({ graph, warnings: [] });
  });

  it("falls back to an empty graph when the envelope has none", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse({ result: "graph" })),
    );

    const result = await fetchServiceGraph(range);
    expect(result).toEqual({ graph: { nodes: [], edges: [] }, warnings: [] });
  });

  it("surfaces non-fatal warnings, e.g. correlate row-limit truncation", async () => {
    const warnings = [
      {
        code: "correlate_row_limit",
        message: "The span join was truncated at its row cap.",
      },
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        jsonResponse({
          result: "graph",
          graph: { nodes: [], edges: [] },
          warnings,
        }),
      ),
    );

    const result = await fetchServiceGraph(range);
    expect(result.warnings).toEqual(warnings);
  });
});
