import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, vi } from "vitest";
import { client as generatedClient } from "../api/gen/client.gen";

export function renderWithClient(ui: ReactElement) {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>{ui}</QueryClientProvider>,
  );
}

const realFetch = globalThis.fetch;

// Registered once per importing test file: restores the generated client's
// config after any test that called `stubFetchRoutes`, mirroring the
// `vi.unstubAllGlobals()` cleanup those files already do for global fetch.
afterEach(() => {
  generatedClient.setConfig({ baseUrl: "/", fetch: realFetch });
});

type JsonRoute = {
  match: string | RegExp;
  body: unknown;
  status?: number;
  /** When set, only match requests with this HTTP method. */
  method?: string;
};

/**
 * Stub URL-matched JSON routes for both raw `fetch` calls (e.g.
 * `promLabelStats`, not yet in the OpenAPI document) and the generated
 * OpenAPI client's transport (everything else) — the client constructs its
 * own `Request(url)` before any fetch mock runs, and `Request` rejects
 * relative URLs under jsdom/Node, so it needs an absolute `baseUrl` too (see
 * api/queryIr.ts's test precedent). Later routes win when multiple match;
 * unmatched URLs 404 so tests fail loudly on unexpected requests.
 *
 * The generated SDK passes a single `Request` object to fetch (not
 * `(url, init)`), so the method is extracted from the `Request` when
 * available. Raw `fetch(url, init)` calls extract the method from the second
 * argument's `method` field (defaulting to GET).
 */
export function stubFetchRoutes(routes: JsonRoute[]) {
  const fn = vi
    .fn()
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input instanceof Request ? input.url : input);
      const method = (
        input instanceof Request ? input.method : (init?.method ?? "GET")
      ).toUpperCase();
      const route = [...routes].reverse().find((r) => {
        const urlMatch =
          typeof r.match === "string"
            ? url.includes(r.match)
            : r.match.test(url);
        if (!urlMatch) return false;
        if (r.method && r.method.toUpperCase() !== method) return false;
        return true;
      });
      if (!route) {
        return Promise.resolve(
          new Response(JSON.stringify({ error: `no stub for ${url}` }), {
            status: 404,
          }),
        );
      }
      return Promise.resolve(
        new Response(JSON.stringify(route.body), {
          status: route.status ?? 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    });
  vi.stubGlobal("fetch", fn);
  generatedClient.setConfig({ baseUrl: "http://localhost", fetch: fn });
  return fn;
}

export const emptyStreams = {
  status: "success",
  data: { resultType: "streams", result: [] },
};

export const emptyMatrix = {
  status: "success",
  data: { resultType: "matrix", result: [] },
};

export const emptyLabels = { status: "success", data: [] };

export function logsResponse(
  rows: {
    tsNs: string;
    line: string;
    labels: Record<string, string>;
  }[],
) {
  return {
    status: "success",
    data: {
      resultType: "streams",
      result: rows.map((r) => ({
        stream: r.labels,
        values: [[r.tsNs, r.line]],
      })),
    },
  };
}
