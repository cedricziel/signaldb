import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { vi } from "vitest";

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

type JsonRoute = { match: string | RegExp; body: unknown; status?: number };

/**
 * Stub global fetch with URL-matched JSON routes. Later routes win when
 * multiple match; unmatched URLs 404 so tests fail loudly on unexpected
 * requests.
 */
export function stubFetchRoutes(routes: JsonRoute[]) {
  const fn = vi.fn().mockImplementation((input: RequestInfo | URL) => {
    const url = String(input);
    const route = [...routes]
      .reverse()
      .find((r) =>
        typeof r.match === "string" ? url.includes(r.match) : r.match.test(url),
      );
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
