import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement, ReactNode } from "react";
import {
  createMemoryRouter,
  Outlet,
  RouterProvider,
  type RouteObject,
} from "react-router";
import { afterEach, vi } from "vitest";
import { client as generatedClient } from "../api/gen/client.gen";
import type { ExploreState } from "../lib/urlState";

function testQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
    },
  });
}

export function renderWithClient(ui: ReactElement) {
  const client = testQueryClient();
  return render(
    <QueryClientProvider client={client}>{ui}</QueryClientProvider>,
  );
}

/** The same provider as {@link renderWithClient}, as a `renderHook` wrapper —
 * for testing a hook that issues queries without mounting a component. */
export function clientWrapper() {
  const client = testQueryClient();
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

/**
 * Renders a route tree through a real data router (`createMemoryRouter` +
 * `RouterProvider`) — needed wherever a mounted component (directly, or via
 * `UnsavedChangesGuard`) calls `useBlocker`, which throws under the plain
 * `<MemoryRouter>` most tests use. Returns the router alongside the render
 * result so a test can also assert on `router.state.location`.
 */
export function renderWithRouter(
  routes: RouteObject[],
  initialEntries: string[] = ["/"],
) {
  const router = createMemoryRouter(routes, { initialEntries });
  return { router, ...renderWithClient(<RouterProvider router={router} />) };
}

/** The shell's outlet context (`state`/`update`, see `App.tsx`) as the
 * element a `<Route element={...}>` renders — every route/page test needs
 * one ancestor route providing this, since `useOutletState` (and anything
 * built on it, like `useSchemaSession`) throws without it. `update`
 * defaults to a no-op mock. */
export function outletContextRoute(
  state: ExploreState,
  update: (patch: Partial<ExploreState>) => void = vi.fn(),
) {
  return <Outlet context={{ state, update }} />;
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
  /** When set, only match requests whose parsed JSON body satisfies it —
   * for endpoints like the Query IR where the URL alone is ambiguous. */
  bodyMatch?: (body: unknown) => boolean;
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
    .mockImplementation(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input instanceof Request ? input.url : input);
        const method = (
          input instanceof Request ? input.method : (init?.method ?? "GET")
        ).toUpperCase();
        let parsedBody: unknown;
        let bodyParsed = false;
        const requestBody = async (): Promise<unknown> => {
          if (bodyParsed) return parsedBody;
          bodyParsed = true;
          try {
            parsedBody =
              input instanceof Request
                ? await input.clone().json()
                : typeof init?.body === "string"
                  ? JSON.parse(init.body)
                  : undefined;
          } catch {
            parsedBody = undefined;
          }
          return parsedBody;
        };
        let route: JsonRoute | undefined;
        for (const r of [...routes].reverse()) {
          const urlMatch =
            typeof r.match === "string"
              ? url.includes(r.match)
              : r.match.test(url);
          if (!urlMatch) continue;
          if (r.method && r.method.toUpperCase() !== method) continue;
          if (r.bodyMatch && !r.bodyMatch(await requestBody())) continue;
          route = r;
          break;
        }
        if (!route) {
          return new Response(JSON.stringify({ error: `no stub for ${url}` }), {
            status: 404,
          });
        }
        return new Response(JSON.stringify(route.body), {
          status: route.status ?? 200,
          headers: { "Content-Type": "application/json" },
        });
      },
    );
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

/** Query IR `logs` rows response, in the column order `api/ir/logs.ts`
 * projects (see `ROW_FIELDS`). */
export function irLogRowsResponse(
  rows: {
    tsNs: string;
    body: string;
    serviceName?: string;
    severityText?: string;
    traceId?: string | null;
    spanId?: string | null;
    scopeName?: string;
    logAttributes?: Record<string, string>;
    scopeAttributes?: Record<string, string>;
    resourceAttributes?: Record<string, string>;
  }[],
) {
  return {
    result: "rows",
    window: { start_ns: 0, end_ns: 0 },
    columns: [
      { name: "timestamp", type: "timestamp_ns" },
      { name: "body", type: "string" },
      { name: "service_name", type: "string" },
      { name: "severity_text", type: "string" },
      { name: "trace_id", type: "string" },
      { name: "span_id", type: "string" },
      { name: "scope_name", type: "string" },
      { name: "log_attributes", type: "map<string,string>" },
      { name: "scope_attributes", type: "map<string,string>" },
      { name: "resource_attributes", type: "map<string,string>" },
    ],
    rows: rows.map((r) => [
      r.tsNs,
      r.body,
      r.serviceName ?? "",
      r.severityText ?? "",
      r.traceId ?? null,
      r.spanId ?? null,
      r.scopeName ?? "",
      r.logAttributes ?? {},
      r.scopeAttributes ?? {},
      r.resourceAttributes ?? {},
    ]),
  };
}

/** An empty Query IR `series` response (the log volume histogram). */
export const emptyIrSeries = {
  result: "series",
  window: { start_ns: 0, end_ns: 0 },
  series: [],
};

/**
 * A response answering either the logs tab's rows query or its volume
 * series query with nothing — a single stub covers both, since each reader
 * only looks at its own field (`rows`/`columns` vs `series`).
 */
export const emptyIrLogs = {
  result: "rows",
  window: { start_ns: 0, end_ns: 0 },
  columns: [],
  rows: [],
  series: [],
};
