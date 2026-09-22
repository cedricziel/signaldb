// Storybook's browser-safe counterpart to `src/test/render.tsx`'s
// `stubFetchRoutes` — same route-matching behaviour, but no vitest import
// (that file is test-only and would break the Storybook build). Install
// from a story/meta decorator, and always call the returned `restore()` on
// cleanup so one story's stub can't leak `fetch` into the next.
import { client as generatedClient } from "../api/gen/client.gen";

export type JsonRoute = {
  match: string | RegExp;
  body: unknown;
  status?: number;
  /** When set, only match requests with this HTTP method. */
  method?: string;
  /** When set, only match requests whose parsed JSON body satisfies it. */
  bodyMatch?: (body: unknown) => boolean;
};

/** Mirrors `test/render.tsx`'s `stubFetchRoutes`: stubs both raw `fetch`
 * calls and the generated OpenAPI client's transport. Returns a `restore()`
 * that puts the real `fetch` and client config back. */
export function installFetchStub(routes: JsonRoute[]): { restore: () => void } {
  const realFetch = globalThis.fetch;
  const fn = async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
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
        typeof r.match === "string" ? url.includes(r.match) : r.match.test(url);
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
  };

  globalThis.fetch = fn as typeof fetch;
  generatedClient.setConfig({ baseUrl: "http://localhost", fetch: fn });

  return {
    restore: () => {
      globalThis.fetch = realFetch;
      generatedClient.setConfig({ baseUrl: "/", fetch: realFetch });
    },
  };
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
  rows: { tsNs: string; line: string; labels: Record<string, string> }[],
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
