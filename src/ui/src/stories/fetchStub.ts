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

/** Query IR `logs` rows response, in the column order `api/ir/logs.ts`
 * projects (see `ROW_FIELDS`) — mirrors `test/render.tsx`'s
 * `irLogRowsResponse`. */
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

/** A Query IR `series` response for the log-volume histogram, stacked by
 * `severity_text`. */
export function irLogVolumeResponse(
  buckets: { level: string; points: [number, number][] }[],
) {
  return {
    result: "series",
    window: { start_ns: 0, end_ns: 0 },
    series: buckets.map((b) => ({
      labels: { severity_text: b.level },
      points: b.points,
    })),
  };
}

/** An empty Query IR `series` response (the log volume histogram). */
export const emptyIrSeries = {
  result: "series",
  window: { start_ns: 0, end_ns: 0 },
  series: [],
};

/** A `describe: fields` response naming `names` as declared, filterable
 * fields — for stubbing a field-picker's discovery request. */
export function describeFieldsResponse(names: string[]) {
  return {
    result: "metadata",
    window: { start_ns: 0, end_ns: 0 },
    metadata: {
      kind: "fields",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate: false,
      },
      fields: names.map((name) => ({
        name,
        type: "string",
        filterable: true,
        origin: "declared",
      })),
    },
  };
}
