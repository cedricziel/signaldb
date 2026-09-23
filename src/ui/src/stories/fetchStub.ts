// Storybook's browser-safe counterpart to `src/test/render.tsx`'s
// `stubFetchRoutes` — same route-matching behaviour, but no vitest import
// (that file is test-only and would break the Storybook build). Install
// from a story/meta decorator, and always call the returned `restore()` on
// cleanup so one story's stub can't leak `fetch` into the next.
import { client as generatedClient } from "../api/gen/client.gen";
import type { WhoamiResponse } from "../api/session";

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

/** A Query IR `table` response for `api/entityDetailStats.ts`'s
 * `buildEntityStatsDoc` — one row of `[...identity, n, errors, p50, p95,
 * p99, last]`, the shape its decoder reads positionally. */
export function irEntityStatsResponse(row: {
  identity?: (string | null)[];
  n: number;
  errors: number;
  p50Ns: number;
  p95Ns: number;
  p99Ns: number;
  lastNs: string;
}) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    rows: [
      [
        ...(row.identity ?? []),
        row.n,
        row.errors,
        row.p50Ns,
        row.p95Ns,
        row.p99Ns,
        row.lastNs,
      ],
    ],
  };
}

/** A Query IR `series` response for a single stepped count/quantile series
 * (`buildEntityCountSeriesDoc`/`buildEntityP95SeriesDoc` in
 * `api/entityDetailStats.ts`), or the empty table's no-data case. */
export function irEntitySeriesResponse(
  points: [number, number][],
  labels: Record<string, string> = {},
) {
  return {
    result: "series",
    window: { start_ns: 0, end_ns: 0 },
    series: points.length === 0 ? [] : [{ labels, points }],
  };
}

/** A Query IR `series` response for `api/operationSeries.ts`'s grouped
 * per-operation query — one `ResultSeries` per operation, keyed by the
 * breakdown field's Loki-style label. */
export function irOperationSeriesResponse(
  labelKey: string,
  byOperation: Record<string, [number, number][]>,
) {
  return {
    result: "series",
    window: { start_ns: 0, end_ns: 0 },
    series: Object.entries(byOperation).map(([operation, points]) => ({
      labels: { [labelKey]: operation },
      points,
    })),
  };
}

/** Universal fallback for any `/api/v1/query` call a story doesn't
 * specifically care about — every consumer reads the envelope through
 * optional chaining, so `{}` is a safe "nothing here" for any result kind,
 * whatever picker or metadata fetch fired it. Placed first in a story's
 * `routes` array so a more specific route later in the array overrides it
 * (`installFetchStub` matches last-route-wins). */
export const irCatchAll: JsonRoute = { match: "/api/v1/query", body: {} };

/** Narrows a Query IR request body to its `result`/`from` fields for
 * routing a story's several distinct `/api/v1/query` calls to different
 * stubbed responses — the discriminator every IR-backed view's stories use
 * (see `TracesView.stories.tsx`, `ErrorsView.stories.tsx`,
 * `CatalogView.stories.tsx`). */
export function irBody(
  match: (body: { result?: string; from?: string }) => boolean,
) {
  return (b: unknown) => match((b ?? {}) as { result?: string; from?: string });
}

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

/** A `GET /api/v1/whoami` response for "alice@example.com" in "acme" —
 * shared by every story that stubs the top bar's tenant/user context
 * (`TopBar`, `Pages/App Shell`). */
export function sampleWhoami(
  overrides: Partial<WhoamiResponse> = {},
): WhoamiResponse {
  return {
    user: {
      id: "user-1",
      email: "alice@example.com",
      display_name: "Alice",
      is_instance_admin: false,
    },
    memberships: [{ tenant_id: "acme", role: "admin" }],
    tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
    datasets: [
      { id: "production", slug: "production", is_default: true },
      { id: "staging", slug: "staging", is_default: false },
    ],
    default_dataset: "production",
    ...overrides,
  };
}

/** The matching `GET /ui/session` response for {@link sampleWhoami} — same
 * user/tenant/memberships, the shape `currentSession()` returns. */
export function sampleCurrentSession(
  who: WhoamiResponse,
  extra: Record<string, unknown> = {},
) {
  return {
    user: who.user,
    tenant: who.tenant.id,
    dataset: who.default_dataset,
    memberships: who.memberships,
    ...extra,
  };
}
