// The Real users page (`/rum/{tab}`) with a plausible frontend app behind
// it — Web Vitals, sessions over time, top errors and a device breakdown.
// Every `/api/v1/query` answer is computed from the request's own shape and
// `range` (see `rumIr`), so figures line up with whatever window the page —
// or a design-sync capture with a frozen clock — asked for. Fixtures are
// this story's own, not the design prototype's `data.js` (never shown on
// the real page — design.md decision 6).
import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter, Outlet, Route, Routes } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import type { ConnectionInfoResponse } from "../../api/gen";
import { irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { growingPageFrame } from "../../stories/PageFrame";
import { RealUsersRoute } from "./RealUsersRoute";

const MS = 1_000_000;

interface IrDoc {
  from?: string;
  result?: string;
  fields?: string[];
  range?: { from?: string; to?: string };
  pipeline?: Array<{
    correlate?: unknown;
    where?: {
      field?: string;
      value?: string | string[];
      or?: { field?: string }[];
    };
    aggregate?: {
      by?: string[];
      aggs?: Array<{
        fn?: string;
        of?: string;
        as?: string;
        where?: { field?: string; value?: string };
      }>;
      step?: string;
    };
  }>;
}

/** Fixture rows for the Network tab's two reads: every call to the app's
 * own API is traced (`orders-svc`/`cart-svc`/`checkout-svc`); the CDN and
 * the telemetry export endpoint are not — the CDN to exercise the
 * untraced-origin callout, the export endpoint to exercise "SDK export". */
const NETWORK_TOTAL_ROWS: (string | number)[][] = [
  [
    "GET",
    "https://api.storefront.example.com/api/products/48213",
    "api.storefront.example.com",
    42000,
    180_000_000,
    210,
  ],
  [
    "GET",
    "https://api.storefront.example.com/api/cart",
    "api.storefront.example.com",
    28000,
    95_000_000,
    40,
  ],
  [
    "POST",
    "https://api.storefront.example.com/api/checkout",
    "api.storefront.example.com",
    9200,
    420_000_000,
    380,
  ],
  [
    "GET",
    "https://reviews.partner-cdn.com/widget/48213",
    "reviews.partner-cdn.com",
    38100,
    260_000_000,
    0,
  ],
  [
    "POST",
    "https://ingest.acme.example.com/v1/traces",
    "ingest.acme.example.com",
    184210,
    40_000_000,
    0,
  ],
];

const NETWORK_TRACED_ROWS: (string | number)[][] = [
  [
    "GET",
    "https://api.storefront.example.com/api/products/48213",
    "api.storefront.example.com",
    "catalog-svc",
    41200,
    90_000_000,
  ],
  [
    "GET",
    "https://api.storefront.example.com/api/cart",
    "api.storefront.example.com",
    "cart-svc",
    27500,
    55_000_000,
  ],
  [
    "POST",
    "https://api.storefront.example.com/api/checkout",
    "api.storefront.example.com",
    "checkout-svc",
    9100,
    260_000_000,
  ],
];

const RESOURCE_ROWS: (string | number)[][] = [
  ["script", 612000, 48_200_000_000, 210, 812_000],
  ["img", 340000, 122_500_000_000, 340, 2_400_000],
  ["css", 88000, 6_100_000_000, 60, 210_000],
  ["fetch", 210000, 9_800_000_000, 140, 480_000],
];

/** Fixture rows for the Pages tab: `/orders/:id` is the worst-poor-share
 * route (a plausible slow product page), `/checkout` is mostly good; a
 * third, unrouted view exercises the missing-route callout. */
const ORDERS_ROUTE = "/orders/:id";
const CHECKOUT_ROUTE = "/checkout";
const ORDERS_URLS = [
  "https://shop.example.com/orders/48213",
  "https://shop.example.com/orders/91820",
];
const CHECKOUT_URL = "https://shop.example.com/checkout";

const PAGE_VIEWS_ROWS: (string | number | null)[][] = [
  [ORDERS_ROUTE, ORDERS_URLS[0]!, 4200],
  [ORDERS_ROUTE, ORDERS_URLS[1]!, 3800],
  [CHECKOUT_ROUTE, CHECKOUT_URL, 3000],
  [null, "not-a-real-url", 40],
];

const PAGE_VITALS: Record<
  string,
  Record<string, { p75: number; good: number; ni: number; poor: number }>
> = {
  [ORDERS_ROUTE]: {
    lcp: { p75: 4300, good: 4000, ni: 0, poor: 4000 },
    inp: { p75: 420, good: 6000, ni: 0, poor: 2000 },
    cls: { p75: 0.18, good: 7000, ni: 0, poor: 1000 },
    fcp: { p75: 1900, good: 6500, ni: 0, poor: 1500 },
    ttfb: { p75: 900, good: 6000, ni: 0, poor: 2000 },
  },
  [CHECKOUT_ROUTE]: {
    lcp: { p75: 2100, good: 2800, ni: 0, poor: 200 },
    inp: { p75: 150, good: 2900, ni: 0, poor: 100 },
    cls: { p75: 0.05, good: 2950, ni: 0, poor: 50 },
    fcp: { p75: 1200, good: 2850, ni: 0, poor: 150 },
    ttfb: { p75: 500, good: 2900, ni: 0, poor: 100 },
  },
};

const PAGE_ERRORS_ROWS: (string | number)[][] = [[ORDERS_ROUTE, 640]];

/** `[template, full, n, ...10 phase-boundary p75s]` — field order matches
 * `NAV_TIMING_FIELDS` in `api/rum.ts`. */
const LOAD_BREAKDOWN_ROWS: (string | number)[][] = [
  [
    ORDERS_ROUTE,
    ORDERS_URLS[0]!,
    4200,
    10,
    40,
    40,
    90,
    90,
    140,
    300,
    650,
    700,
    900,
  ],
  [
    ORDERS_ROUTE,
    ORDERS_URLS[1]!,
    3800,
    12,
    45,
    45,
    95,
    95,
    145,
    310,
    660,
    710,
    910,
  ],
  [
    CHECKOUT_ROUTE,
    CHECKOUT_URL,
    3000,
    5,
    20,
    20,
    50,
    50,
    80,
    160,
    260,
    300,
    420,
  ],
];

/** Per page route, `[requestUrl, n, p75 ms]` — the request URLs match
 * `NETWORK_TRACED_ROWS`'s own origins/templates so the join finds
 * `catalog-svc`/`checkout-svc`. */
const BACKEND_CALL_ROWS: Record<string, (string | number)[][]> = {
  [ORDERS_ROUTE]: [
    ["https://api.storefront.example.com/api/products/48213", 4200, 180],
    ["https://api.storefront.example.com/api/products/91820", 3800, 185],
  ],
  [CHECKOUT_ROUTE]: [
    ["https://api.storefront.example.com/api/checkout", 3000, 420],
  ],
};

function routeFilterOf(
  pipe: NonNullable<IrDoc["pipeline"]>,
): string | undefined {
  for (const stage of pipe) {
    if (
      stage.where?.field === "url.template" &&
      typeof stage.where.value === "string"
    ) {
      return stage.where.value;
    }
  }
  return undefined;
}

/** `[template, full, css_selector, tag_name, n]` for the Interactions tab. */
const INTERACTION_ROWS: (string | number | null)[][] = [
  [
    ORDERS_ROUTE,
    ORDERS_URLS[0]!,
    "html > body > div.app > div.product > button.buy",
    "button",
    812,
  ],
  [ORDERS_ROUTE, ORDERS_URLS[0]!, null, "a", 340],
  [
    CHECKOUT_ROUTE,
    CHECKOUT_URL,
    "html > body > form > button.submit",
    "button",
    640,
  ],
];

/** `[session.id, first_ts, last_ts, views, errors, slow, entry, exit, user,
 * ua, mobile]` — the Sessions tab's aggregate rows (`buildSessionsListDoc`). */
const SESSIONS_ROWS: (string | number | boolean | null)[][] = [
  [
    "8f14e45f-ceea-467e-adc0-fb62a1a8be22",
    1_700_002_600_000_000_000,
    1_700_003_020_000_000_000,
    5,
    0,
    0,
    CHECKOUT_ROUTE,
    "/thanks",
    "user-1001",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    false,
  ],
  [
    "c1a2b3d4-5e6f-4789-9abc-def012345678",
    1_700_002_000_000_000_000,
    1_700_002_540_000_000_000,
    9,
    2,
    1,
    ORDERS_ROUTE,
    CHECKOUT_ROUTE,
    "user-2044",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    true,
  ],
  [
    "aa11bb22-cc33-4dd4-8ee5-ff6677889900",
    1_700_001_400_000_000_000,
    1_700_001_460_000_000_000,
    1,
    0,
    0,
    null,
    null,
    null,
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    false,
  ],
];

/** Column names for the session-detail `rows` reads — `irColumn`-mapped, the
 * same convention `api/rumSessionDetail.ts`'s `pick()` tries first. */
const SESSION_SPAN_COLUMNS = [
  "trace_id",
  "span_id",
  "parent_span_id",
  "span_name",
  "span_kind",
  "service_name",
  "start_time_unix_nano",
  "duration",
  "status_code",
  "http_request_method",
  "url_full",
  "http_response_status_code",
];

const SESSION_LOG_COLUMNS = [
  "timestamp",
  "event_name",
  "trace_id",
  "url_template",
  "url_full",
  "browser_web_vital_name",
  "browser_web_vital_rating",
  "browser_web_vital_value",
  "browser_css_selector",
  "browser_tag_name",
  "exception_type",
  "exception_message",
  "exception_stacktrace",
  "resource_attributes",
];

/** One session's timeline (the first `SESSIONS_ROWS` entry): a page view, a
 * failed checkout request, and the exception it caused — the spec's own
 * "A failed checkout request" scenario. */
const SESSION_SPAN_ROWS: (string | number | null)[][] = [
  [
    "trace-501",
    "span-501",
    null,
    "POST",
    "Client",
    "storefront-web",
    "1700002610000000000",
    "180000000",
    "Unset",
    "POST",
    "https://api.storefront.example.com/api/checkout",
    502,
  ],
];

const SESSION_LOG_ROWS: (string | number | null)[][] = [
  [
    "1700002600000000000",
    "browser.navigation",
    null,
    "/checkout",
    "https://shop.example.com/checkout",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    JSON.stringify({
      "session.id": "8f14e45f-ceea-467e-adc0-fb62a1a8be22",
      "user.id": "user-1001",
    }),
  ],
  [
    "1700002612800000000",
    "exception",
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    "TypeError",
    "Cannot read properties of undefined (reading 'total')",
    "TypeError: Cannot read properties of undefined\n  at Checkout.render (checkout.js:42)",
    JSON.stringify({
      "session.id": "8f14e45f-ceea-467e-adc0-fb62a1a8be22",
      "user.id": "user-1001",
    }),
  ],
];

function eventNameOf(pipe: NonNullable<IrDoc["pipeline"]>): string | undefined {
  for (const stage of pipe) {
    if (
      stage.where?.field === "event_name" &&
      typeof stage.where.value === "string"
    ) {
      return stage.where.value;
    }
  }
  return undefined;
}

/** A deterministic wobble around `base`, one value per bucket. */
function wave(seed: number, base: number, amp: number, n: number): number[] {
  return Array.from({ length: n }, (_, i) =>
    Math.max(
      0,
      Math.round(
        base +
          amp * Math.sin(i / 4 + seed) +
          amp * 0.4 * Math.cos(i * 1.7 + seed),
      ),
    ),
  );
}

function seriesPoints(
  fromMs: number,
  toMs: number,
  vals: number[],
): [number, number][] {
  const n = vals.length;
  const step = n > 1 ? (toMs - fromMs) / n : 0;
  return vals.map((v, i): [number, number] => [(fromMs + i * step) * MS, v]);
}

const VITALS: Record<
  string,
  { p75: number; good: number; ni: number; poor: number }
> = {
  lcp: { p75: 2140, good: 128200, ni: 41400, poor: 14608 },
  inp: { p75: 178, good: 156100, ni: 22300, poor: 5808 },
  cls: { p75: 0.08, good: 149800, ni: 28900, poor: 5508 },
  fcp: { p75: 1320, good: 140200, ni: 33100, poor: 10908 },
  ttfb: { p75: 640, good: 132400, ni: 38900, poor: 12908 },
};

/** Answers every Real users query from the request alone — a single
 * document, or (the KPI strip and sessions-over-time, bundled per
 * `api/rum.ts`'s module doc) a multi-query/formula document, answered as
 * one output series per named sub-query. */
function rumIr(raw: unknown): unknown {
  const b = (raw ?? {}) as { queries?: Record<string, IrDoc> };
  if (b.queries) {
    return {
      result: "series",
      series: Object.entries(b.queries).map(([key, sub]) => ({
        labels: { formula: key },
        points: seriesPointsForDoc(sub),
      })),
    };
  }
  return singleDocResponse((raw ?? {}) as IrDoc);
}

function singleDocResponse(b: IrDoc): unknown {
  // Session-detail reads (`api/rumSessionDetail.ts`'s `buildSessionSpansDoc`/
  // `buildSessionLogsDoc`) are `rows` queries with no `aggregate` stage —
  // handled before the aggregate-keyed branches below, which all assume one.
  if (b.result === "rows") {
    if (b.from === "traces") {
      return {
        result: "rows",
        columns: SESSION_SPAN_COLUMNS.map((name) => ({ name })),
        rows: SESSION_SPAN_ROWS,
      };
    }
    if (b.from === "logs") {
      return {
        result: "rows",
        columns: SESSION_LOG_COLUMNS.map((name) => ({ name })),
        rows: SESSION_LOG_ROWS,
      };
    }
  }

  const pipe = b.pipeline ?? [];
  const agg = pipe.find((s) => s.aggregate)?.aggregate;
  const by = agg?.by ?? [];
  const toMs = Number(b.range?.to ?? 0) / MS || 1_700_003_600_000;
  const fromMs = Number(b.range?.from ?? 0) / MS || toMs - 3_600_000;

  // App discovery (rumEventWhere + aggregate by service.name).
  if (by[0] === "service.name") {
    return {
      result: "table",
      rows: [
        ["storefront-web", 184210, "webjs", "production", "2026.09.26-3"],
        ["admin-web", 512, "webjs", "production", "2026.09.20-1"],
      ],
    };
  }

  // Web Vitals: (name, rating) or name-only p75.
  if (by[0] === "browser.web_vital.name") {
    if (by.length === 2) {
      return {
        result: "table",
        rows: Object.entries(VITALS).flatMap(([name, v]) => [
          [name, "good", v.good, v.p75],
          [name, "needs-improvement", v.ni, null],
          [name, "poor", v.poor, null],
        ]),
      };
    }
    return {
      result: "table",
      rows: Object.entries(VITALS).map(([name, v]) => [name, v.p75]),
    };
  }

  // Browser breakdown — empty, matching the real deployment's missing
  // browser.brands attribute (design.md — Context) until the next SDK
  // update ships it.
  if (by[0] === "resource.browser.brands") {
    return { result: "table", rows: [] };
  }
  if (by[0] === "resource.browser.mobile") {
    return {
      result: "table",
      rows: [
        [false, 151200],
        [true, 33010],
      ],
    };
  }

  if (
    by.length === 1 &&
    by[0] === "url.full" &&
    eventNameOf(pipe) === "browser.resource_timing"
  ) {
    return {
      result: "table",
      rows: BACKEND_CALL_ROWS[routeFilterOf(pipe) ?? ""] ?? [],
    };
  }

  // Pages: views, per-route vital ratings/p75 and errors — all grouped by
  // (url.template, url.full), disambiguated by `by.length` alone (no
  // event_name collision yet at these lengths).
  if (by[0] === "url.template") {
    const eventName = eventNameOf(pipe);
    if (by.length === 1) {
      return { result: "table", rows: PAGE_ERRORS_ROWS };
    }
    if (by.length === 2 && eventName === "browser.navigation") {
      return { result: "table", rows: PAGE_VIEWS_ROWS };
    }
    if (by.length === 2 && eventName === "browser.navigation_timing") {
      return { result: "table", rows: LOAD_BREAKDOWN_ROWS };
    }

    if (by.length === 3) {
      return {
        result: "table",
        rows: Object.entries(PAGE_VITALS).flatMap(([route, vitals]) =>
          Object.entries(vitals).map(([name, v]) => [
            route,
            ORDERS_URLS[0]!,
            name,
            v.p75,
          ]),
        ),
      };
    }
    if (by.length === 4 && eventName === "browser.web_vital") {
      return {
        result: "table",
        rows: Object.entries(PAGE_VITALS).flatMap(([route, vitals]) =>
          Object.entries(vitals).flatMap(([name, v]) => [
            [route, ORDERS_URLS[0]!, name, "good", v.good],
            [route, ORDERS_URLS[0]!, name, "needs-improvement", v.ni],
            [route, ORDERS_URLS[0]!, name, "poor", v.poor],
          ]),
        ),
      };
    }
    if (by.length === 4 && eventName === "browser.user_action.click") {
      return { result: "table", rows: INTERACTION_ROWS };
    }
  }

  // Sessions list (buildSessionsListDoc).
  if (by[0] === "session.id") {
    return { result: "table", rows: SESSIONS_ROWS };
  }

  // Network: totals (buildNetworkRequestsDoc), the correlate join
  // (buildNetworkCorrelateDoc, `parent.`-prefixed grouping), and resources
  // by initiator type (buildResourcesDoc).
  if (by[0] === "http.request.method") {
    return { result: "table", rows: NETWORK_TOTAL_ROWS };
  }
  if (by[0] === "parent.http.request.method") {
    return { result: "table", rows: NETWORK_TRACED_ROWS };
  }
  if (by[0] === "browser.resource_timing.initiator_type") {
    return { result: "table", rows: RESOURCE_ROWS };
  }

  // Error groups (api/errors.ts's shared shape).
  if (by[0] === "exception.type") {
    const ago = (s: number) => (toMs - s * 1000) * MS;
    if (b.from === "traces") {
      return {
        result: "table",
        rows: [
          [
            "TypeError",
            "Cannot read properties of undefined (reading 'total')",
            "storefront-web",
            "true",
            412,
            fromMs * MS,
            ago(120),
          ],
        ],
      };
    }
    return { result: "table", rows: [] };
  }

  return { result: "table", rows: [] };
}

/** The bucketed points for one KPI/sessions-over-time sub-query (standalone
 * or nested in a multi-query document) — a deterministic wave keyed by
 * which metric its aggregate's `of`/`where` mark it as. */
function seriesPointsForDoc(b: IrDoc): [number, number][] {
  const pipe = b.pipeline ?? [];
  const agg = pipe.find((s) => s.aggregate)?.aggregate;
  const firstAgg = agg?.aggs?.[0];
  const toMs = Number(b.range?.to ?? 0) / MS || 1_700_003_600_000;
  const fromMs = Number(b.range?.from ?? 0) / MS || toMs - 3_600_000;
  if (!firstAgg) return [];
  const step = Number(agg?.step?.replace("s", "")) || 60;
  const span = toMs - fromMs;
  const n = Math.max(1, Math.round(span / 1000 / step));
  const isUsers = firstAgg.of === "user.id";
  const isErrors = firstAgg.where?.value === "exception";
  const isViews = firstAgg.where?.value === "browser.navigation";
  // buildTracedShareDoc's two named queries — traces-sourced, unlike every
  // other multi-query doc here — with the traced count ~83% of the total,
  // so the KPI's derived share reads as a plausible "mostly traced" figure.
  const isTracedCount = firstAgg.of === "parent.span_id";
  const isClientTotal = b.from === "traces" && !isTracedCount;
  const base = isTracedCount
    ? 290
    : isClientTotal
      ? 350
      : isUsers
        ? 900
        : isErrors
          ? 60
          : isViews
            ? 2100
            : 3200;
  const amp = isTracedCount || isClientTotal ? 12 : isErrors ? 20 : base * 0.25;
  return seriesPoints(fromMs, toMs, wave(2, base, amp, n));
}

const CONNECTION: ConnectionInfoResponse = {
  tenant_id: "acme",
  dataset_id: "production",
  headers: {
    authorization: "Bearer <api-key>",
    "x-tenant-id": "acme",
    "x-dataset-id": "production",
  },
  ingest: {
    otlp_grpc: {
      authority: "ingest.acme.example.com:4317",
      url: "https://ingest.acme.example.com:4317",
      protocol: "grpc",
      signals: ["traces", "logs", "metrics"],
      tls: true,
    },
    otlp_http: {
      url: "https://ingest.acme.example.com:4318",
      protocol: "http",
      tls: true,
      paths: {
        logs: "/v1/logs",
        metrics: "/v1/metrics",
        profiles: "/v1/profiles",
        traces: "/v1/traces",
      },
    },
    // Deliberately not a "prometheus" path segment — compatGuard.test.ts
    // flags that shape as a Loki/Tempo/Prometheus/Pyroscope *compat* path;
    // this is SignalDB's own native remote-write ingest URL, unrelated.
    prometheus_remote_write:
      "https://ingest.acme.example.com/api/v1/remote-write",
  },
  notes: [],
  otel_env: {
    OTEL_EXPORTER_OTLP_ENDPOINT: "https://ingest.acme.example.com:4317",
    OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
    OTEL_EXPORTER_OTLP_HEADERS:
      "authorization=Bearer <api-key>,x-tenant-id=acme,x-dataset-id=production",
  },
  public_endpoints_configured: true,
  query: {
    api_url: "https://acme.example.com",
    compat: { tempo: "", loki: "", prometheus: "", pyroscope: "" },
    openapi: "https://acme.example.com/openapi.json",
    query_ir: "https://acme.example.com/api/v1/query",
  },
  required_scopes: { ingest: ["write"], query: ["read"] },
};

const routes: JsonRoute[] = [
  irCatchAll,
  { match: "/api/v1/query", body: {}, bodyFor: rumIr },
  { match: "/api/v1/connection", body: CONNECTION },
];

/** No frontend app has sent RUM data in this window. */
const emptyRoutes: JsonRoute[] = [
  irCatchAll,
  {
    match: "/api/v1/query",
    body: { result: "table", rows: [], columns: [], series: [] },
  },
  { match: "/api/v1/connection", body: CONNECTION },
];

const STATE: ExploreState = {
  ...DEFAULT_STATE,
  tenant: "acme",
  dataset: "production",
};

function RealUsersPage({
  path = "/rum/overview",
  stubRoutes = routes,
}: {
  path?: string;
  stubRoutes?: JsonRoute[];
}) {
  return (
    <StoryFetchStub routes={stubRoutes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={[path]}>
          <Routes>
            <Route
              element={<Outlet context={{ state: STATE, update: () => {} }} />}
            >
              <Route path="/rum/:tab" element={<RealUsersRoute />} />
            </Route>
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Real Users",
  parameters: { layout: "fullscreen" },
  decorators: [growingPageFrame],
} satisfies Meta<typeof RealUsersPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <RealUsersPage />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <RealUsersPage />
    </DarkScope>
  ),
};

export const Empty: Story = {
  render: () => <RealUsersPage stubRoutes={emptyRoutes} />,
};

export const Setup: Story = {
  render: () => <RealUsersPage path="/rum/setup?app=storefront-web" />,
};

export const Network: Story = {
  render: () => <RealUsersPage path="/rum/network?app=storefront-web" />,
};

export const Pages: Story = {
  render: () => <RealUsersPage path="/rum/pages?app=storefront-web" />,
};

export const PagesRouteDetail: Story = {
  render: () => (
    <RealUsersPage
      path={`/rum/pages?app=storefront-web&route=${encodeURIComponent(ORDERS_ROUTE)}`}
    />
  ),
};

export const Sessions: Story = {
  render: () => <RealUsersPage path="/rum/sessions?app=storefront-web" />,
};

export const SessionDetail: Story = {
  render: () => (
    <RealUsersPage path="/rum/sessions?app=storefront-web&session=8f14e45f-ceea-467e-adc0-fb62a1a8be22" />
  ),
};

export const Interactions: Story = {
  render: () => <RealUsersPage path="/rum/interactions?app=storefront-web" />,
};
