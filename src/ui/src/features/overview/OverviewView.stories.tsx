// The System Overview (`/overview`) with a small, plausible system behind
// it: ten services, two external dependencies, two deploys in the window.
// Every `/api/v1/query` answer is computed from the request's own shape and
// `range` (see `overviewIr`), so series line up with whatever window the
// page — or a design-sync capture with a frozen clock — asked for.
import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  irCatchAll,
  sampleCurrentSession,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { OverviewView } from "./OverviewView";

const WHO = sampleWhoami();
const MS = 1_000_000;
const BUCKETS = 30;

// name, requests/s, error fraction, p95 ms
const SERVICES: [string, number, number, number][] = [
  ["api-gateway", 2860, 0.004, 88],
  ["search", 1210, 0.001, 120],
  ["recommendations", 820, 0.004, 240],
  ["cart", 640, 0.002, 42],
  ["inventory", 530, 0.003, 190],
  ["checkout", 412, 0.021, 318],
  ["orders", 390, 0.001, 74],
  ["payments", 180, 0.009, 460],
  ["shipping-quote", 150, 0.026, 610],
  ["notifications", 95, 0, 35],
];
const PROFILED = new Set(["api-gateway", "checkout", "cart", "search"]);
const EDGES: [string, string, number][] = [
  ["api-gateway", "search", 1210],
  ["api-gateway", "recommendations", 820],
  ["api-gateway", "cart", 640],
  ["api-gateway", "checkout", 412],
  ["api-gateway", "orders", 210],
  ["recommendations", "search", 300],
  ["checkout", "cart", 412],
  ["checkout", "inventory", 412],
  ["checkout", "payments", 180],
  ["checkout", "shipping-quote", 150],
  ["orders", "notifications", 95],
  ["payments", "stripe-api", 180],
  ["orders", "postgres", 390],
];

interface IrDoc {
  from?: string;
  result?: string;
  range?: { from?: string; to?: string };
  pipeline?: Array<{
    where?: { field?: string };
    describe?: unknown;
    aggregate?: {
      by?: string[];
      aggs?: Array<{ fn?: string; where?: unknown }>;
    };
  }>;
}

/** A deterministic wobble around `base`, one value per bucket. */
function wave(seed: number, base: number, amp: number): number[] {
  return Array.from({ length: BUCKETS }, (_, i) =>
    Math.max(
      0,
      base +
        amp * Math.sin(i / 4 + seed) +
        amp * 0.4 * Math.cos(i * 1.7 + seed),
    ),
  );
}

function points(fromMs: number, toMs: number, vals: number[]) {
  const step = (toMs - fromMs) / BUCKETS;
  return vals.map((v, i): [number, number] => [(fromMs + i * step) * MS, v]);
}

const series = (labels: Record<string, string>, pts: [number, number][]) => ({
  result: "series",
  series: [{ labels, points: pts }],
});

/** Answers every Overview query from the request alone. */
function overviewIr(raw: unknown): unknown {
  const b = (raw ?? {}) as IrDoc;
  const pipe = b.pipeline ?? [];
  const agg = pipe.find((s) => s.aggregate)?.aggregate ?? {};
  const by = agg.by ?? [];
  const toMs = Number(b.range?.to ?? 0) / MS || 1_700_003_600_000;
  const fromMs = Number(b.range?.from ?? 0) / MS || toMs - 3_600_000;
  const span = toMs - fromMs;

  if (b.result === "graph") {
    const errOf = new Map(SERVICES.map(([n, , e]) => [n, e]));
    return {
      result: "graph",
      graph: {
        nodes: [
          ...SERVICES.map(([n, r, e, p]) => ({
            id: n,
            name: n,
            kind: "service",
            request_rate: r,
            error_rate: e,
            p95_ns: p * MS,
          })),
          {
            id: "stripe-api",
            name: "stripe-api",
            kind: "external",
            error_rate: 0.006,
            p95_ns: 380 * MS,
          },
          {
            id: "postgres",
            name: "postgres",
            kind: "external",
            error_rate: 0,
            p95_ns: 12 * MS,
          },
        ],
        edges: EDGES.map(([s, t, c]) => ({
          source: s,
          target: t,
          count: c * 60,
          rate: c,
          error_rate: errOf.get(t) ?? 0.006,
        })),
      },
    };
  }
  if (pipe[0]?.describe) {
    return {
      result: "metadata",
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "exact",
          window_scoped: false,
          sampled: false,
          approximate: false,
        },
        values: [{ value: "production" }, { value: "staging" }],
      },
    };
  }

  if (b.result === "series") {
    const first = agg.aggs?.[0] ?? {};
    const base =
      {
        logs: 1600,
        traces: 760,
        metrics: 250,
        metrics_histogram: 60,
        profiles: 95,
      }[b.from ?? ""] ?? 40;
    if (by.includes("service.name")) {
      return {
        result: "series",
        series: SERVICES.map(([n, r], i) => ({
          labels: { service_name: n },
          points: points(fromMs, toMs, wave(i, r * 120, r * 20)),
        })),
      };
    }
    if (by.includes("exception.type")) {
      return series({}, points(fromMs, toMs, wave(3, 10, 6)));
    }
    if (first.fn === "quantile") {
      return series(
        {},
        points(
          fromMs,
          toMs,
          wave(2, 200, 30).map((v) => v * MS),
        ),
      );
    }
    if (first.where) {
      return series({}, points(fromMs, toMs, wave(5, 30, 15)));
    }
    if (pipe.some((s) => s.where?.field === "parent_span_id")) {
      return series({}, points(fromMs, toMs, wave(1, 2860 * 120, 300 * 120)));
    }
    return series(
      {},
      points(fromMs, toMs, wave(base, base * 1000, base * 200)),
    );
  }

  // result: "table"
  if (by.includes("service.version")) {
    const at = (f: number) => (fromMs + f * span) * MS;
    return {
      result: "table",
      rows: [
        ["cart", "v1.17.2", at(0), at(0.25)],
        ["cart", "v1.18.0", at(0.27), at(1)],
        ["checkout", "v2.40.1", at(0), at(0.72)],
        ["checkout", "v2.41.0", at(0.73), at(1)],
        ["search", "v3.7.2", at(0), at(1)],
      ],
    };
  }
  if (by[0] === "span.name") {
    return {
      result: "table",
      rows: [
        ["POST /v1/quote", "shipping-quote", 900, 610 * MS, 1940 * MS],
        ["POST /v1/charge", "payments", 1200, 460 * MS, 1210 * MS],
        ["POST /api/checkout", "checkout", 4000, 318 * MS, 2400 * MS],
        [
          "GET /recommendations/{user}",
          "recommendations",
          8000,
          240 * MS,
          780 * MS,
        ],
        ["InventoryService/Reserve", "inventory", 5000, 190 * MS, 640 * MS],
        ["GET /search", "search", 9000, 120 * MS, 410 * MS],
      ],
    };
  }
  if (by[0] === "exception.type") {
    const ago = (s: number) => (toMs - s * 1000) * MS;
    return {
      result: "table",
      rows:
        b.from === "traces"
          ? [
              [
                "PaymentDeclinedError",
                "card_declined: issuer returned do_not_honor",
                "payments",
                "false",
                12044,
                fromMs * MS,
                ago(2),
              ],
              [
                "DeadlineExceeded",
                "context deadline exceeded calling inventory.Reserve after 2000ms",
                "checkout",
                "true",
                8391,
                fromMs * MS,
                ago(23),
              ],
              [
                "UpstreamUnavailable",
                "503 Service Unavailable from carrier-api /v1/quote",
                "shipping-quote",
                "true",
                3918,
                fromMs * MS,
                ago(108),
              ],
            ]
          : [
              [
                "RedisPoolExhausted",
                "redis: connection pool exhausted (size=64, wait=500ms)",
                "cart",
                null,
                4102,
                fromMs * MS,
                ago(136),
              ],
              [
                "NullPointerException",
                'Cannot invoke "Address.getCountry()" because "shipping" is null',
                "checkout",
                null,
                1877,
                fromMs * MS,
                ago(290),
              ],
            ],
    };
  }
  if (by.length === 0) {
    // The KPI strip's root-span stats: n, errors, p50, p95, p99, last.
    const n = (2860 * span) / 1000;
    return {
      result: "table",
      rows: [[n, n * 0.0074, 40 * MS, 214 * MS, 890 * MS, toMs * MS]],
    };
  }
  if (by[0] === "service.name") {
    const traces = b.from === "traces";
    return {
      result: "table",
      rows: SERVICES.filter(
        ([n]) => b.from !== "profiles" || PROFILED.has(n),
      ).map(([n, r, e, p]) => {
        const ids = by.map((_, i) => (i === 0 ? n : null));
        const count = (r * span) / 1000;
        return traces
          ? [...ids, count, count * e, p * 0.4 * MS, p * MS, toMs * MS]
          : [...ids, count * 2, toMs * MS];
      }),
    };
  }
  return { result: "table", rows: [] };
}

const routes: JsonRoute[] = [
  irCatchAll,
  { match: "/api/v1/query", body: {}, bodyFor: overviewIr },
  { match: "/api/v1/whoami", body: WHO },
  { match: "/ui/session", method: "GET", body: sampleCurrentSession(WHO) },
  {
    match: "/memberships",
    body: [
      {
        user_id: "user-1",
        email: WHO.user!.email,
        role: "admin",
        granted_by: "local",
      },
    ],
  },
  {
    match: "/source-context",
    method: "GET",
    body: { configured: true, linked: false },
  },
];

/** No service reported in the window: every query answers empty. */
const emptyRoutes: JsonRoute[] = [
  irCatchAll,
  { match: "/api/v1/whoami", body: WHO },
  { match: "/ui/session", method: "GET", body: sampleCurrentSession(WHO) },
];

const STATE: ExploreState = {
  ...DEFAULT_STATE,
  tenant: "acme",
  dataset: "production",
};

function OverviewPage({
  path = "/overview",
  stubRoutes = routes,
}: {
  path?: string;
  stubRoutes?: JsonRoute[];
}) {
  return (
    <StoryFetchStub routes={stubRoutes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={[path]}>
          <OverviewView state={STATE} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Overview",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof OverviewPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <OverviewPage />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <OverviewPage />
    </DarkScope>
  ),
};

/** Opened from the palette's "Open setup checklist" (`/overview?setup`). */
export const SetupChecklist: Story = {
  render: () => <OverviewPage path="/overview?setup" />,
};

export const Empty: Story = {
  render: () => <OverviewPage stubRoutes={emptyRoutes} />,
};
