import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { compositeKey } from "../../lib/traceGroups";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  irBody,
  irCatchAll,
  irEntitySeriesResponse,
  irEntityStatsResponse,
  irOperationSeriesResponse,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { CatalogView } from "./CatalogView";

/** The metric-definition/registry lookups behind the sparkline column and
 * secondary entity types' own aggregates — an empty hit list is a safe
 * "nothing here" rather than a 404 that would surface as a console error. */
const catchAllEntities: JsonRoute = {
  match: "/api/v1/schema/entities",
  body: { hits: [] },
};
const catchAllMetrics: JsonRoute = {
  match: "/api/v1/schema/metrics",
  body: { hits: [] },
};

function isDescribeFields(from: string) {
  return (b: unknown): boolean => {
    const body = b as {
      from?: string;
      pipeline?: Array<{ describe?: { target?: string } }>;
    };
    return (
      body.from === from && body.pipeline?.[0]?.describe?.target === "fields"
    );
  };
}

/** The `by` dimensions of a request's `aggregate` stage, wherever it sits in
 * the pipeline — distinguishes the service-level RED aggregate
 * (`by: ["service.name", "service.namespace"]`) from the entity detail's
 * operations breakdown (`by: [..., "span.name"]`, see `EntityDetail.tsx`'s
 * `breakdownEntity`), both `from: "traces", result: "table"`. */
function aggregateBy(b: unknown): string[] {
  const body = b as { pipeline?: Array<{ aggregate?: { by?: string[] } }> };
  for (const stage of body.pipeline ?? []) {
    if (stage.aggregate?.by) return stage.aggregate.by;
  }
  return [];
}

/** Names of one request's aggregate outputs — distinguishes the KPI strip's
 * own stats/series queries (`buildEntityStatsDoc`, `buildEntity*SeriesDoc` in
 * `api/entityDetailStats.ts`) from the older per-row RED aggregate above,
 * which shares the same `result`/`from` but a different agg set. */
function aggNames(b: unknown): string[] {
  const body = b as {
    pipeline?: Array<{ aggregate?: { aggs?: Array<{ as?: string }> } }>;
  };
  for (const stage of body.pipeline ?? []) {
    if (stage.aggregate?.aggs)
      return stage.aggregate.aggs.map((a) => a.as ?? "");
  }
  return [];
}

/** Whether the aggregate's first agg carries its own `where` — the KPI
 * strip's error-count series (`errorsOnly: true` in `buildEntityCountSeriesDoc`)
 * is the only `aggs: ["n"]` request that does. */
function firstAggHasWhere(b: unknown): boolean {
  const body = b as {
    pipeline?: Array<{ aggregate?: { aggs?: Array<{ where?: unknown }> } }>;
  };
  for (const stage of body.pipeline ?? []) {
    if (stage.aggregate?.aggs)
      return stage.aggregate.aggs[0]?.where !== undefined;
  }
  return false;
}

/** `buildEntityStatsDoc`/`buildEntityCountSeriesDoc`/`buildEntityP95SeriesDoc`
 * query both the current window and, for the "vs prev" comparison, the equal-
 * length window immediately before it (`previousPeriod` in
 * `api/entityDetailStats.ts`) — same shape, different `range`. A story fakes
 * "now" as whatever the request's own `range.to` is closest to: the current
 * window's `to` is ~now, the previous window's is ~one window-length earlier. */
function isCurrentWindow(b: unknown): boolean {
  const body = b as { range?: { to?: string } };
  const toNs = Number(body.range?.to ?? 0);
  const nowNs = Date.now() * 1_000_000;
  return Math.abs(nowNs - toNs) < 10_000 * 1_000_000;
}

/** Whether the pipeline pins the given field to an exact value — the KPI
 * strip's stats/series queries are pinned to one entity
 * (`scopeWhere`/`pinsWhere` in `api/entityDetailStats.ts`), unlike the
 * entity list's own per-row activity sparkline, which shares the same
 * `aggs: ["n"]` shape but enumerates every entity unpinned. */
function hasWhereField(b: unknown, field: string): boolean {
  const body = b as { pipeline?: Array<{ where?: { field?: string } }> };
  return (body.pipeline ?? []).some((s) => s.where?.field === field);
}

/** Field metadata for the traces source — enough for the catalog to report
 * itself as "analyzed" and offer the "service" identity dimension. */
const tracesFieldsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: isDescribeFields("traces"),
  body: {
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
      fields: [
        {
          name: "service.name",
          type: "string",
          filterable: true,
          origin: "declared",
        },
      ],
    },
  },
};

/** The service entity type's own RED aggregate (`buildEntitySourceDoc`,
 * `from: "traces"`): [service.name, service.namespace, n, errors, p50, p95,
 * last] — the "service" entity's identity is two dimensions (see
 * `entityTypes.ts`). */
const servicesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "traces")(b) &&
    !aggregateBy(b).includes("span.name") &&
    !aggNames(b).includes("p99"),
  body: {
    result: "table",
    rows: [
      [
        "checkout",
        "storefront",
        18_200,
        112,
        45_000_000,
        210_000_000,
        "1700003600000000000",
      ],
      [
        "payments",
        "storefront",
        9_400,
        340,
        60_000_000,
        480_000_000,
        "1700003580000000000",
      ],
      [
        "inventory",
        "storefront",
        7_100,
        8,
        12_000_000,
        55_000_000,
        "1700003550000000000",
      ],
      [
        "notifications",
        "platform",
        2_300,
        2,
        20_000_000,
        90_000_000,
        "1700003400000000000",
      ],
    ],
  },
};

/** ~20 realistic, distinct operations for the checkout service, ranked by
 * rate and summing to about the KPI strip's own rate — `n: 18_200` over the
 * default 1h window is `entityStatsCurrentRoute`'s `ratePerSec` of ~5.1/s
 * (`kpis.current.ratePerSec` in `EntityDetail.tsx`), so each operation's own
 * `n` here is scaled the same way (rate × 3600s) rather than picked to look
 * roughly right — the two numbers must actually add up on screen. */
const OPERATIONS = [
  { name: "POST /checkout", n: 6_480, errRate: 0.021, p50: 40, p95: 190 },
  { name: "GET /cart", n: 3_960, errRate: 0.002, p50: 15, p95: 60 },
  { name: "GET /cart/:id", n: 1_800, errRate: 0.004, p50: 18, p95: 65 },
  {
    name: "POST /checkout/validate",
    n: 1_260,
    errRate: 0.011,
    p50: 30,
    p95: 160,
  },
  { name: "GET /products", n: 1_080, errRate: 0.001, p50: 22, p95: 80 },
  { name: "POST /cart/items", n: 792, errRate: 0.006, p50: 25, p95: 95 },
  { name: "GET /orders/:id", n: 576, errRate: 0.003, p50: 20, p95: 70 },
  { name: "DELETE /cart/items/:id", n: 432, errRate: 0.002, p50: 12, p95: 45 },
  { name: "POST /checkout/payment", n: 324, errRate: 0.034, p50: 55, p95: 240 },
  { name: "GET /promotions", n: 252, errRate: 0, p50: 10, p95: 35 },
  { name: "POST /cart/coupon", n: 180, errRate: 0.019, p50: 28, p95: 110 },
  { name: "GET /shipping/rates", n: 144, errRate: 0.008, p50: 45, p95: 150 },
  { name: "POST /checkout/address", n: 108, errRate: 0.005, p50: 20, p95: 75 },
  { name: "GET /inventory/:sku", n: 90, errRate: 0.002, p50: 14, p95: 50 },
  {
    name: "PATCH /cart/items/:id",
    n: 72,
    errRate: 0.004,
    p50: 18,
    p95: 60,
  },
  { name: "GET /checkout/summary", n: 54, errRate: 0.001, p50: 16, p95: 55 },
  { name: "POST /wishlist/items", n: 43, errRate: 0, p50: 12, p95: 40 },
  { name: "GET /recommendations", n: 36, errRate: 0.003, p50: 35, p95: 130 },
  { name: "POST /checkout/gift-card", n: 29, errRate: 0.012, p50: 22, p95: 85 },
  { name: "GET /returns/:id", n: 22, errRate: 0.006, p50: 19, p95: 70 },
];

/** The entity detail's operations breakdown (`EntityDetail.tsx`'s
 * `breakdownEntity`, identity `["span.name"]`, pinned to the drilled-in
 * service): [span.name, n, errors, p50, p95, last]. */
const operationsBreakdownRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "traces")(b) &&
    aggregateBy(b).includes("span.name"),
  body: {
    result: "table",
    rows: OPERATIONS.map((op, i) => [
      op.name,
      op.n,
      Math.round(op.n * op.errRate),
      op.p50 * 1_000_000,
      op.p95 * 1_000_000,
      String(1_700_003_600_000_000_000 - i * 5_000_000_000),
    ]),
  },
};

/** A tiny seeded PRNG (mulberry32) — deterministic per operation, so its
 * story renders the same wobble on every build rather than a fresh random
 * one each run. */
function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++)
    h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

/** One operation's own "last hour" series — a smooth-ish wave around its
 * own rate (not every operation's identical sawtooth), plus a little seeded
 * noise, plus a spike bucket on the busier operations. Each operation gets
 * its own phase and noise seed from its name, so two operations at the same
 * rate still draw visibly different shapes. */
function operationSeries(op: (typeof OPERATIONS)[number]): [number, number][] {
  const seed = hashString(op.name);
  const rand = mulberry32(seed);
  const phase = (seed % 628) / 100; // 0..2π-ish, distinct per operation
  const spikeAt = op.n > 1000 ? 5 + (seed % 20) : -1;
  const avg = op.n / 30;
  return Array.from({ length: 30 }, (_, i) => {
    const wave = 1 + 0.3 * Math.sin(phase + i * 0.35);
    const noise = 1 + (rand() - 0.5) * 0.3;
    const spike = i === spikeAt ? 1.8 : 1;
    return [
      1_700_000_000_000_000_000 + i * 120_000_000_000,
      Math.max(0, Math.round(avg * wave * noise * spike)),
    ];
  });
}

/** The Operations table's per-operation "last hour" sparklines
 * (`useOperationSeries`/`fetchOperationSeries`, `from: "traces", result:
 * "series"`, grouped by `span.name`) — each operation draws its own shape
 * (see `operationSeries`), scaled to its own rate. */
const operationSeriesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "series" && body.from === "traces")(b) &&
    aggregateBy(b).includes("span.name"),
  body: irOperationSeriesResponse(
    "span_name",
    Object.fromEntries(OPERATIONS.map((op) => [op.name, operationSeries(op)])),
  ),
};

/** The entity list's activity sparkline (`buildActivityDoc`, `from:
 * "traces", result: "series"`, grouped by the service identity) — the
 * fallback column shown for an entity type the registry names no metric
 * for, which "service" is (see `useSparklineColumn`'s `activity` path). */
const activitySparklineRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "series" && b.from === "traces"),
  body: {
    result: "series",
    series: [
      {
        labels: { service_name: "checkout", service_namespace: "storefront" },
        points: Array.from({ length: 20 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 180_000_000_000,
          40 + (i % 6) * 3,
        ]),
      },
      {
        labels: { service_name: "payments", service_namespace: "storefront" },
        points: Array.from({ length: 20 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 180_000_000_000,
          18 + (i % 4) * 2,
        ]),
      },
      {
        labels: { service_name: "inventory", service_namespace: "storefront" },
        points: Array.from({ length: 20 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 180_000_000_000,
          12 + (i % 3),
        ]),
      },
      {
        labels: {
          service_name: "notifications",
          service_namespace: "platform",
        },
        points: Array.from({ length: 20 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 180_000_000_000,
          4 + (i % 2),
        ]),
      },
    ],
  },
};

/** Member spans for the entity detail's "Recent matching spans" table
 * (`buildMembersDoc`, `from: "traces"`, `result: "rows"`) — same eight-column
 * shape `TracesView.stories.tsx`'s span route uses. */
const membersRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "rows" && b.from === "traces"),
  body: {
    result: "rows",
    rows: [
      [
        "t1cafe",
        "root",
        null,
        "checkout",
        "checkout",
        "1700003600000000000",
        "45000000",
        "OK",
      ],
      [
        "t2beef",
        "root",
        null,
        "checkout",
        "checkout",
        "1700003580000000000",
        "210000000",
        "ERROR",
      ],
    ],
  },
};

/** A count series shaped like `buildEntityCountSeriesDoc`'s buckets — a mild
 * upward trend, so the KPI strip's rate card reads as "picking up" rather
 * than a flat line. */
function bucketedSeries(
  base: number,
  bumpEvery: number,
  bumpAmount: number,
): [number, number][] {
  return Array.from({ length: 20 }, (_, i) => [
    1_700_000_000_000_000_000 + i * 180_000_000_000,
    base + i * 6 + (i % bumpEvery === 0 ? bumpAmount : 0),
  ]);
}

/** The entity detail KPI strip's own current-window stats (`buildEntityStatsDoc`
 * in `api/entityDetailStats.ts`) — the "checkout" service's numbers, up from
 * the previous-period route below so the cards show a "worse than before"
 * comparison (see `entityStatsPreviousRoute`). */
const entityStatsCurrentRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "traces")(b) &&
    aggNames(b).includes("p99") &&
    isCurrentWindow(b),
  body: irEntityStatsResponse({
    identity: ["checkout", "storefront"],
    n: 18_200,
    errors: 950,
    p50Ns: 45_000_000,
    p95Ns: 210_000_000,
    p99Ns: 320_000_000,
    lastNs: "1700003600000000000",
  }),
};

/** The equal-length window immediately before `entityStatsCurrentRoute`'s —
 * lower traffic and a cleaner error rate, so the KPI strip's "vs prev"
 * figures have real (and mixed-tone) direction to show. */
const entityStatsPreviousRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "traces")(b) &&
    aggNames(b).includes("p99") &&
    !isCurrentWindow(b),
  body: irEntityStatsResponse({
    identity: ["checkout", "storefront"],
    n: 15_800,
    errors: 550,
    p50Ns: 38_000_000,
    p95Ns: 180_000_000,
    p99Ns: 290_000_000,
    lastNs: "1699996800000000000",
  }),
};

/** The rate sparkline's own count series — `n` total per bucket, pinned to
 * "checkout", unlike the entity list's unpinned per-row activity series
 * (`activitySparklineRoute`) that shares the same `aggs: ["n"]` shape. */
const entityRateSeriesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "series" && body.from === "traces")(b) &&
    aggNames(b).join() === "n" &&
    !firstAggHasWhere(b) &&
    hasWhereField(b, "service.name"),
  body: irEntitySeriesResponse(bucketedSeries(600, 5, 200)),
};

/** The errors sparkline's error-only count series (`errorsOnly: true`). */
const entityErrorSeriesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "series" && body.from === "traces")(b) &&
    aggNames(b).join() === "n" &&
    firstAggHasWhere(b) &&
    hasWhereField(b, "service.name"),
  body: irEntitySeriesResponse(bucketedSeries(20, 4, 15)),
};

/** The duration sparkline's p95 series. */
const entityP95SeriesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "series" && body.from === "traces")(b) &&
    aggNames(b).join() === "p95" &&
    hasWhereField(b, "service.name"),
  body: irEntitySeriesResponse(bucketedSeries(150_000_000, 6, 40_000_000)),
};

/** The entity detail's "Error groups" section (`EntityErrorGroups`,
 * `api/errors.ts`'s `buildErrorGroupDoc`, `from: "traces"`) — five realistic
 * exception groups for the "checkout" service, ranked by count so the
 * section's own top-5-by-count slicing has something to prove: [type,
 * message, service, escaped, n, first, last]. */
const errorGroupsTracesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "traces")(b) &&
    aggregateBy(b).includes("exception.type"),
  body: {
    result: "table",
    rows: [
      [
        "PaymentDeclinedError",
        "card declined by issuer",
        "checkout",
        "true",
        128,
        "1700003000000000000",
        "1700003600000000000",
      ],
      [
        "DeadlineExceededError",
        "upstream call timed out",
        "checkout",
        "true",
        94,
        "1700002800000000000",
        "1700003580000000000",
      ],
      [
        "PoolExhaustedError",
        "connection pool exhausted",
        "checkout",
        "true",
        61,
        "1700002600000000000",
        "1700003500000000000",
      ],
      [
        "NullPointerException",
        "null reference in checkout handler",
        "checkout",
        "false",
        37,
        "1700002400000000000",
        "1700003400000000000",
      ],
      [
        "UpstreamServiceError",
        "503 from payments-gateway",
        "checkout",
        "true",
        15,
        "1700002200000000000",
        "1700003300000000000",
      ],
    ],
  },
};

/** The same "Error groups" section's logs-sourced half of the merge (see
 * `fetchErrorGroups`) — empty, so the story's five groups above aren't
 * doubled by an identical logs-sourced set. */
const errorGroupsLogsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "table" && body.from === "logs")(b) &&
    aggregateBy(b).includes("exception.type"),
  body: { result: "table", rows: [] },
};

/** Each error-group row's own "Last hour" sparkline (`fetchErrorGroupVolume`,
 * `aggregate.by: ["exception.type"]` only — distinct from the KPI strip's
 * `service.name`-keyed series above). One shared shape for every row. */
const errorGroupVolumeRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) =>
    irBody((body) => body.result === "series")(b) &&
    aggregateBy(b).join() === "exception.type",
  body: irEntitySeriesResponse(bucketedSeries(3, 4, 4)),
};

const routes: JsonRoute[] = [
  irCatchAll,
  catchAllEntities,
  catchAllMetrics,
  tracesFieldsRoute,
  servicesRoute,
  operationsBreakdownRoute,
  activitySparklineRoute,
  membersRoute,
  entityStatsCurrentRoute,
  entityStatsPreviousRoute,
  entityRateSeriesRoute,
  entityErrorSeriesRoute,
  entityP95SeriesRoute,
  operationSeriesRoute,
  errorGroupsTracesRoute,
  errorGroupsLogsRoute,
  errorGroupVolumeRoute,
];

function CatalogPage({ state }: { state: ExploreState }) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/catalog"]}>
          <CatalogView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Catalog",
  parameters: { layout: "fullscreen" },
  decorators: [
    // `DarkScope` uses `min-height: 100%` (not a fixed `height`), so it
    // already stretches to cover whatever a story renders — no fixed height
    // needed here to keep a taller entity detail page's later sections
    // (Error groups, Time by dependency, the spans table) inside the dark
    // scope.
    (Story) => (
      <div style={{ width: 1280 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof CatalogPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <CatalogPage state={{ ...DEFAULT_STATE, signal: "catalog" }} />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <CatalogPage state={{ ...DEFAULT_STATE, signal: "catalog" }} />
    </DarkScope>
  ),
};

const entityDetailState: ExploreState = {
  ...DEFAULT_STATE,
  signal: "catalog",
  catalogEntity: "service",
  catalogPrimary: compositeKey(["checkout", "storefront"]),
};

export const EntityDetail: Story = {
  render: () => <CatalogPage state={entityDetailState} />,
};

export const EntityDetailDark: Story = {
  render: () => (
    <DarkScope>
      <CatalogPage state={entityDetailState} />
    </DarkScope>
  ),
};
