import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { compositeKey } from "../../lib/traceGroups";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { irBody, irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
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
    !aggregateBy(b).includes("span.name"),
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
    rows: [
      [
        "POST /checkout",
        12_400,
        88,
        40_000_000,
        190_000_000,
        "1700003600000000000",
      ],
      ["GET /cart", 4_100, 6, 15_000_000, 60_000_000, "1700003550000000000"],
      [
        "POST /checkout/validate",
        1_700,
        18,
        30_000_000,
        160_000_000,
        "1700003500000000000",
      ],
    ],
  },
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

const routes: JsonRoute[] = [
  irCatchAll,
  catchAllEntities,
  catchAllMetrics,
  tracesFieldsRoute,
  servicesRoute,
  operationsBreakdownRoute,
  activitySparklineRoute,
  membersRoute,
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
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
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

export const EntityDetail: Story = {
  render: () => (
    <CatalogPage
      state={{
        ...DEFAULT_STATE,
        signal: "catalog",
        catalogEntity: "service",
        catalogPrimary: compositeKey(["checkout", "storefront"]),
      }}
    />
  ),
};
